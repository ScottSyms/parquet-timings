# AIS Parquet Optimisations

All optimisations live in two files:
- **Writer:** `src/main.rs` (generator binary)
- **Reader:** `src/bin/benchmark.rs` (benchmark binary)

## Writing Optimisations

### 1. Row Group Size (`--row-group-size`)
- **Default:** 1,000,000 rows
- **Sweep optimal:** 5,000,000 rows
- Larger RGs → fewer row groups → less metadata overhead for full-scans, but worse for point-lookups (more rows decoded per RG hit). The optimal 5M works best with bloom/Hilbert to skip the big RGs.

### 2. Data Page Size (`--data-page-size`)
- **Default:** 0 (parquet-rs internal default ~1MB)
- **Sweep optimal:** 262,144 bytes (256KB)
- Smaller pages → finer-grained skipping at the page level via stats, at the cost of more page headers.

### 3. Bloom Filter FPP (`--bloom-fpp`)
- **Default:** 0.01
- **Sweep optimal:** 0.001
- Lower FPP = larger filters but fewer false positives → fewer unnecessary RG decodes.

### 4. Bloom Filter NDV (`--bloom-ndv`)
- **Default:** 1,000,000
- **Sweep optimal:** 10,000,000
- Sets expected number of distinct values. Too low → hash collisions degrade filter accuracy.

### 5. Per-Column Bloom NDV (`--bloom-ndv-per-column COLUMN=NDV`)
- Overrides the global `--bloom-ndv` per column to avoid over-allocating for low-cardinality columns:
  - `MMSI=50000` — ~50K vessels in 3-month window
  - `IMO=1000` — low cardinality (many vessels lack IMO)
  - `CallSign=2000` — low cardinality
  - `VesselName=2000` — low cardinality
  - `VesselType=100` — very low cardinality (handful of types)

### 6. Bloom Columns (`--bloom-column`)
- **Default:** `MMSI`, `IMO`
- Must also include string columns explicitly: `--bloom-column CallSign --bloom-column VesselName --bloom-column VesselType`
- String bloom filters use `BYTE_ARRAY` type with exact-match checks. They don't help prefix/substring queries.

### 7. Smart Row Group Sizing (`--smart-rgs`)
- Caps the effective RGS at `min(user_rgs, avg_partition_rows / 5)`
- For 93M rows across ~93 daily partitions (avg ~1M/partition), produces RGS ≈ 200K, yielding ~5 spatially-coherent row groups per day.
- More RGs → more bloom/Hilbert skip opportunities.

### 8. Hilbert Index (HILBERT_BITS = 31)
- **Files:** `src/main.rs:1735`, `src/bin/benchmark.rs:25-26`
- Uses 31 bits per axis instead of 32. Keeps Hilbert indices within `[0, 2⁶²)` — safe for parquet's `UInt64` → `Int64` physical-type round-trip (`i64::MAX ≈ 9.2 × 10¹⁸`, max index = `2⁶² - 1 ≈ 4.6 × 10¹⁸`).
- With 32 bits, indices can exceed `i64::MAX`, causing negative values after the `UInt64` → `Int64` cast, corrupting min/max statistics and making `hilbert_skip_row_group` always return `false`.
- Spatial precision at 31 bits: ~6mm per cell at the equator (vs ~3mm at 32 bits) — well within AIS GPS noise (~10m).

## Reading Optimisations

### 9. Pruning Efficiency tracking
- Per-query skip breakdown in benchmark output:
  - `stats_skip` — column chunk statistics (min/max ranges)
  - `hilbert_skip` — spatial Hilbert-curve interval overlap check
  - `bloom_skip` — bloom filter exact-match rejection
  - `decoded_row_groups` — RGs that passed all filters
- Added to `write_report()` at the bottom of the benchmark module.

### 10. Hilbert pruning with interval overlap
- `hilbert_skip_row_group()` at `benchmark.rs:1011`: reads `hilbert_index` min/max from column statistics and checks overlap with query-derived Hilbert intervals.

### 11. String bloom filter checking
- `check_bloom()` at `benchmark.rs:1093`: handles `Type::BYTE_ARRAY` via `filter.check(&value)` (requires `&[u8]` which `&str` coerces to).

## Replicate the Optimal Setup

```bash
# Generator (3-month window)
cargo run --release -- \
  --source parquet/ais_2024.parquet \
  --only hilbert-bloom \
  --hilbert-bloom-output partition_hilbert_bloom_optimal \
  --row-group-size 5000000 \
  --data-page-size 262144 \
  --bloom-fpp 0.001 \
  --bloom-ndv 10000000 \
  --bloom-ndv-per-column MMSI=50000 \
  --bloom-ndv-per-column IMO=1000 \
  --bloom-ndv-per-column CallSign=2000 \
  --bloom-ndv-per-column VesselName=2000 \
  --bloom-ndv-per-column VesselType=100 \
  --bloom-column MMSI \
  --bloom-column IMO \
  --bloom-column CallSign \
  --bloom-column VesselName \
  --bloom-column VesselType \
  --smart-rgs

# Benchmark
cargo run --release --bin benchmark -- \
  --source parquet/ais_2024.parquet \
  --partitioned-hilbert-bloom partition_hilbert_bloom_optimal \
  --report report.md
```

## Results

**Combined geo-mean speedup: 2.40x** (baseline default params vs tuned params + smart-rgs + per-column NDV)

| Query | Baseline (s) | Optimal (s) | Speedup |
|---|---|---|---|
| MMSI exact lookup | 1.28 | 0.44 | 2.88x |
| IMO exact lookup | 1.37 | 0.62 | 2.20x |
| VesselName exact match | 15.66 | 1.54 | 10.16x |
| CallSign exact match | 14.55 | 1.47 | 9.89x |
| VesselName + time range | 6.29 | 0.46 | 13.55x |
| MMSI + time range | 0.57 | 0.30 | 1.92x |
| Bloom test: IMO = exact (exists) | 1.33 | 0.59 | 2.25x |
