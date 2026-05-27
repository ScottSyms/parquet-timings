# AIS Parquet Optimizer

Optimizes 743M-row AIS vessel tracking data into multiple Hive-partitioned Parquet layouts for benchmarking row-group pruning, bloom-filter skipping, and Hilbert-curve interval pruning.

## Layouts

| Layout | Strategy |
|---|---|
| **Monolithic** | Single 16 GB Parquet file (baseline) |
| **Partitioned** | Hive-partitioned by `year/month/day`, sorted by `BaseDateTime` |
| **Partitioned+Bloom** | Same as Partitioned, plus bloom filters on `MMSI`, `IMO`, `CallSign`, `VesselName`, `VesselType` |
| **Partitioned+Hilbert** | Same as Partitioned, rows reordered by 2D Hilbert curve (`lat, lon`) at 31 bits/axis, with row-group min/max on the Hilbert column for interval pruning |
| **Partitioned+Hilbert+Bloom** | Hilbert + bloom filters combined |

All layouts use ZSTD(6) compression.

## Prerequisites

- Rust 2021 edition
- CSV source files named `AIS_2024_MM_DD.csv` in `./csv/` (one per day, 366 files)
- The sample at `sample.csv` shows the expected columns (17 data columns + metadata)
- DuckDB (optional, for ad-hoc inspection)
- Python 3 + `pandas`, `numpy`, `matplotlib` (for `scripts/analyze_sweep.py`)

## Build

```sh
cargo build --release
```

Two binaries:
- `ais-parquet-optimizer` — generates source Parquet and optimized layouts
- `benchmark` — benchmarks all layouts and produces a Markdown report

## Workflow

### 1. Rebuild the source Parquet from CSV

```sh
cargo run --release --bin ais-parquet-optimizer -- \
  --rebuild-source --overwrite
```

Reads `csv/AIS_2024_*.csv`, transforms types (Int64 for MMSI/IMO, Float32 for coordinates, Boolean for TransceiverClass), and writes `parquet/ais_2024.parquet` sorted by `BaseDateTime` with ZSTD(22) compression.

Without `--overwrite` the command errors if the source already exists.

### 2. Generate optimized layouts (all four)

```sh
cargo run --release --bin ais-parquet-optimizer -- \
  --overwrite
```

Creates four output directories:

| Directory | Layout |
|---|---|
| `partition_parquet/` | Partitioned |
| `partition_bloom_parquet/` | Partitioned + Bloom |
| `partition_hilbert_parquet/` | Partitioned + Hilbert |
| `partition_hilbert_bloom_parquet/` | Partitioned + Hilbert + Bloom |

Each is Hive-partitioned `year=X/month=Y/day=Z/part-00000.parquet`.

### 3. Run the benchmark

```sh
cargo run --release --bin benchmark
```

Runs 31 queries across all five layouts (monolithic + 4 optimized), prints per-query timing and a consistency check, and writes `rust_benchmark_report.md`.

## Parallel generation

`--only all` runs each day partition as an independent task that reads from source once and writes all four outputs for that day. Control concurrency with `--jobs`:

```sh
# Use 4 concurrent partition tasks
cargo run --release --bin ais-parquet-optimizer -- \
  --overwrite --jobs 4
```

Default: `num_cpus` (sequentially with `--jobs 1`).

## Individual layouts

Generate one layout at a time instead of all four:

```sh
# Source only
cargo run -- --rebuild-source --overwrite --only source

# Partitioned only
cargo run -- --overwrite --only partition

# Partitioned + Bloom only
cargo run -- --overwrite --only bloom

# Partitioned + Hilbert only
cargo run -- --overwrite --only hilbert

# Partitioned + Hilbert + Bloom only
cargo run -- --overwrite --only hilbert-bloom
```

## Custom paths

```sh
cargo run --release --bin ais-parquet-optimizer -- \
  --source /custom/path/ais_2024.parquet \
  --partition-output /outputs/partition \
  --bloom-output /outputs/partition_bloom \
  --hilbert-output /outputs/partition_hilbert \
  --hilbert-bloom-output /outputs/partition_hilbert_bloom \
  --overwrite
```

## Tuning Parameters

All generator flags can be combined to control the Parquet layout. A
[parameter sweep](scripts/param_sweep.sh) determined the optimal values across 93M rows
of AIS data (3 months). Results are analyzed in
[timings/param_sweep_report.md](timings/param_sweep_report.md).

### Row group size (`--row-group-size`)

Number of rows per row group. Default: 1,000,000. Sweep optimal: 5,000,000.

Larger RGs → fewer row groups → less metadata overhead for full-scans, but worse
for point lookups (more rows decoded per RG hit). The optimal 5M works well with
bloom and Hilbert filtering to skip the large RGs.

```sh
cargo run --release --bin ais-parquet-optimizer -- \
  --row-group-size 5000000 --overwrite
```

### Smart row group sizing (`--smart-rgs`)

Caps the effective RGS at `min(user_rgs, avg_partition_rows / 5)`. For 93M rows
across ~93 daily partitions (avg ~1M/day), this produces RGS ≈ 200K, yielding ~5
spatially-coherent row groups per partition. More RGs → more bloom and Hilbert
skip opportunities.

```sh
cargo run --release --bin ais-parquet-optimizer -- \
  --row-group-size 5000000 --smart-rgs --overwrite
```

### Data page size (`--data-page-size`)

Bytes per data page. Default: 0 (parquet-rs internal default ~1MB). Sweep optimal:
262,144 bytes (256KB).

Smaller pages → finer-grained skipping at the page level via statistics, at the
cost of more page headers.

```sh
cargo run --release --bin ais-parquet-optimizer -- \
  --data-page-size 262144 --overwrite
```

### Bloom filter false positive rate (`--bloom-fpp`)

Target FPP for all bloom filters. Default: 0.01. Sweep optimal: 0.001.

Lower FPP = larger filters but fewer false positives → fewer unnecessary RG
decodes.

### Bloom filter NDV (`--bloom-ndv`)

Expected distinct values per bloom filter. Default: 1,000,000. Sweep optimal:
10,000,000.

Too low → hash collisions degrade filter accuracy. 10M covers high-cardinality
columns (e.g., MMSI, IMO).

### Per-column bloom NDV (`--bloom-ndv-per-column COLUMN=NDV`)

Overrides the global `--bloom-ndv` per column to avoid over-allocating for
low-cardinality columns:

| Column | NDV | Rationale |
|---|---|---|
| `MMSI` | 50,000 | ~50K unique vessels in a 3-month window |
| `IMO` | 1,000 | Many vessels lack IMO |
| `CallSign` | 2,000 | Low cardinality |
| `VesselName` | 2,000 | Low cardinality |
| `VesselType` | 100 | Handful of vessel types |

```sh
cargo run --release --bin ais-parquet-optimizer -- \
  --bloom-ndv-per-column MMSI=50000 \
  --bloom-ndv-per-column IMO=1000 \
  --bloom-ndv-per-column CallSign=2000 \
  --bloom-ndv-per-column VesselName=2000 \
  --bloom-ndv-per-column VesselType=100 \
  --overwrite
```

### Bloom columns (`--bloom-column`)

Default bloom columns are `MMSI` and `IMO`. String columns must be explicitly
enabled via `--bloom-column`:

```sh
cargo run --release --bin ais-parquet-optimizer -- \
  --bloom-column MMSI \
  --bloom-column IMO \
  --bloom-column CallSign \
  --bloom-column VesselName \
  --bloom-column VesselType \
  --overwrite
```

String bloom filters use `BYTE_ARRAY` type with exact-match checks. They don't
help prefix or substring queries.

### Hilbert bits (HILBERT_BITS = 31)

Defined at `src/main.rs:1735` and `src/bin/benchmark.rs:25`.

Uses 31 bits per axis instead of 32. This keeps Hilbert indices within `[0, 2⁶²)`
— safe for Parquet's `UInt64` → `Int64` physical-type round-trip (`i64::MAX ≈
9.2 × 10¹⁸`, max index = `2⁶² - 1 ≈ 4.6 × 10¹⁸`). With 32 bits, indices would
exceed `i64::MAX`, causing negative values after the cast and corrupting min/max
statistics.

Spatial precision at 31 bits: ~6mm per cell at the equator — well within AIS GPS
noise (~10m). See [`hilbert.md`](hilbert.md) for the encode/decode algorithm.

## Optimal Configuration

Full command to replicate the combined validation (Experiment 3 in the sweep):

```sh
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
```

Benchmark it:

```sh
cargo run --release --bin benchmark -- \
  --source parquet/ais_2024.parquet \
  --partitioned-hilbert-bloom partition_hilbert_bloom_optimal \
  --report report.md
```

### Results

**Combined geo-mean speedup: 2.40x** (baseline default params vs tuned params +
smart-rgs + per-column NDV).

| Query | Baseline (s) | Optimal (s) | Speedup |
|---|---|---|---|
| MMSI exact lookup | 1.28 | 0.44 | 2.88x |
| IMO exact lookup | 1.37 | 0.62 | 2.20x |
| VesselName exact match | 15.66 | 1.54 | 10.16x |
| CallSign exact match | 14.55 | 1.47 | 9.89x |
| VesselName + time range | 6.29 | 0.46 | 13.55x |
| MMSI + time range | 0.57 | 0.30 | 1.92x |
| Bloom test: IMO = exact (exists) | 1.33 | 0.59 | 2.25x |

## How Hilbert Pruning Works

The Hilbert curve maps 2D space (lat, lon) to a 1D index while preserving
locality: nearby points in space produce nearby indices. This lets Parquet's
row-group min/max statistics act as a spatial index.

### Encoding (writer side)

1. `scale(lat)` and `scale(lon)` map each coordinate from its world range
   (e.g., `[-90, 90]` for latitude) to a `u32` in `[0, 2^31 - 1]` via linear
   interpolation.
2. `hilbert_index_2d([lat, lon])` interleaves the bits of both coordinates
   through a bitwise transpose, producing a single `u64`. The transpose is
   the key step — it rotates the coordinate axes so the interleaved bits
   follow the Hilbert curve's recursive Z-order.
3. The index is stored as a `UInt64` column with per-RG min/max statistics.

### Decoding (reader side, query time)

1. `query_hilbert_intervals(bounding_box)` converts the query's lat/lon range
   into a set of disjoint Hilbert-index intervals. It uses a recursive
   quadtree descent:
   - Start with the full space at `HILBERT_BITS`.
   - For each cell: if fully inside the query → emit its Hilbert interval
     (`cell_interval_2d`); if partially overlapping → subdivide into 4
     Hilbert-sorted children and recurse; if outside → discard.
   - Capped at 512 intervals to prevent pathological splits.
   - Resulting intervals are merged (sorted, overlapping adjacent merged).
2. `hilbert_skip_row_group(rg_min, rg_max, intervals)` checks whether any
   interval overlaps the RG's stored `[min, max]` Hilbert range. If none
   overlap, the entire row group is skipped.

See [`hilbert.md`](hilbert.md) for full pseudocode.

## Benchmark options

```sh
cargo run --release --bin benchmark -- \
  --warmup-runs 2 \
  --timed-runs 5 \
  --report my_report.md
```

Defaults: 1 warm-up run, 2 timed runs.

## Architecture

```
csv/AIS_2024_*.csv
        │
        ▼  (build_source_from_csv)
parquet/ais_2024.parquet   ← monolithic source, ZSTD(22)
        │
        ▼  (per partition, with bounded concurrency)
┌─────────────────────────────────────┐
│ process_partition_all              │
│                                     │
│  read source partition (one pass)  │
│        │                           │
│  ┌─────┼─────┬──────────┐         │
│  ▼     ▼     ▼          ▼         │
│ part  bloom  staging  (append      │
│              files     hilbert idx)│
│                      │             │
│                      ▼             │
│               sort by              │
│               hilbert_index        │
│                      │             │
│                 ┌────┴────┐        │
│                 ▼         ▼        │
│              hilbert    hilbert    │
│                         +bloom    │
└─────────────────────────────────────┘
```

- **Partitioned layouts** sort by `BaseDateTime`, write through `PartitionWriter` (strips year/month/day columns).
- **Hilbert layouts** use a 2D Hilbert curve (`lat, lon`) at 31 bits/axis, stored as a `UInt64` column. Row groups have min/max statistics on `hilbert_index`, enabling interval-based pruning. 31 bits avoids the `UInt64` → `Int64` statistics overflow (values stay within `i64::MAX`).
- **Bloom layouts** write bloom filters for the configured columns (`--bloom-column`) with per-column NDV (`--bloom-ndv-per-column`). String columns use `BYTE_ARRAY` type bloom filters with exact-match checking.
- **Smart RGS** (`--smart-rgs`) caps the effective row-group size to produce ~5 spatially-coherent RGs per daily partition, increasing bloom and Hilbert skip opportunities.
- **Staging** spills Hilbert-appended batches into a temporary directory, then re-reads and sorts by `hilbert_index` before final output.

## Schema

| Column | Type | Notes |
|---|---|---|
| MMSI | Int64 | |
| BaseDateTime | Timestamp (microsecond) | |
| LAT | Float32 | |
| LON | Float32 | |
| SOG | Float32 | |
| COG | Float32 | |
| Heading | Float32 | |
| VesselName | Utf8 | |
| IMO | Int64 | `IMO` prefix stripped from CSV |
| CallSign | Utf8 | |
| VesselType | Utf8 | |
| Status | Utf8 | |
| Length | Float32 | |
| Width | Float32 | |
| Draft | Float32 | |
| Cargo | Utf8 | |
| TransceiverClass | Boolean | `true` only when CSV value is `B` |
| hilbert_index | UInt64 | Hilbert layouts only (not in source) |

## DuckDB inspection

```sql
SELECT COUNT(*) FROM read_parquet('partition_parquet/**/*.parquet');
DESCRIBE SELECT * FROM read_parquet('parquet/ais_2024.parquet');
```

## Python benchmark

A PyArrow-based reference benchmark exists at `benchmark.py`:

```sh
uv run benchmark.py
```

This produces `python_benchmark_report.md`. It is kept as an ecosystem comparison and is not as fully featured as the Rust benchmark.
