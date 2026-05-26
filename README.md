# AIS Parquet Optimizer

Optimizes 743M-row AIS vessel tracking data into multiple Hive-partitioned Parquet layouts for benchmarking row-group pruning, bloom-filter skipping, and Hilbert-curve interval pruning.

## Layouts

| Layout | Strategy |
|---|---|
| **Monolithic** | Single 16 GB Parquet file (baseline) |
| **Partitioned** | Hive-partitioned by `year/month/day`, sorted by `BaseDateTime` |
| **Partitioned+Bloom** | Same as Partitioned, plus bloom filters on `MMSI` and `IMO` |
| **Partitioned+Hilbert** | Same as Partitioned, rows reordered by 4D Hilbert curve (`time, lat, lon, mmsi`) with row-group min/max on the Hilbert column for interval pruning |
| **Partitioned+Hilbert+Bloom** | Hilbert + bloom filters combined |

All layouts use ZSTD level 22.

## Prerequisites

- Rust 2021 edition
- CSV source files named `AIS_2024_MM_DD.csv` in `./csv/` (one per day, 366 files)
- The sample at `sample.csv` shows the expected columns (17 data columns + metadata)
- DuckDB (optional, for ad-hoc inspection)

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

## Row-group size

```sh
cargo run --release --bin ais-parquet-optimizer -- \
  --row-group-size 50000 --overwrite
```

Default: 50 000 rows/group. Smaller groups improve row-group pruning and bloom-filter selectivity for point lookups but increase metadata overhead and reduce compression ratio. Also affects bloom filter NDV sizing (currently hardcoded at 1_000_000).

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
- **Hilbert layouts** use a 4D Hilbert curve (time, lat, lon, mmsi) at 16 bits/axis, stored as a `UInt64` column. Row groups have min/max statistics on `hilbert_index`, enabling interval-based pruning.
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
