#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GEN="$ROOT/target/release/ais-parquet-optimizer"
BENCH="$ROOT/target/release/ais-parquet-benchmark"
TIMINGS="$ROOT/timings"
MAX_ROWS=93000000
CSV_DIR="$ROOT/csv"
SOURCE="$ROOT/parquet/ais_2024.parquet"

echo "=== Step 0: Build source if missing (3 months of data) ==="
if [ ! -f "$SOURCE" ]; then
  "$GEN" \
    --rebuild-source \
    --csv-dir "$CSV_DIR" \
    --source "$SOURCE" \
    --max-rows "$MAX_ROWS"
else
  echo "  source exists, skipping rebuild"
fi

echo ""
echo "=== Experiment 1: Row Group Size x Data Page Size (hilbert_bloom) ==="
RGS_VALUES=(100000 500000 1000000 5000000)
DPS_VALUES=(65536 262144 1048576)

for rgs in "${RGS_VALUES[@]}"; do
  for dps in "${DPS_VALUES[@]}"; do
    COMDIR="$TIMINGS/rgs-dps/rgs_${rgs}-dps_${dps}"
    if [ -d "$COMDIR" ]; then
      echo "  SKIP (exists): $COMDIR"
      continue
    fi
    mkdir -p "$(dirname "$COMDIR")"
    echo "--- rgs_${rgs}-dps_${dps} ---"
    "$GEN" \
      --source "$SOURCE" \
      --only hilbert_bloom \
      --hilbert-bloom-output "$COMDIR" \
      --row-group-size "$rgs" \
      --data-page-size "$dps" \
      --max-rows "$MAX_ROWS"
    "$BENCH" \
      --source "$SOURCE" \
      --partitioned-hilbert-bloom "$COMDIR" \
      --report "$COMDIR/report.md" \
      --json-results "$COMDIR/results.json" \
      --only-format hilbert_bloom \
      --warmup-runs 1 --timed-runs 2
  done
done

echo ""
echo "=== Experiment 2: Bloom FPP x NDV (hilbert_bloom) ==="
FPP_VALUES=(0.1 0.01 0.001)
NDV_VALUES=(100000 1000000 10000000)

for fpp in "${FPP_VALUES[@]}"; do
  for ndv in "${NDV_VALUES[@]}"; do
    COMDIR="$TIMINGS/bloom/fpp_${fpp}-ndv_${ndv}"
    if [ -d "$COMDIR" ]; then
      echo "  SKIP (exists): $COMDIR"
      continue
    fi
    mkdir -p "$(dirname "$COMDIR")"
    echo "--- fpp_${fpp}-ndv_${ndv} ---"
    "$GEN" \
      --source "$SOURCE" \
      --only hilbert_bloom \
      --hilbert-bloom-output "$COMDIR" \
      --bloom-fpp "$fpp" \
      --bloom-ndv "$ndv" \
      --max-rows "$MAX_ROWS"
    "$BENCH" \
      --source "$SOURCE" \
      --partitioned-hilbert-bloom "$COMDIR" \
      --report "$COMDIR/report.md" \
      --json-results "$COMDIR/results.json" \
      --only-format hilbert_bloom \
      --warmup-runs 1 --timed-runs 2
  done
done

echo ""
echo "=== Step: Analyze results ==="
python3 "$ROOT/scripts/analyze_sweep.py" "$TIMINGS"

echo ""
echo "=== Done. Report: $TIMINGS/param_sweep_report.md ==="
