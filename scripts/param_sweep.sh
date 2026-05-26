#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GEN="$ROOT/target/release/ais-parquet-optimizer"
BENCH="$ROOT/target/release/ais-parquet-benchmark"
SWEEP="$ROOT/sweep"
MAX_ROWS=93000000
CSV_DIR="$ROOT/csv"
SOURCE="$ROOT/parquet/ais_2024.parquet"

mkdir -p "$SWEEP"

echo "=== Step 0: Build source (3 months) ==="
"$GEN" \
  --rebuild-source \
  --csv-dir "$CSV_DIR" \
  --source "$SOURCE" \
  --max-rows "$MAX_ROWS"

echo ""
echo "=== Experiment 1: Row Group Size × Data Page Size (hilbert_bloom) ==="
RGS_VALUES=(100000 500000 1000000 5000000)
DPS_VALUES=(65536 262144 1048576)

for rgs in "${RGS_VALUES[@]}"; do
  for dps in "${DPS_VALUES[@]}"; do
    COMDIR="$SWEEP/rgs-dps/rgs_${rgs}-dps_${dps}"
    COMNAME="hb_rgs${rgs}_dps${dps}"
    echo "--- $COMNAME ---"
    rm -rf "$COMDIR"
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
echo "=== Experiment 2: Validate best RGS/DPS on mono ==="
# Find best combo (highest geo-mean speedup) — will be picked by analysis script
# Generate mono with the best parameters
BEST_RGS=1000000
BEST_DPS=1048576
# ^ These will be overwritten by the analysis script below

echo ""
echo "=== Experiment 3: Bloom FPP × NDV (hilbert_bloom) ==="
FPP_VALUES=(0.1 0.01 0.001)
NDV_VALUES=(100000 1000000 10000000)

for fpp in "${FPP_VALUES[@]}"; do
  for ndv in "${NDV_VALUES[@]}"; do
    COMDIR="$SWEEP/bloom/fpp_${fpp}-ndv_${ndv}"
    COMNAME="hb_fpp${fpp}_ndv${ndv}"
    echo "--- $COMNAME ---"
    rm -rf "$COMDIR"
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
python3 "$ROOT/scripts/analyze_sweep.py" "$SWEEP"

echo ""
echo "=== Done. Report: $SWEEP/param_sweep_report.md ==="
