#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GEN="$ROOT/target/release/ais-parquet-optimizer"
BENCH="$ROOT/target/release/benchmark"
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
      --only hilbert-bloom \
      --hilbert-bloom-output "$COMDIR" \
      --row-group-size "$rgs" \
      --data-page-size "$dps" \
      --max-rows "$MAX_ROWS"
    "$BENCH" \
      --source "$SOURCE" \
      --partitioned-hilbert-bloom "$COMDIR" \
      --report "$COMDIR/report.md" \
      --json-results "$COMDIR/results.json" \
      --only-format hilbert-bloom \
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
      --only hilbert-bloom \
      --hilbert-bloom-output "$COMDIR" \
      --bloom-fpp "$fpp" \
      --bloom-ndv "$ndv" \
      --max-rows "$MAX_ROWS"
    "$BENCH" \
      --source "$SOURCE" \
      --partitioned-hilbert-bloom "$COMDIR" \
      --report "$COMDIR/report.md" \
      --json-results "$COMDIR/results.json" \
      --only-format hilbert-bloom \
      --warmup-runs 1 --timed-runs 2
  done
done

echo ""
echo "=== Experiment 3: Combined optimal (all params, per-column NDV) ==="
BEST_RGS=5000000
BEST_DPS=65536
BEST_FPP=0.001
BEST_NDV=10000000
COMBINED_DIR="$TIMINGS/combined"
if [ -d "$COMBINED_DIR" ]; then
  echo "  SKIP (exists): $COMBINED_DIR"
else
  mkdir -p "$(dirname "$COMBINED_DIR")"
  echo "--- combined: RGS=$BEST_RGS DPS=$BEST_DPS FPP=$BEST_FPP NDV=$BEST_NDV + per-column NDV ---"
  echo "--- including: baseline default for comparison ---"
  BASELINE_DIR="$TIMINGS/combined/baseline"
  OPTIMAL_DIR="$TIMINGS/combined/optimal"
  mkdir -p "$BASELINE_DIR" "$OPTIMAL_DIR"
  echo "  generating baseline (default params + smart-rgs)..."
  "$GEN" \
    --source "$SOURCE" \
    --only hilbert-bloom \
    --hilbert-bloom-output "$BASELINE_DIR" \
    --max-rows "$MAX_ROWS" \
    --smart-rgs \
    --overwrite
  echo "  benchmarking baseline..."
  "$BENCH" \
    --source "$SOURCE" \
    --partitioned-hilbert-bloom "$BASELINE_DIR" \
    --report "$BASELINE_DIR/report.md" \
    --json-results "$BASELINE_DIR/results.json" \
    --only-format hilbert-bloom \
    --warmup-runs 1 --timed-runs 2
  echo "  generating optimal (tuned params + per-column NDV + smart-rgs)..."
  "$GEN" \
    --source "$SOURCE" \
    --only hilbert-bloom \
    --hilbert-bloom-output "$OPTIMAL_DIR" \
    --row-group-size "$BEST_RGS" \
    --data-page-size "$BEST_DPS" \
    --bloom-fpp "$BEST_FPP" \
    --bloom-ndv "$BEST_NDV" \
    --bloom-ndv-per-column MMSI=50000 \
    --bloom-ndv-per-column IMO=1000 \
    --bloom-ndv-per-column CallSign=2000 \
    --bloom-ndv-per-column VesselName=2000 \
    --bloom-ndv-per-column VesselType=100 \
    --max-rows "$MAX_ROWS" \
    --smart-rgs \
    --overwrite
  echo "  benchmarking optimal..."
  "$BENCH" \
    --source "$SOURCE" \
    --partitioned-hilbert-bloom "$OPTIMAL_DIR" \
    --report "$OPTIMAL_DIR/report.md" \
    --json-results "$OPTIMAL_DIR/results.json" \
    --only-format hilbert-bloom \
    --warmup-runs 1 --timed-runs 2
  echo "  combined validation done"
fi

echo ""
echo "=== Step: Analyze results ==="
python3 "$ROOT/scripts/analyze_sweep.py" "$TIMINGS"

echo ""
echo "=== Done. Report: $TIMINGS/param_sweep_report.md ==="
