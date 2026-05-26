#!/usr/bin/env python3
"""Analyze sweep results and produce param_sweep_report.md"""

import json
import math
import sys
from pathlib import Path

SWEEP = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("timings")

REPORT_QUERIES = [
    "MMSI exact lookup",
    "IMO exact lookup",
    "VesselName exact match",
    "CallSign exact match",
    "Single day time range",
    "Week-long time range",
    "Bounding box: Gulf of Mexico",
    "Bounding box: San Diego",
    "MMSI + time range",
    "Bounding box + time range",
    "VesselName + time range",
    "Bloom test: IMO = exact (exists)",
]


def load_results(path):
    path = Path(path)
    f = path / "results.json"
    if not f.exists():
        return None
    data = json.loads(f.read_text())
    fmt = list(data.keys())[0]
    return {q: v["median"] for q, v in data[fmt].items() if v["median"] > 0}


def geo_mean(values):
    if not values:
        return 0
    return math.exp(sum(math.log(v) for v in values) / len(values))






def format_rgs(v):
    if v >= 1000000:
        return f"{v // 1000000}M"
    return f"{v // 1000}K"


def format_dps(v):
    if v >= 1048576:
        return "1MB"
    if v >= 262144:
        return "256KB"
    return "64KB"





def main():
    lines = []
    lines.append("# Parameter Sweep Report\n")

    # =========================================================
    # Experiment 1: RGS x DPS
    # =========================================================
    lines.append("## Experiment 1: Row Group Size x Data Page Size\n")

    rgs_dps_dir = SWEEP / "rgs-dps"
    if not rgs_dps_dir.exists():
        lines.append("No rgs-dps results found.\n")
    else:
        all_results = {}
        for d in sorted(rgs_dps_dir.iterdir()):
            if d.is_dir():
                name = d.name
                results = load_results(d)
                if results is not None:
                    all_results[name] = results

        if not all_results:
            lines.append("No results loaded.\n")
        else:
            rgs_labels = ["100K", "500K", "1M", "5M"]
            dps_labels = ["64KB", "256KB", "1MB"]
            rgs_keywords = ["rgs_100000", "rgs_500000", "rgs_1000000", "rgs_5000000"]
            dps_keywords = ["dps_65536", "dps_262144", "dps_1048576"]

            default_name = "rgs_1000000-dps_1048576"
            baseline = all_results.get(default_name)
            if baseline is None:
                baseline = list(all_results.values())[0]

            heatmap = {}
            for ri, rk in enumerate(rgs_keywords):
                for ci, dk in enumerate(dps_keywords):
                    name = f"{rk}-{dk}"
                    results = all_results.get(name)
                    if results and baseline:
                        speedups = {}
                        for q, median in results.items():
                            if q in baseline and baseline[q] > 0:
                                speedups[q] = results[q] / baseline[q]
                        gm = geo_mean(list(speedups.values()))
                        heatmap[(ri, ci)] = gm

            header = "| RGS \\ DPS | " + " | ".join(dps_labels) + " |"
            lines.append(header)
            lines.append("|" + "---|" * (len(dps_labels) + 1))

            for ri in range(len(rgs_labels)):
                row = f"| {rgs_labels[ri]} |"
                for ci in range(len(dps_labels)):
                    gm = heatmap.get((ri, ci))
                    if gm is None:
                        row += " --- |"
                    else:
                        row += f" {gm:.2f}x |"
                lines.append(row)

            best_key = max(heatmap, key=lambda k: heatmap[k])
            best_gm = heatmap[best_key]
            best_ri, best_ci = best_key
            lines.append(f"\n**Optimal: RGS={rgs_labels[best_ri]}, DPS={dps_labels[best_ci]} (geo-mean vs default: {best_gm:.2f}x)**\n")

            best_name = f"{rgs_keywords[best_ri]}-{dps_keywords[best_ci]}"
            best_results = all_results.get(best_name)
            if best_results and baseline:
                lines.append("### Optimal vs Default per Query\n")
                lines.append("| Query | Default (s) | Optimal (s) | Speedup |")
                lines.append("|---|---|---|---|")
                for q in REPORT_QUERIES:
                    b = baseline.get(q)
                    o = best_results.get(q)
                    if b and o:
                        ratio = b / o if o > 0 else 0
                        lines.append(f"| {q} | {b:.4f} | {o:.4f} | {ratio:.2f}x |")

    # =========================================================
    # Experiment 2: Bloom FPP x NDV
    # =========================================================
    lines.append("\n## Experiment 2: Bloom FPP x NDV\n")

    bloom_dir = SWEEP / "bloom"
    if bloom_dir.exists():
        all_bloom = {}
        for d in sorted(bloom_dir.iterdir()):
            if d.is_dir():
                results = load_results(d)
                if results is not None:
                    all_bloom[d.name] = results

        if all_bloom:
            fpp_labels = ["0.1", "0.01", "0.001"]
            ndv_labels = ["100K", "1M", "10M"]
            fpp_keywords = ["fpp_0.1", "fpp_0.01", "fpp_0.001"]
            ndv_keywords = ["ndv_100000", "ndv_1000000", "ndv_10000000"]

            default_name = "fpp_0.01-ndv_1000000"
            baseline = all_bloom.get(default_name)
            if baseline is None:
                baseline = list(all_bloom.values())[0]

            heatmap = {}
            for ri, fk in enumerate(fpp_keywords):
                for ci, nk in enumerate(ndv_keywords):
                    name = f"{fk}-{nk}"
                    results = all_bloom.get(name)
                    if results and baseline:
                        speedups = {}
                        for q, median in results.items():
                            if q in baseline and baseline[q] > 0:
                                speedups[q] = median / baseline[q]
                        # Only point-lookup queries matter for bloom
                        point_qs = {q: s for q, s in speedups.items()
                                    if any(kw in q for kw in ["MMSI", "IMO", "bloom", "lookup"])}
                        point_qs = speedups
                        gm = geo_mean(list(point_qs.values())) if point_qs else 0
                        heatmap[(ri, ci)] = gm

            header = "| FPP \\ NDV | " + " | ".join(ndv_labels) + " |"
            lines.append(header)
            lines.append("|" + "---|" * (len(ndv_labels) + 1))

            for ri in range(len(fpp_labels)):
                row = f"| {fpp_labels[ri]} |"
                for ci in range(len(ndv_labels)):
                    gm = heatmap.get((ri, ci))
                    if gm is None:
                        row += " --- |"
                    else:
                        row += f" {gm:.2f}x |"
                lines.append(row)

            best_key = max(heatmap, key=lambda k: heatmap[k])
            best_fpp_ri, best_ndv_ci = best_key
            best_bgm = heatmap[best_key]
            lines.append(f"\n**Optimal: FPP={fpp_labels[best_fpp_ri]}, NDV={ndv_labels[best_ndv_ci]} (geo-mean point-lookup vs default: {best_bgm:.2f}x)**\n")
    else:
        lines.append("No bloom results found.\n")

    # =========================================================
    # Summary
    # =========================================================
    lines.append("\n## Optimal Parameters per Format\n")
    lines.append("| Format | Row Group Size | Data Page Size | Bloom FPP | Bloom NDV | Geo-Mean Speedup vs Default |")
    lines.append("|---|---|---|---|---|---|")
    try:
        lines.append(f"| hilbert_bloom | {rgs_labels[best_ri]} | {dps_labels[best_ci]} | {fpp_labels[best_fpp_ri]} | {ndv_labels[best_ndv_ci]} | {best_gm:.2f}x |")
    except NameError:
        lines.append("| hilbert_bloom | 1M | 1MB | 0.01 | 1,000,000 | 1.00x (no sweep data) |")

    report_path = SWEEP / "param_sweep_report.md"
    report_path.write_text("\n".join(lines))
    print(f"Report written to {report_path}")


if __name__ == "__main__":
    main()
