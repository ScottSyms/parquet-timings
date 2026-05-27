# AIS Rust Parquet Benchmark Report

## Formats

- **partition_hilbert_bloom**: `/Users/scottsyms/code/data/mc/mcdec/timings/combined/optimal` partition-pruned hilbert-pruned bloom-pruned

## Median Time Summary

| Query | partition_hilbert_bloom | Best | 2nd Best | 3rd Best | 4th Best | 5th Best |
|---|---|---|---|---|---|---|
| MMSI exact lookup | 0.4444s | partition_hilbert_bloom (0.4444s) | - | - | - | - |
| MMSI multi lookup (5 MMSIs) | 2.4459s | partition_hilbert_bloom (2.4459s) | - | - | - | - |
| IMO exact lookup | 0.6244s | partition_hilbert_bloom (0.6244s) | - | - | - | - |
| IMO multi lookup (4 IMOs) | 2.7307s | partition_hilbert_bloom (2.7307s) | - | - | - | - |
| VesselName exact match | 1.5414s | partition_hilbert_bloom (1.5414s) | - | - | - | - |
| VesselName LIKE prefix | 17.9605s | partition_hilbert_bloom (17.9605s) | - | - | - | - |
| CallSign exact match | 1.4712s | partition_hilbert_bloom (1.4712s) | - | - | - | - |
| Single day time range | 0.1461s | partition_hilbert_bloom (0.1461s) | - | - | - | - |
| Week-long time range | 0.9472s | partition_hilbert_bloom (0.9472s) | - | - | - | - |
| Month-long time range | 4.2276s | partition_hilbert_bloom (4.2276s) | - | - | - | - |
| Specific hour window | 0.1384s | partition_hilbert_bloom (0.1384s) | - | - | - | - |
| Bounding box: Gulf of Mexico | 14.2129s | partition_hilbert_bloom (14.2129s) | - | - | - | - |
| Bounding box: Pacific NW (Puget Sound) | 5.3716s | partition_hilbert_bloom (5.3716s) | - | - | - | - |
| Bounding box: New York Harbor | 4.0926s | partition_hilbert_bloom (4.0926s) | - | - | - | - |
| Bounding box: San Diego | 1.7872s | partition_hilbert_bloom (1.7872s) | - | - | - | - |
| MMSI + time range | 0.2956s | partition_hilbert_bloom (0.2956s) | - | - | - | - |
| Bounding box + time range | 1.0767s | partition_hilbert_bloom (1.0767s) | - | - | - | - |
| VesselName + time range | 0.4644s | partition_hilbert_bloom (0.4644s) | - | - | - | - |
| VesselType + bounding box | 12.8667s | partition_hilbert_bloom (12.8667s) | - | - | - | - |
| COUNT all rows | 0.0359s | partition_hilbert_bloom (0.0359s) | - | - | - | - |
| COUNT by VesselType | 35.2011s | partition_hilbert_bloom (35.2011s) | - | - | - | - |
| COUNT by day | 18.2347s | partition_hilbert_bloom (18.2347s) | - | - | - | - |
| Distinct MMSI count | 14.9825s | partition_hilbert_bloom (14.9825s) | - | - | - | - |
| Bloom test: CallSign = exact (exists) | 0.2844s | partition_hilbert_bloom (0.2844s) | - | - | - | - |
| Bloom test: CallSign = exact (not exists) | 0.0352s | partition_hilbert_bloom (0.0352s) | - | - | - | - |
| Bloom test: VesselName = exact (exists) | 1.3884s | partition_hilbert_bloom (1.3884s) | - | - | - | - |
| Bloom test: VesselName = exact (not exists) | 0.4034s | partition_hilbert_bloom (0.4034s) | - | - | - | - |
| Bloom test: IMO = exact (exists) | 0.5904s | partition_hilbert_bloom (0.5904s) | - | - | - | - |
| Bloom test: IMO = exact (not exists) | 6.0169s | partition_hilbert_bloom (6.0169s) | - | - | - | - |
| Bloom test: CallSign LIKE prefix | 17.3387s | partition_hilbert_bloom (17.3387s) | - | - | - | - |
| Bloom test: VesselName LIKE substring | 31.4159s | partition_hilbert_bloom (31.4159s) | - | - | - | - |

## Pruning Efficiency

### partition_hilbert_bloom

| Query | Total RGs | Stats Skip | Hilbert Skip | Bloom Skip | Decoded |
|---|---:|---:|---:|---:|---:|
| MMSI exact lookup | 2064 | 0% | 0% | 94% | 6% |
| MMSI multi lookup (5 MMSIs) | 2064 | 0% | 0% | 71% | 29% |
| IMO exact lookup | 2064 | 0% | 0% | 93% | 7% |
| IMO multi lookup (4 IMOs) | 2064 | 0% | 0% | 71% | 29% |
| VesselName exact match | 2064 | 0% | 0% | 83% | 17% |
| VesselName LIKE prefix | 2064 | 0% | 0% | 0% | 100% |
| CallSign exact match | 2064 | 0% | 0% | 83% | 17% |
| Single day time range | 22 | 0% | 0% | 0% | 100% |
| Week-long time range | 148 | 0% | 0% | 0% | 100% |
| Month-long time range | 668 | 0% | 0% | 0% | 100% |
| Specific hour window | 22 | 0% | 0% | 0% | 100% |
| Bounding box: Gulf of Mexico | 2064 | 43% | 0% | 0% | 57% |
| Bounding box: Pacific NW (Puget Sound) | 2064 | 67% | 0% | 0% | 32% |
| Bounding box: New York Harbor | 2064 | 62% | 9% | 0% | 29% |
| Bounding box: San Diego | 2064 | 74% | 8% | 0% | 18% |
| MMSI + time range | 668 | 0% | 0% | 93% | 7% |
| Bounding box + time range | 148 | 51% | 0% | 0% | 49% |
| VesselName + time range | 668 | 0% | 0% | 93% | 7% |
| VesselType + bounding box | 2064 | 43% | 0% | 0% | 57% |
| COUNT all rows | 2064 | 0% | 0% | 0% | 100% |
| COUNT by VesselType | 2064 | 0% | 0% | 0% | 100% |
| COUNT by day | 2064 | 0% | 0% | 0% | 100% |
| Distinct MMSI count | 2064 | 0% | 0% | 0% | 100% |
| Bloom test: CallSign = exact (exists) | 2064 | 0% | 0% | 96% | 4% |
| Bloom test: CallSign = exact (not exists) | 2064 | 100% | 0% | 0% | 0% |
| Bloom test: VesselName = exact (exists) | 2064 | 0% | 0% | 83% | 17% |
| Bloom test: VesselName = exact (not exists) | 2064 | 0% | 0% | 95% | 5% |
| Bloom test: IMO = exact (exists) | 2064 | 0% | 0% | 93% | 7% |
| Bloom test: IMO = exact (not exists) | 2064 | 0% | 0% | 38% | 62% |
| Bloom test: CallSign LIKE prefix | 2064 | 0% | 0% | 0% | 100% |
| Bloom test: VesselName LIKE substring | 2064 | 0% | 0% | 0% | 100% |


## Detailed Results

### partition_hilbert_bloom

| Category | Query | Count | Min | Mean | Median | Max | Files Scanned | RG Total | RG Stats Skip | RG Hilbert Skip | RG Bloom Skip | RG Decoded | Rows Decoded |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Point Lookups | MMSI exact lookup | 17745 | 0.4431 | 0.4437 | 0.4444 | 0.4444 | 366 | 2064 | 9 | 0 | 1935 | 120 | 30773347 |
| Point Lookups | MMSI multi lookup (5 MMSIs) | 154497 | 2.2553 | 2.3506 | 2.4459 | 2.4459 | 366 | 2064 | 0 | 0 | 1459 | 605 | 168738824 |
| Point Lookups | IMO exact lookup | 17745 | 0.5787 | 0.6016 | 0.6244 | 0.6244 | 366 | 2064 | 0 | 0 | 1918 | 146 | 34058447 |
| Point Lookups | IMO multi lookup (4 IMOs) | 102098 | 2.5835 | 2.6571 | 2.7307 | 2.7307 | 366 | 2064 | 0 | 0 | 1470 | 594 | 165081883 |
| Vessel Name / Callsign | VesselName exact match | 17745 | 1.4009 | 1.4712 | 1.5414 | 1.5414 | 366 | 2064 | 0 | 0 | 1723 | 341 | 61194987 |
| Vessel Name / Callsign | VesselName LIKE prefix | 1214334 | 17.3851 | 17.6728 | 17.9605 | 17.9605 | 366 | 2064 | 0 | 0 | 0 | 2064 | 743268520 |
| Vessel Name / Callsign | CallSign exact match | 33826 | 1.4343 | 1.4528 | 1.4712 | 1.4712 | 366 | 2064 | 0 | 0 | 1709 | 355 | 69944793 |
| Time Range | Single day time range | 8779885 | 0.1282 | 0.1372 | 0.1461 | 0.1461 | 1 | 22 | 0 | 0 | 0 | 22 | 8779885 |
| Time Range | Week-long time range | 58814886 | 0.8541 | 0.9006 | 0.9472 | 0.9472 | 7 | 148 | 0 | 0 | 0 | 148 | 58814886 |
| Time Range | Month-long time range | 264312029 | 3.8598 | 4.0437 | 4.2276 | 4.2276 | 31 | 668 | 0 | 0 | 0 | 668 | 264312029 |
| Time Range | Specific hour window | 360515 | 0.1378 | 0.1381 | 0.1384 | 0.1384 | 1 | 22 | 0 | 0 | 0 | 22 | 8779885 |
| Bounding Box | Bounding box: Gulf of Mexico | 277005218 | 12.8725 | 13.5427 | 14.2129 | 14.2129 | 366 | 2064 | 880 | 1 | 0 | 1183 | 403179181 |
| Bounding Box | Bounding box: Pacific NW (Puget Sound) | 67565901 | 5.0194 | 5.1955 | 5.3716 | 5.3716 | 366 | 2064 | 1392 | 2 | 0 | 670 | 194820640 |
| Bounding Box | Bounding box: New York Harbor | 21322627 | 3.6890 | 3.8908 | 4.0926 | 4.0926 | 366 | 2064 | 1280 | 178 | 0 | 606 | 163576681 |
| Bounding Box | Bounding box: San Diego | 12167112 | 1.7811 | 1.7842 | 1.7872 | 1.7872 | 366 | 2064 | 1537 | 162 | 0 | 365 | 71276668 |
| Combined Queries | MMSI + time range | 13174 | 0.2939 | 0.2947 | 0.2956 | 0.2956 | 31 | 668 | 3 | 0 | 619 | 46 | 18683222 |
| Combined Queries | Bounding box + time range | 11387087 | 1.0765 | 1.0766 | 1.0767 | 1.0767 | 7 | 148 | 76 | 0 | 0 | 72 | 29243304 |
| Combined Queries | VesselName + time range | 13174 | 0.4613 | 0.4628 | 0.4644 | 0.4644 | 31 | 668 | 0 | 0 | 621 | 47 | 19089379 |
| Combined Queries | VesselType + bounding box | 16729409 | 12.4934 | 12.6800 | 12.8667 | 12.8667 | 366 | 2064 | 880 | 1 | 0 | 1183 | 403179181 |
| Aggregations | COUNT all rows | 743268520 | 0.0348 | 0.0353 | 0.0359 | 0.0359 | 366 | 2064 | 0 | 0 | 0 | 2064 | 743268520 |
| Aggregations | COUNT by VesselType | 89 | 34.8718 | 35.0365 | 35.2011 | 35.2011 | 366 | 2064 | 0 | 0 | 0 | 2064 | 743268520 |
| Aggregations | COUNT by day | 366 | 17.3887 | 17.8117 | 18.2347 | 18.2347 | 366 | 2064 | 0 | 0 | 0 | 2064 | 743268520 |
| Aggregations | Distinct MMSI count | 80096 | 14.5545 | 14.7685 | 14.9825 | 14.9825 | 366 | 2064 | 0 | 0 | 0 | 2064 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: CallSign = exact (exists) | 0 | 0.2834 | 0.2839 | 0.2844 | 0.2844 | 366 | 2064 | 0 | 0 | 1989 | 75 | 9437586 |
| Bloom Filter Effectiveness | Bloom test: CallSign = exact (not exists) | 0 | 0.0345 | 0.0348 | 0.0352 | 0.0352 | 366 | 2064 | 2058 | 0 | 6 | 0 | 0 |
| Bloom Filter Effectiveness | Bloom test: VesselName = exact (exists) | 17745 | 1.3831 | 1.3858 | 1.3884 | 1.3884 | 366 | 2064 | 0 | 0 | 1723 | 341 | 61194987 |
| Bloom Filter Effectiveness | Bloom test: VesselName = exact (not exists) | 0 | 0.3999 | 0.4016 | 0.4034 | 0.4034 | 366 | 2064 | 0 | 0 | 1953 | 111 | 13953399 |
| Bloom Filter Effectiveness | Bloom test: IMO = exact (exists) | 17745 | 0.5738 | 0.5821 | 0.5904 | 0.5904 | 366 | 2064 | 0 | 0 | 1918 | 146 | 34058447 |
| Bloom Filter Effectiveness | Bloom test: IMO = exact (not exists) | 1406519 | 5.9711 | 5.9940 | 6.0169 | 6.0169 | 366 | 2064 | 0 | 0 | 793 | 1271 | 436337649 |
| Bloom Filter Effectiveness | Bloom test: CallSign LIKE prefix | 40733731 | 17.0645 | 17.2016 | 17.3387 | 17.3387 | 366 | 2064 | 0 | 0 | 0 | 2064 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: VesselName LIKE substring | 3694215 | 31.3900 | 31.4030 | 31.4159 | 31.4159 | 366 | 2064 | 0 | 0 | 0 | 2064 | 743268520 |

