# AIS Rust Parquet Benchmark Report

## Formats

- **mono** (monolithic): `parquet/ais_2024.parquet`
- **part** (partitioned): `partition_parquet` partition-pruned
- **part_bloom** (partitioned_bloom): `partition_bloom_parquet` partition-pruned bloom-pruned
- **hilbert** (partition_hilbert): `partition_hilbert_parquet` partition-pruned hilbert-pruned
- **hilbert_bloom** (partition_hilbert_bloom): `partition_hilbert_bloom_parquet` partition-pruned hilbert-pruned bloom-pruned

## Median Time Summary

| Query | mono | part | part_bloom | hilbert | hilbert_bloom | Best | 2nd Best | 3rd Best | 4th Best | 5th Best |
|---|---|---|---|---|---|---|---|---|---|---|
| MMSI exact lookup | 8.13s | 8.14s | 3.07s | 8.13s | 1.00s | hilbert_bloom (8.10x) | part_bloom (2.65x) | equiv | equiv | equiv |
| MMSI multi lookup (5 MMSIs) | 9.04s | 9.04s | 9.38s | 9.03s | 3.66s | hilbert_bloom (2.47x) | equiv | equiv | equiv | equiv |
| IMO exact lookup | 13.27s | 13.27s | 4.79s | 10.07s | 1.15s | hilbert_bloom (11.53x) | part_bloom (2.77x) | hilbert (1.32x) | equiv | equiv |
| IMO multi lookup (4 IMOs) | 13.77s | 13.78s | 13.88s | 10.66s | 4.29s | hilbert_bloom (3.21x) | hilbert (1.29x) | equiv | equiv | equiv |
| VesselName exact match | 19.53s | 17.35s | 17.67s | 13.94s | 14.08s | hilbert (1.40x) | hilbert_bloom (1.39x) | part (1.13x) | part_bloom (1.11x) | equiv |
| VesselName LIKE prefix | 22.43s | 20.76s | 20.64s | 17.27s | 16.95s | hilbert_bloom (1.32x) | hilbert (1.30x) | part_bloom (1.09x) | part (1.08x) | equiv |
| CallSign exact match | 17.12s | 15.59s | 15.92s | 13.05s | 13.17s | hilbert (1.31x) | hilbert_bloom (1.30x) | part (1.10x) | part_bloom (1.08x) | equiv |
| Single day time range | 0.15s | 0.10s | 0.10s | 0.12s | 0.11s | equiv | equiv | equiv | equiv | equiv |
| Week-long time range | 0.85s | 0.69s | 0.69s | 0.78s | 0.77s | equiv | equiv | equiv | equiv | equiv |
| Month-long time range | 3.75s | 3.11s | 3.09s | 3.53s | 3.46s | equiv | equiv | equiv | equiv | equiv |
| Specific hour window | 0.14s | 0.01s | 0.01s | 0.13s | 0.12s | equiv | equiv | equiv | equiv | equiv |
| Bounding box: Gulf of Mexico | 26.16s | 22.45s | 22.33s | 13.35s | 13.04s | hilbert_bloom (2.01x) | hilbert (1.96x) | part_bloom (1.17x) | part (1.17x) | equiv |
| Bounding box: Pacific NW (Puget Sound) | 16.44s | 16.14s | 16.14s | 6.65s | 6.49s | hilbert_bloom (2.53x) | hilbert (2.47x) | equiv | equiv | equiv |
| Bounding box: New York Harbor | 17.03s | 16.63s | 16.64s | 7.17s | 6.99s | hilbert_bloom (2.44x) | hilbert (2.37x) | equiv | equiv | equiv |
| Bounding box: San Diego | 18.00s | 17.48s | 17.49s | 5.90s | 5.76s | hilbert_bloom (3.13x) | hilbert (3.05x) | equiv | equiv | equiv |
| MMSI + time range | 3.65s | 3.11s | 2.33s | 3.50s | 0.60s | hilbert_bloom (6.04x) | part_bloom (1.56x) | equiv | equiv | equiv |
| Bounding box + time range | 2.09s | 1.93s | 1.92s | 1.10s | 1.10s | equiv | equiv | equiv | equiv | equiv |
| VesselName + time range | 7.67s | 6.44s | 6.53s | 5.66s | 5.66s | hilbert (1.36x) | hilbert_bloom (1.35x) | part (1.19x) | part_bloom (1.17x) | equiv |
| VesselType + bounding box | 23.67s | 24.25s | 24.47s | 13.28s | 13.33s | hilbert (1.78x) | hilbert_bloom (1.78x) | equiv | equiv | equiv |
| COUNT all rows | 0.01s | 0.02s | 0.02s | 0.02s | 0.02s | equiv | equiv | equiv | equiv | equiv |
| COUNT by VesselType | 34.69s | 36.28s | 36.04s | 34.95s | 34.31s | equiv | equiv | equiv | part_bloom (0.96x) | part (0.96x) |
| COUNT by day | 17.06s | 15.46s | 15.20s | 16.53s | 16.28s | part_bloom (1.12x) | part (1.10x) | equiv | equiv | equiv |
| Distinct MMSI count | 16.41s | 16.73s | 16.23s | 14.72s | 14.15s | hilbert_bloom (1.16x) | hilbert (1.11x) | equiv | equiv | equiv |
| Bloom test: CallSign = exact (exists) | 16.57s | 15.44s | 15.89s | 13.03s | 13.19s | hilbert (1.27x) | hilbert_bloom (1.26x) | part (1.07x) | equiv | equiv |
| Bloom test: CallSign = exact (not exists) | 0.26s | 0.15s | 0.15s | 0.12s | 0.12s | equiv | equiv | equiv | equiv | equiv |
| Bloom test: VesselName = exact (exists) | 18.86s | 17.24s | 17.67s | 13.96s | 14.10s | hilbert (1.35x) | hilbert_bloom (1.34x) | part (1.09x) | part_bloom (1.07x) | equiv |
| Bloom test: VesselName = exact (not exists) | 18.71s | 16.86s | 17.25s | 13.73s | 13.89s | hilbert (1.36x) | hilbert_bloom (1.35x) | part (1.11x) | part_bloom (1.08x) | equiv |
| Bloom test: IMO = exact (exists) | 13.31s | 13.28s | 4.81s | 9.99s | 1.16s | hilbert_bloom (11.43x) | part_bloom (2.77x) | hilbert (1.33x) | equiv | equiv |
| Bloom test: IMO = exact (not exists) | 13.29s | 13.24s | 13.78s | 9.97s | 8.03s | hilbert_bloom (1.65x) | hilbert (1.33x) | equiv | equiv | equiv |
| Bloom test: CallSign LIKE prefix | 20.82s | 19.67s | 19.74s | 16.78s | 16.67s | hilbert_bloom (1.25x) | hilbert (1.24x) | part (1.06x) | part_bloom (1.05x) | equiv |
| Bloom test: VesselName LIKE substring | 43.95s | 40.17s | 40.15s | 31.01s | 30.78s | hilbert_bloom (1.43x) | hilbert (1.42x) | part_bloom (1.09x) | part (1.09x) | equiv |

## Detailed Results

### monolithic

| Category | Query | Count | Min | Mean | Median | Max | Files Scanned | RG Total | RG Stats Skip | RG Hilbert Skip | RG Bloom Skip | RG Decoded | Rows Decoded |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Point Lookups | MMSI exact lookup | 17745 | 8.1268 | 8.1303 | 8.1339 | 8.1339 | 1 | 744 | 0 | 0 | 0 | 744 | 743268520 |
| Point Lookups | MMSI multi lookup (5 MMSIs) | 154497 | 9.0348 | 9.0360 | 9.0372 | 9.0372 | 1 | 744 | 0 | 0 | 0 | 744 | 743268520 |
| Point Lookups | IMO exact lookup | 17745 | 13.2586 | 13.2658 | 13.2730 | 13.2730 | 1 | 744 | 0 | 0 | 0 | 744 | 743268520 |
| Point Lookups | IMO multi lookup (4 IMOs) | 102098 | 13.7617 | 13.7643 | 13.7670 | 13.7670 | 1 | 744 | 0 | 0 | 0 | 744 | 743268520 |
| Vessel Name / Callsign | VesselName exact match | 17745 | 19.1527 | 19.3412 | 19.5296 | 19.5296 | 1 | 744 | 0 | 0 | 0 | 744 | 743268520 |
| Vessel Name / Callsign | VesselName LIKE prefix | 1214334 | 22.4107 | 22.4225 | 22.4343 | 22.4343 | 1 | 744 | 0 | 0 | 0 | 744 | 743268520 |
| Vessel Name / Callsign | CallSign exact match | 33826 | 16.5251 | 16.8219 | 17.1186 | 17.1186 | 1 | 744 | 0 | 0 | 0 | 744 | 743268520 |
| Time Range | Single day time range | 8779885 | 0.1406 | 0.1437 | 0.1469 | 0.1469 | 1 | 744 | 735 | 0 | 0 | 9 | 9000000 |
| Time Range | Week-long time range | 58814886 | 0.8296 | 0.8391 | 0.8487 | 0.8487 | 1 | 744 | 685 | 0 | 0 | 59 | 59000000 |
| Time Range | Month-long time range | 264312029 | 3.6225 | 3.6883 | 3.7541 | 3.7541 | 1 | 744 | 479 | 0 | 0 | 265 | 265000000 |
| Time Range | Specific hour window | 360515 | 0.1396 | 0.1397 | 0.1397 | 0.1397 | 1 | 744 | 735 | 0 | 0 | 9 | 9000000 |
| Bounding Box | Bounding box: Gulf of Mexico | 277005218 | 23.0028 | 24.5838 | 26.1648 | 26.1648 | 1 | 744 | 0 | 0 | 0 | 744 | 743268520 |
| Bounding Box | Bounding box: Pacific NW (Puget Sound) | 67565901 | 16.4137 | 16.4275 | 16.4412 | 16.4412 | 1 | 744 | 0 | 0 | 0 | 744 | 743268520 |
| Bounding Box | Bounding box: New York Harbor | 21322627 | 16.9105 | 16.9688 | 17.0272 | 17.0272 | 1 | 744 | 0 | 0 | 0 | 744 | 743268520 |
| Bounding Box | Bounding box: San Diego | 12167112 | 17.8103 | 17.9034 | 17.9965 | 17.9965 | 1 | 744 | 0 | 0 | 0 | 744 | 743268520 |
| Combined Queries | MMSI + time range | 13174 | 3.6429 | 3.6445 | 3.6461 | 3.6461 | 1 | 744 | 479 | 0 | 0 | 265 | 265000000 |
| Combined Queries | Bounding box + time range | 11387087 | 2.0889 | 2.0900 | 2.0911 | 2.0911 | 1 | 744 | 685 | 0 | 0 | 59 | 59000000 |
| Combined Queries | VesselName + time range | 13174 | 7.6679 | 7.6689 | 7.6699 | 7.6699 | 1 | 744 | 479 | 0 | 0 | 265 | 265000000 |
| Combined Queries | VesselType + bounding box | 16729409 | 22.6349 | 23.1515 | 23.6680 | 23.6680 | 1 | 744 | 0 | 0 | 0 | 744 | 743268520 |
| Aggregations | COUNT all rows | 743268520 | 0.0090 | 0.0093 | 0.0096 | 0.0096 | 1 | 744 | 0 | 0 | 0 | 744 | 743268520 |
| Aggregations | COUNT by VesselType | 89 | 34.6848 | 34.6894 | 34.6940 | 34.6940 | 1 | 744 | 0 | 0 | 0 | 744 | 743268520 |
| Aggregations | COUNT by day | 366 | 16.6941 | 16.8750 | 17.0559 | 17.0559 | 1 | 744 | 0 | 0 | 0 | 744 | 743268520 |
| Aggregations | Distinct MMSI count | 80096 | 16.3455 | 16.3780 | 16.4104 | 16.4104 | 1 | 744 | 0 | 0 | 0 | 744 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: CallSign = exact (exists) | 0 | 16.4624 | 16.5149 | 16.5674 | 16.5674 | 1 | 744 | 0 | 0 | 0 | 744 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: CallSign = exact (not exists) | 0 | 0.2627 | 0.2629 | 0.2631 | 0.2631 | 1 | 744 | 731 | 0 | 0 | 13 | 13000000 |
| Bloom Filter Effectiveness | Bloom test: VesselName = exact (exists) | 17745 | 18.8379 | 18.8491 | 18.8604 | 18.8604 | 1 | 744 | 0 | 0 | 0 | 744 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: VesselName = exact (not exists) | 0 | 18.3317 | 18.5200 | 18.7082 | 18.7082 | 1 | 744 | 0 | 0 | 0 | 744 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: IMO = exact (exists) | 17745 | 13.2665 | 13.2875 | 13.3084 | 13.3084 | 1 | 744 | 0 | 0 | 0 | 744 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: IMO = exact (not exists) | 1406519 | 13.2683 | 13.2779 | 13.2875 | 13.2875 | 1 | 744 | 0 | 0 | 0 | 744 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: CallSign LIKE prefix | 40733731 | 20.7988 | 20.8076 | 20.8164 | 20.8164 | 1 | 744 | 0 | 0 | 0 | 744 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: VesselName LIKE substring | 3694215 | 43.8832 | 43.9155 | 43.9479 | 43.9479 | 1 | 744 | 0 | 0 | 0 | 744 | 743268520 |

### partitioned

| Category | Query | Count | Min | Mean | Median | Max | Files Scanned | RG Total | RG Stats Skip | RG Hilbert Skip | RG Bloom Skip | RG Decoded | Rows Decoded |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Point Lookups | MMSI exact lookup | 17745 | 8.1373 | 8.1376 | 8.1378 | 8.1378 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Point Lookups | MMSI multi lookup (5 MMSIs) | 154497 | 9.0388 | 9.0388 | 9.0388 | 9.0388 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Point Lookups | IMO exact lookup | 17745 | 13.2622 | 13.2644 | 13.2666 | 13.2666 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Point Lookups | IMO multi lookup (4 IMOs) | 102098 | 13.7645 | 13.7736 | 13.7827 | 13.7827 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Vessel Name / Callsign | VesselName exact match | 17745 | 17.3450 | 17.3499 | 17.3547 | 17.3547 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Vessel Name / Callsign | VesselName LIKE prefix | 1214334 | 20.7432 | 20.7497 | 20.7563 | 20.7563 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Vessel Name / Callsign | CallSign exact match | 33826 | 15.5760 | 15.5817 | 15.5874 | 15.5874 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Time Range | Single day time range | 8779885 | 0.1019 | 0.1023 | 0.1026 | 0.1026 | 1 | 9 | 0 | 0 | 0 | 9 | 8779885 |
| Time Range | Week-long time range | 58814886 | 0.6889 | 0.6905 | 0.6920 | 0.6920 | 7 | 63 | 0 | 0 | 0 | 63 | 58814886 |
| Time Range | Month-long time range | 264312029 | 3.1083 | 3.1097 | 3.1111 | 3.1111 | 31 | 282 | 0 | 0 | 0 | 282 | 264312029 |
| Time Range | Specific hour window | 360515 | 0.0118 | 0.0118 | 0.0119 | 0.0119 | 1 | 9 | 8 | 0 | 0 | 1 | 1000000 |
| Bounding Box | Bounding box: Gulf of Mexico | 277005218 | 22.3472 | 22.3997 | 22.4521 | 22.4521 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bounding Box | Bounding box: Pacific NW (Puget Sound) | 67565901 | 16.1279 | 16.1357 | 16.1435 | 16.1435 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bounding Box | Bounding box: New York Harbor | 21322627 | 16.5973 | 16.6133 | 16.6294 | 16.6294 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bounding Box | Bounding box: San Diego | 12167112 | 17.4343 | 17.4556 | 17.4770 | 17.4770 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Combined Queries | MMSI + time range | 13174 | 3.1040 | 3.1073 | 3.1106 | 3.1106 | 31 | 282 | 0 | 0 | 0 | 282 | 264312029 |
| Combined Queries | Bounding box + time range | 11387087 | 1.9228 | 1.9243 | 1.9257 | 1.9257 | 7 | 63 | 0 | 0 | 0 | 63 | 58814886 |
| Combined Queries | VesselName + time range | 13174 | 6.4395 | 6.4415 | 6.4435 | 6.4435 | 31 | 282 | 0 | 0 | 0 | 282 | 264312029 |
| Combined Queries | VesselType + bounding box | 16729409 | 23.9728 | 24.1094 | 24.2459 | 24.2459 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Aggregations | COUNT all rows | 743268520 | 0.0191 | 0.0209 | 0.0227 | 0.0227 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Aggregations | COUNT by VesselType | 89 | 36.1285 | 36.2067 | 36.2850 | 36.2850 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Aggregations | COUNT by day | 366 | 15.3034 | 15.3807 | 15.4581 | 15.4581 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Aggregations | Distinct MMSI count | 80096 | 16.2544 | 16.4900 | 16.7256 | 16.7256 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: CallSign = exact (exists) | 0 | 15.4395 | 15.4404 | 15.4413 | 15.4413 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: CallSign = exact (not exists) | 0 | 0.1445 | 0.1457 | 0.1470 | 0.1470 | 366 | 1034 | 1027 | 0 | 0 | 7 | 7000000 |
| Bloom Filter Effectiveness | Bloom test: VesselName = exact (exists) | 17745 | 17.2180 | 17.2304 | 17.2428 | 17.2428 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: VesselName = exact (not exists) | 0 | 16.8099 | 16.8330 | 16.8561 | 16.8561 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: IMO = exact (exists) | 17745 | 13.2292 | 13.2562 | 13.2831 | 13.2831 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: IMO = exact (not exists) | 1406519 | 13.2267 | 13.2313 | 13.2358 | 13.2358 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: CallSign LIKE prefix | 40733731 | 19.6654 | 19.6691 | 19.6728 | 19.6728 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: VesselName LIKE substring | 3694215 | 40.0900 | 40.1277 | 40.1653 | 40.1653 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |

### partitioned_bloom

| Category | Query | Count | Min | Mean | Median | Max | Files Scanned | RG Total | RG Stats Skip | RG Hilbert Skip | RG Bloom Skip | RG Decoded | Rows Decoded |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Point Lookups | MMSI exact lookup | 17745 | 3.0705 | 3.0714 | 3.0722 | 3.0722 | 366 | 1034 | 0 | 0 | 717 | 317 | 246493677 |
| Point Lookups | MMSI multi lookup (5 MMSIs) | 154497 | 9.3616 | 9.3692 | 9.3767 | 9.3767 | 366 | 1034 | 0 | 0 | 2 | 1032 | 743137016 |
| Point Lookups | IMO exact lookup | 17745 | 4.7713 | 4.7808 | 4.7903 | 4.7903 | 366 | 1034 | 0 | 0 | 717 | 317 | 246493677 |
| Point Lookups | IMO multi lookup (4 IMOs) | 102098 | 13.8706 | 13.8739 | 13.8771 | 13.8771 | 366 | 1034 | 0 | 0 | 24 | 1010 | 730530062 |
| Vessel Name / Callsign | VesselName exact match | 17745 | 17.6434 | 17.6559 | 17.6683 | 17.6683 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Vessel Name / Callsign | VesselName LIKE prefix | 1214334 | 20.6360 | 20.6364 | 20.6369 | 20.6369 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Vessel Name / Callsign | CallSign exact match | 33826 | 15.9084 | 15.9141 | 15.9199 | 15.9199 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Time Range | Single day time range | 8779885 | 0.1022 | 0.1026 | 0.1030 | 0.1030 | 1 | 9 | 0 | 0 | 0 | 9 | 8779885 |
| Time Range | Week-long time range | 58814886 | 0.6889 | 0.6895 | 0.6901 | 0.6901 | 7 | 63 | 0 | 0 | 0 | 63 | 58814886 |
| Time Range | Month-long time range | 264312029 | 3.0905 | 3.0914 | 3.0922 | 3.0922 | 31 | 282 | 0 | 0 | 0 | 282 | 264312029 |
| Time Range | Specific hour window | 360515 | 0.0118 | 0.0118 | 0.0118 | 0.0118 | 1 | 9 | 8 | 0 | 0 | 1 | 1000000 |
| Bounding Box | Bounding box: Gulf of Mexico | 277005218 | 22.3124 | 22.3195 | 22.3266 | 22.3266 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bounding Box | Bounding box: Pacific NW (Puget Sound) | 67565901 | 16.1001 | 16.1224 | 16.1446 | 16.1446 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bounding Box | Bounding box: New York Harbor | 21322627 | 16.6254 | 16.6352 | 16.6450 | 16.6450 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bounding Box | Bounding box: San Diego | 12167112 | 17.4450 | 17.4694 | 17.4938 | 17.4938 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Combined Queries | MMSI + time range | 13174 | 2.3274 | 2.3305 | 2.3336 | 2.3336 | 31 | 282 | 0 | 0 | 81 | 201 | 189585029 |
| Combined Queries | Bounding box + time range | 11387087 | 1.9123 | 1.9153 | 1.9183 | 1.9183 | 7 | 63 | 0 | 0 | 0 | 63 | 58814886 |
| Combined Queries | VesselName + time range | 13174 | 6.5207 | 6.5252 | 6.5296 | 6.5296 | 31 | 282 | 0 | 0 | 0 | 282 | 264312029 |
| Combined Queries | VesselType + bounding box | 16729409 | 24.4341 | 24.4514 | 24.4687 | 24.4687 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Aggregations | COUNT all rows | 743268520 | 0.0195 | 0.0202 | 0.0209 | 0.0209 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Aggregations | COUNT by VesselType | 89 | 36.0371 | 36.0380 | 36.0389 | 36.0389 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Aggregations | COUNT by day | 366 | 15.1692 | 15.1846 | 15.2001 | 15.2001 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Aggregations | Distinct MMSI count | 80096 | 16.1679 | 16.1979 | 16.2278 | 16.2278 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: CallSign = exact (exists) | 0 | 15.8911 | 15.8914 | 15.8918 | 15.8918 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: CallSign = exact (not exists) | 0 | 0.1484 | 0.1492 | 0.1501 | 0.1501 | 366 | 1034 | 1027 | 0 | 0 | 7 | 7000000 |
| Bloom Filter Effectiveness | Bloom test: VesselName = exact (exists) | 17745 | 17.6459 | 17.6573 | 17.6688 | 17.6688 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: VesselName = exact (not exists) | 0 | 17.1955 | 17.2204 | 17.2453 | 17.2453 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: IMO = exact (exists) | 17745 | 4.7941 | 4.8004 | 4.8068 | 4.8068 | 366 | 1034 | 0 | 0 | 717 | 317 | 246493677 |
| Bloom Filter Effectiveness | Bloom test: IMO = exact (not exists) | 1406519 | 13.6917 | 13.7374 | 13.7830 | 13.7830 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: CallSign LIKE prefix | 40733731 | 19.7293 | 19.7336 | 19.7380 | 19.7380 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: VesselName LIKE substring | 3694215 | 40.0819 | 40.1168 | 40.1518 | 40.1518 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |

### partition_hilbert

| Category | Query | Count | Min | Mean | Median | Max | Files Scanned | RG Total | RG Stats Skip | RG Hilbert Skip | RG Bloom Skip | RG Decoded | Rows Decoded |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Point Lookups | MMSI exact lookup | 17745 | 8.1245 | 8.1285 | 8.1325 | 8.1325 | 366 | 1034 | 7 | 0 | 0 | 1027 | 742877080 |
| Point Lookups | MMSI multi lookup (5 MMSIs) | 154497 | 9.0213 | 9.0235 | 9.0257 | 9.0257 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Point Lookups | IMO exact lookup | 17745 | 10.0663 | 10.0697 | 10.0731 | 10.0731 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Point Lookups | IMO multi lookup (4 IMOs) | 102098 | 10.6476 | 10.6519 | 10.6563 | 10.6563 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Vessel Name / Callsign | VesselName exact match | 17745 | 13.9369 | 13.9380 | 13.9391 | 13.9391 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Vessel Name / Callsign | VesselName LIKE prefix | 1214334 | 17.2661 | 17.2702 | 17.2744 | 17.2744 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Vessel Name / Callsign | CallSign exact match | 33826 | 13.0227 | 13.0365 | 13.0503 | 13.0503 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Time Range | Single day time range | 8779885 | 0.1167 | 0.1169 | 0.1171 | 0.1171 | 1 | 9 | 0 | 0 | 0 | 9 | 8779885 |
| Time Range | Week-long time range | 58814886 | 0.7833 | 0.7836 | 0.7840 | 0.7840 | 7 | 63 | 0 | 0 | 0 | 63 | 58814886 |
| Time Range | Month-long time range | 264312029 | 3.5229 | 3.5241 | 3.5254 | 3.5254 | 31 | 282 | 0 | 0 | 0 | 282 | 264312029 |
| Time Range | Specific hour window | 360515 | 0.1260 | 0.1267 | 0.1273 | 0.1273 | 1 | 9 | 0 | 0 | 0 | 9 | 8779885 |
| Bounding Box | Bounding box: Gulf of Mexico | 277005218 | 13.3423 | 13.3471 | 13.3519 | 13.3519 | 366 | 1034 | 360 | 0 | 0 | 674 | 431231724 |
| Bounding Box | Bounding box: Pacific NW (Puget Sound) | 67565901 | 6.6343 | 6.6414 | 6.6486 | 6.6486 | 366 | 1034 | 510 | 0 | 0 | 524 | 281231724 |
| Bounding Box | Bounding box: New York Harbor | 21322627 | 7.1583 | 7.1647 | 7.1710 | 7.1710 | 366 | 1034 | 416 | 0 | 0 | 618 | 345818340 |
| Bounding Box | Bounding box: San Diego | 12167112 | 5.8694 | 5.8846 | 5.8998 | 5.8998 | 366 | 1034 | 501 | 0 | 0 | 533 | 290231724 |
| Combined Queries | MMSI + time range | 13174 | 3.4996 | 3.5009 | 3.5022 | 3.5022 | 31 | 282 | 1 | 0 | 0 | 281 | 264171817 |
| Combined Queries | Bounding box + time range | 11387087 | 1.0950 | 1.0961 | 1.0972 | 1.0972 | 7 | 63 | 31 | 0 | 0 | 32 | 32000000 |
| Combined Queries | VesselName + time range | 13174 | 5.6503 | 5.6544 | 5.6584 | 5.6584 | 31 | 282 | 0 | 0 | 0 | 282 | 264312029 |
| Combined Queries | VesselType + bounding box | 16729409 | 13.2753 | 13.2772 | 13.2790 | 13.2790 | 366 | 1034 | 360 | 0 | 0 | 674 | 431231724 |
| Aggregations | COUNT all rows | 743268520 | 0.0204 | 0.0209 | 0.0214 | 0.0214 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Aggregations | COUNT by VesselType | 89 | 34.9074 | 34.9290 | 34.9506 | 34.9506 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Aggregations | COUNT by day | 366 | 16.5241 | 16.5279 | 16.5316 | 16.5316 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Aggregations | Distinct MMSI count | 80096 | 14.4367 | 14.5795 | 14.7224 | 14.7224 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: CallSign = exact (exists) | 0 | 12.9907 | 13.0118 | 13.0328 | 13.0328 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: CallSign = exact (not exists) | 0 | 0.1149 | 0.1159 | 0.1168 | 0.1168 | 366 | 1034 | 1028 | 0 | 0 | 6 | 6000000 |
| Bloom Filter Effectiveness | Bloom test: VesselName = exact (exists) | 17745 | 13.9432 | 13.9531 | 13.9631 | 13.9631 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: VesselName = exact (not exists) | 0 | 13.6968 | 13.7110 | 13.7252 | 13.7252 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: IMO = exact (exists) | 17745 | 9.9145 | 9.9500 | 9.9855 | 9.9855 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: IMO = exact (not exists) | 1406519 | 9.9696 | 9.9699 | 9.9702 | 9.9702 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: CallSign LIKE prefix | 40733731 | 16.7064 | 16.7433 | 16.7801 | 16.7801 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: VesselName LIKE substring | 3694215 | 30.9418 | 30.9761 | 31.0104 | 31.0104 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |

### partition_hilbert_bloom

| Category | Query | Count | Min | Mean | Median | Max | Files Scanned | RG Total | RG Stats Skip | RG Hilbert Skip | RG Bloom Skip | RG Decoded | Rows Decoded |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Point Lookups | MMSI exact lookup | 17745 | 1.0040 | 1.0042 | 1.0044 | 1.0044 | 366 | 1034 | 7 | 0 | 914 | 113 | 57028555 |
| Point Lookups | MMSI multi lookup (5 MMSIs) | 154497 | 3.6556 | 3.6578 | 3.6600 | 3.6600 | 366 | 1034 | 0 | 0 | 523 | 511 | 269103416 |
| Point Lookups | IMO exact lookup | 17745 | 1.1504 | 1.1509 | 1.1513 | 1.1513 | 366 | 1034 | 0 | 0 | 921 | 113 | 57028555 |
| Point Lookups | IMO multi lookup (4 IMOs) | 102098 | 4.2733 | 4.2795 | 4.2857 | 4.2857 | 366 | 1034 | 0 | 0 | 528 | 506 | 271979942 |
| Vessel Name / Callsign | VesselName exact match | 17745 | 14.0647 | 14.0722 | 14.0796 | 14.0796 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Vessel Name / Callsign | VesselName LIKE prefix | 1214334 | 16.9466 | 16.9480 | 16.9495 | 16.9495 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Vessel Name / Callsign | CallSign exact match | 33826 | 13.1475 | 13.1588 | 13.1700 | 13.1700 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Time Range | Single day time range | 8779885 | 0.1144 | 0.1146 | 0.1148 | 0.1148 | 1 | 9 | 0 | 0 | 0 | 9 | 8779885 |
| Time Range | Week-long time range | 58814886 | 0.7693 | 0.7694 | 0.7695 | 0.7695 | 7 | 63 | 0 | 0 | 0 | 63 | 58814886 |
| Time Range | Month-long time range | 264312029 | 3.4559 | 3.4559 | 3.4560 | 3.4560 | 31 | 282 | 0 | 0 | 0 | 282 | 264312029 |
| Time Range | Specific hour window | 360515 | 0.1237 | 0.1237 | 0.1238 | 0.1238 | 1 | 9 | 0 | 0 | 0 | 9 | 8779885 |
| Bounding Box | Bounding box: Gulf of Mexico | 277005218 | 13.0255 | 13.0335 | 13.0416 | 13.0416 | 366 | 1034 | 360 | 0 | 0 | 674 | 431231724 |
| Bounding Box | Bounding box: Pacific NW (Puget Sound) | 67565901 | 6.4508 | 6.4684 | 6.4860 | 6.4860 | 366 | 1034 | 510 | 0 | 0 | 524 | 281231724 |
| Bounding Box | Bounding box: New York Harbor | 21322627 | 6.9831 | 6.9862 | 6.9894 | 6.9894 | 366 | 1034 | 416 | 0 | 0 | 618 | 345818340 |
| Bounding Box | Bounding box: San Diego | 12167112 | 5.7541 | 5.7551 | 5.7562 | 5.7562 | 366 | 1034 | 501 | 0 | 0 | 533 | 290231724 |
| Combined Queries | MMSI + time range | 13174 | 0.6011 | 0.6022 | 0.6034 | 0.6034 | 31 | 282 | 1 | 0 | 243 | 38 | 38000000 |
| Combined Queries | Bounding box + time range | 11387087 | 1.0746 | 1.0890 | 1.1034 | 1.1034 | 7 | 63 | 31 | 0 | 0 | 32 | 32000000 |
| Combined Queries | VesselName + time range | 13174 | 5.6644 | 5.6645 | 5.6647 | 5.6647 | 31 | 282 | 0 | 0 | 0 | 282 | 264312029 |
| Combined Queries | VesselType + bounding box | 16729409 | 13.3028 | 13.3157 | 13.3286 | 13.3286 | 366 | 1034 | 360 | 0 | 0 | 674 | 431231724 |
| Aggregations | COUNT all rows | 743268520 | 0.0200 | 0.0205 | 0.0210 | 0.0210 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Aggregations | COUNT by VesselType | 89 | 34.2669 | 34.2880 | 34.3092 | 34.3092 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Aggregations | COUNT by day | 366 | 16.2809 | 16.2829 | 16.2849 | 16.2849 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Aggregations | Distinct MMSI count | 80096 | 14.1404 | 14.1467 | 14.1531 | 14.1531 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: CallSign = exact (exists) | 0 | 13.1826 | 13.1861 | 13.1897 | 13.1897 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: CallSign = exact (not exists) | 0 | 0.1169 | 0.1178 | 0.1187 | 0.1187 | 366 | 1034 | 1028 | 0 | 0 | 6 | 6000000 |
| Bloom Filter Effectiveness | Bloom test: VesselName = exact (exists) | 17745 | 14.0880 | 14.0947 | 14.1014 | 14.1014 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: VesselName = exact (not exists) | 0 | 13.8619 | 13.8752 | 13.8885 | 13.8885 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: IMO = exact (exists) | 17745 | 1.1617 | 1.1632 | 1.1647 | 1.1647 | 366 | 1034 | 0 | 0 | 921 | 113 | 57028555 |
| Bloom Filter Effectiveness | Bloom test: IMO = exact (not exists) | 1406519 | 7.9811 | 8.0054 | 8.0298 | 8.0298 | 366 | 1034 | 0 | 0 | 198 | 836 | 569353987 |
| Bloom Filter Effectiveness | Bloom test: CallSign LIKE prefix | 40733731 | 16.6669 | 16.6677 | 16.6686 | 16.6686 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |
| Bloom Filter Effectiveness | Bloom test: VesselName LIKE substring | 3694215 | 30.7825 | 30.7832 | 30.7838 | 30.7838 | 366 | 1034 | 0 | 0 | 0 | 1034 | 743268520 |

