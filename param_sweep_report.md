# Parameter Sweep Report

## Experiment 1: Row Group Size x Data Page Size

| RGS \ DPS | 64KB | 256KB | 1MB |
|---|---|---|---|
| 100K | 1.16x | 1.15x | 1.15x |
| 500K | 0.91x | 0.91x | 0.89x |
| 1M | 0.96x | 0.96x | 1.00x |
| 5M | 1.19x | 1.23x | 1.17x |

**Optimal: RGS=5M, DPS=256KB (geo-mean vs default: 1.23x)**

### Optimal vs Default per Query

| Query | Default (s) | Optimal (s) | Speedup |
|---|---|---|---|
| MMSI exact lookup | 1.0507 | 2.0200 | 0.52x |
| IMO exact lookup | 1.2472 | 2.5068 | 0.50x |
| VesselName exact match | 29.3434 | 29.4049 | 1.00x |
| CallSign exact match | 13.5132 | 13.1787 | 1.03x |
| Single day time range | 0.1255 | 0.1148 | 1.09x |
| Week-long time range | 0.8170 | 0.7559 | 1.08x |
| Bounding box: Gulf of Mexico | 14.3784 | 14.6814 | 0.98x |
| Bounding box: San Diego | 5.7812 | 8.8554 | 0.65x |
| MMSI + time range | 0.6042 | 1.4912 | 0.41x |
| Bounding box + time range | 1.0795 | 1.1145 | 0.97x |
| VesselName + time range | 5.6879 | 5.5036 | 1.03x |
| Bloom test: IMO = exact (exists) | 1.1981 | 2.4854 | 0.48x |

## Experiment 2: Bloom FPP x NDV

| FPP \ NDV (point-lookup geo-mean) | 100K | 1M | 10M |
|---|---|---|---|
| 0.1 | 0.76x | 0.89x | 1.61x |
| 0.01 | 0.82x | 1.00x | 2.35x |
| 0.001 | 0.78x | 0.93x | 5.28x |

**Optimal: FPP=0.001, NDV=10M (geo-mean point-lookup vs default: 5.28x)**


## Experiment 3: Combined Validation (smart-rgs + per-column NDV)

| Query | Baseline (s) | Optimal (s) | Speedup |
|---|---|---|---|
| MMSI exact lookup | 1.2781 | 0.4444 | 2.88x |
| IMO exact lookup | 1.3745 | 0.6244 | 2.20x |
| VesselName exact match | 15.6601 | 1.5414 | 10.16x |
| CallSign exact match | 14.5453 | 1.4712 | 9.89x |
| Single day time range | 0.1465 | 0.1461 | 1.00x |
| Week-long time range | 0.9528 | 0.9472 | 1.01x |
| Bounding box: Gulf of Mexico | 14.2273 | 14.2129 | 1.00x |
| Bounding box: San Diego | 1.7781 | 1.7872 | 0.99x |
| MMSI + time range | 0.5663 | 0.2956 | 1.92x |
| Bounding box + time range | 1.0768 | 1.0767 | 1.00x |
| VesselName + time range | 6.2906 | 0.4644 | 13.55x |
| Bloom test: IMO = exact (exists) | 1.3280 | 0.5904 | 2.25x |

**Combined geo-mean speedup: 2.40x**


## Optimal Parameters per Format

| Format | Row Group Size | Data Page Size | Bloom FPP | Bloom NDV | Geo-Mean Speedup vs Default |
|---|---|---|---|---|---|
| hilbert_bloom | 5M | 256KB | 0.001 | 10M | 1.23x |