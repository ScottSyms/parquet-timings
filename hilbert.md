# Hilbert Curve: Encode / Decode

```python
# ── Encode: (lat, lon) → 64-bit Hilbert index ──────────────────────

HILBERT_BITS = 31
MAX_AXIS = (1 << HILBERT_BITS) - 1  # 2^31 - 1

def scale(value: float, world_min: float, world_max: float) -> int:
    """Normalise a lat/lon to a u32 in [0, MAX_AXIS]."""
    if world_max <= world_min or not is_finite(value):
        return 0
    t = clamp((value - world_min) / (world_max - world_min), 0.0, 1.0)
    return round(t * MAX_AXIS)


def hilbert_index_2d(point: [int, int]) -> int:
    """
    Convert a 2D coordinate pair to a 1D Hilbert curve index (u64).

    Steps:
      1. Bitwise transpose the interleaved axes so adjacent cells
         in 2D space map to adjacent 1D indices.
      2. Interleave bits of both axes into a single u64:
         for each bit position (MSB → LSB):
           for each axis:
             output ← (output << 1) | (axis[bit] & 1)
    """
    axes = [point[0], point[1]]

    # ── transpose ──
    # For each power-of-two stride from (1 << HILBERT_BITS-1) down to 2,
    # conditionally swap bits between axis[0] and axis[i] based on the
    # current bit in each axis.  (This is the recursive Hilbert curve
    # "rotation" unfolding implemented iteratively.)
    m = 1 << (HILBERT_BITS - 1)
    q = m
    while q > 1:
        p = q - 1
        for i in [0, 1]:
            if axes[i] & q:
                axes[0] ^= p
            else:
                t = (axes[0] ^ axes[i]) & p
                axes[0] ^= t
                axes[i] ^= t
        q >>= 1
    axes[1] ^= axes[0]

    # ── interleave bits ──
    index = 0
    for bit in reversed(range(HILBERT_BITS)):
        for axis in axes:
            index = (index << 1) | ((axis >> bit) & 1)
    return index


# ── Decode: (lat_min, lat_max, lon_min, lon_max) → sorted,
#            merged Hilbert intervals to compare against RG stats ────

def query_hilbert_intervals(lat: (float, float), lon: (float, float),
                            bounds: HilbertBounds) -> list[(int, int)]:
    """
    Convert a lat/lon bounding box into a set of disjoint Hilbert
    index intervals that cover exactly the box.

    Returns [] if no spatial range is present.
    """
    q_min_lat = scale(lat[0], bounds.min_lat, bounds.max_lat)
    q_max_lat = scale(lat[1], bounds.min_lat, bounds.max_lat)
    q_min_lon = scale(lon[0], bounds.min_lon, bounds.max_lon)
    q_max_lon = scale(lon[1], bounds.min_lon, bounds.max_lon)

    if q_min_lat > q_max_lat or q_min_lon > q_max_lon:
        return []

    query = [(q_min_lat, q_max_lat), (q_min_lon, q_max_lon)]
    intervals = []
    visit_hilbert_cell_2d(0, 0, HILBERT_BITS, query, intervals)
    return merge_intervals(intervals)


MAX_INTERVALS = 512  # cap to prevent pathological split


def visit_hilbert_cell_2d(a0: int, b0: int, bits: int,
                          query: list[(int, int)],
                          intervals: list[(int, int)]):
    """
    Recursive quadtree descent.

    - a0, b0: origin of current cell in axis units
    - bits:   remaining bits (cell spans 2^bits in each axis)
    - query:  [(lat_min, lat_max), (lon_min, lon_max)]

    If the cell is fully inside the query → emit its full interval.
    If partially overlapping → subdivide into 4 Hilbert-sorted children
    and recurse.  Fully outside → discard.
    """
    size = 1 << bits
    a1, b1 = a0 + size - 1, b0 + size - 1

    # ── outside → discard ──
    if a1 < query[0][0] or a0 > query[0][1] \
       or b1 < query[1][0] or b0 > query[1][1]:
        return

    # ── fully inside (or max depth/interval cap) → emit interval ──
    fully_inside = (query[0][0] <= a0 and a1 <= query[0][1]
                    and query[1][0] <= b0 and b1 <= query[1][1])
    if fully_inside or bits == 0 or len(intervals) >= MAX_INTERVALS:
        intervals.append(cell_interval_2d(a0, b0, bits))
        return

    # ── partially overlapping → subdivide ──
    child_bits = bits - 1
    child_size = 1 << child_bits

    # Build 4 children, sort them by their Hilbert index (this is the
    # key insight: Hilbert curves visit children in Fourier order, not
    # raster order).
    children = []
    for da in [0, child_size]:
        for db in [0, child_size]:
            child = (a0 + da, b0 + db)
            children.append((hilbert_index_2d(child), child))
    children.sort_by_key(child → child[0])

    for _, (ca, cb) in children:
        visit_hilbert_cell_2d(ca, cb, child_bits, query, intervals)


def cell_interval_2d(a0: int, b0: int, remaining_bits: int) -> (int, int):
    """
    Compute the range of Hilbert indices covered by this cell.

    The cell's index prefix = hilbert_index(a0, b0) >> (2 * remaining_bits).
    All indices in the cell share this prefix; the remaining bits
    vary from 0 to (1 << 2*remaining_bits) - 1.
    """
    prefix_shift = 2 * remaining_bits
    if prefix_shift == 0:               # single point
        idx = hilbert_index_2d([a0, b0])
        return (idx, idx)
    if prefix_shift >= 64:              # whole space
        return (0, UINT64_MAX)

    prefix = hilbert_index_2d([a0, b0]) >> prefix_shift
    start = prefix << prefix_shift
    end   = start + (1 << prefix_shift) - 1
    return (start, end)


def merge_intervals(intervals: list[(int, int)]) -> list[(int, int)]:
    """Sort by start, then merge overlapping/adjacent intervals."""
    if not intervals:
        return []
    intervals.sort(key=pair → pair[0])
    merged = [intervals[0]]
    for start, end in intervals[1:]:
        if start > merged[-1][1] + 1:
            merged.append((start, end))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
    return merged


# ── Query-time pruning check (per row group) ───────────────────────

def hilbert_skip_row_group(rg_min_hilbert: int, rg_max_hilbert: int,
                            query_intervals: list[(int, int)]) -> bool:
    """
    A row group can be skipped if NO query interval overlaps
    the RG's stored [min, max] Hilbert index range.
    """
    return not any(rg_max_hilbert >= q_start and rg_min_hilbert <= q_end
                   for q_start, q_end in query_intervals)
```
