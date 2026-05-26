#!/usr/bin/env python3
"""
AIS Parquet Format Benchmark

Compares five storage formats without DuckDB:
1. Monolithic parquet (parquet/ais_2024.parquet)
2. Partitioned parquet by year/month/day (partition_parquet/)
3. Partitioned parquet + bloom filters (partition_bloom_parquet/)
4. Partitioned parquet + Hilbert ordering (partition_hilbert_parquet/)
5. Partitioned parquet + Hilbert ordering + bloom filters (partition_hilbert_bloom_parquet/)

The benchmark uses PyArrow Dataset filters so each layout is tested through the
structure it was built for: Hive partition pruning, equality predicates that can
use Parquet bloom filters where supported, and Hilbert index range pruning.
"""

from __future__ import annotations

import signal
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
MONOLITHIC_PATH = BASE_DIR / "parquet" / "ais_2024.parquet"
PARTITIONED_PATH = BASE_DIR / "partition_parquet"
PARTITIONED_BLOOM_PATH = BASE_DIR / "partition_bloom_parquet"
PARTITIONED_HILBERT_PATH = BASE_DIR / "partition_hilbert_parquet"
PARTITIONED_HILBERT_BLOOM_PATH = BASE_DIR / "partition_hilbert_bloom_parquet"

QUERY_TIMEOUT = 300  # seconds
HILBERT_BITS = 21
HILBERT_MAX_AXIS = (1 << HILBERT_BITS) - 1
HILBERT_MAX_INTERVALS = 512


@dataclass(frozen=True)
class FormatSpec:
    name: str
    path: Path
    partitioned: bool = False
    bloom: bool = False
    hilbert: bool = False

    @property
    def description(self) -> str:
        parts = ["PyArrow Dataset scan"]
        if self.partitioned:
            parts.append("Hive partition pruning")
        if self.bloom:
            parts.append("equality predicates on bloom-filtered columns")
        if self.hilbert:
            parts.append("Hilbert index range pruning")
        return " + ".join(parts)


FORMATS = [
    FormatSpec("monolithic", MONOLITHIC_PATH),
    FormatSpec("partitioned", PARTITIONED_PATH, partitioned=True),
    FormatSpec("partitioned_bloom", PARTITIONED_BLOOM_PATH, partitioned=True, bloom=True),
    FormatSpec(
        "partition_hilbert",
        PARTITIONED_HILBERT_PATH,
        partitioned=True,
        hilbert=True,
    ),
    FormatSpec(
        "partition_hilbert_bloom",
        PARTITIONED_HILBERT_BLOOM_PATH,
        partitioned=True,
        bloom=True,
        hilbert=True,
    ),
]


@dataclass(frozen=True)
class RangeFilter:
    column: str
    lower: float | None = None
    upper: float | None = None
    lower_inclusive: bool = True
    upper_inclusive: bool = True


@dataclass(frozen=True)
class EqualityFilter:
    column: str
    value: str | int | float


@dataclass(frozen=True)
class InFilter:
    column: str
    values: tuple[str | int | float, ...]


@dataclass(frozen=True)
class TimeRangeFilter:
    start: datetime
    end: datetime


@dataclass(frozen=True)
class StringMatch:
    column: str
    pattern: str
    kind: str  # prefix | substring


@dataclass(frozen=True)
class QueryDef:
    name: str
    category: str
    operation: str
    columns: tuple[str, ...] = ()
    equality_filters: tuple[EqualityFilter, ...] = ()
    in_filters: tuple[InFilter, ...] = ()
    range_filters: tuple[RangeFilter, ...] = ()
    time_range: TimeRangeFilter | None = None
    string_match: StringMatch | None = None
    group_by: str | None = None
    distinct: str | None = None


@dataclass
class QueryPlan:
    scan_filter: ds.Expression | None
    exact_filter: ds.Expression | None
    columns: list[str]
    operation_description: str
    post_filter: Callable[[pa.Table], pa.Table] | None = None


@dataclass
class QueryResult:
    name: str
    row_count: int
    operation_used: str = ""
    times: list[float] = field(default_factory=list)
    timed_out: bool = False

    @property
    def min_time(self) -> float:
        valid = [t for t in self.times if t > 0]
        return min(valid) if valid else 0.0

    @property
    def mean_time(self) -> float:
        valid = [t for t in self.times if t > 0]
        return statistics.mean(valid) if valid else 0.0

    @property
    def median_time(self) -> float:
        valid = [t for t in self.times if t > 0]
        return statistics.median(valid) if valid else 0.0

    @property
    def max_time(self) -> float:
        valid = [t for t in self.times if t > 0]
        return max(valid) if valid else 0.0


def utc_dt(year: int, month: int, day: int, hour: int = 0) -> datetime:
    return datetime(year, month, day, hour, tzinfo=timezone.utc)


QUERIES = [
    QueryDef(
        name="MMSI exact lookup",
        category="Point Lookups",
        operation="rows",
        equality_filters=(EqualityFilter("MMSI", "563073700"),),
    ),
    QueryDef(
        name="MMSI multi lookup (5 MMSIs)",
        category="Point Lookups",
        operation="count",
        in_filters=(InFilter("MMSI", ("563073700", "248669000", "367481310", "367776660", "319194000")),),
    ),
    QueryDef(
        name="IMO exact lookup",
        category="Point Lookups",
        operation="rows",
        equality_filters=(EqualityFilter("IMO", "IMO9348651"),),
    ),
    QueryDef(
        name="IMO multi lookup (4 IMOs)",
        category="Point Lookups",
        operation="count",
        in_filters=(InFilter("IMO", ("IMO9348651", "IMO9199323", "IMO9826146", "IMO9843807")),),
    ),
    QueryDef(
        name="VesselName exact match",
        category="Vessel Name / Callsign",
        operation="rows",
        equality_filters=(EqualityFilter("VesselName", "MAERSK MEMPHIS"),),
    ),
    QueryDef(
        name="VesselName LIKE prefix",
        category="Vessel Name / Callsign",
        operation="count",
        columns=("VesselName",),
        string_match=StringMatch("VesselName", "MAERSK", "prefix"),
    ),
    QueryDef(
        name="CallSign exact match",
        category="Vessel Name / Callsign",
        operation="rows",
        equality_filters=(EqualityFilter("CallSign", "WDK6270"),),
    ),
    QueryDef(
        name="Single day time range",
        category="Time Range",
        operation="count",
        time_range=TimeRangeFilter(utc_dt(2024, 10, 20), utc_dt(2024, 10, 21)),
    ),
    QueryDef(
        name="Week-long time range",
        category="Time Range",
        operation="count",
        time_range=TimeRangeFilter(utc_dt(2024, 10, 14), utc_dt(2024, 10, 21)),
    ),
    QueryDef(
        name="Month-long time range",
        category="Time Range",
        operation="count",
        time_range=TimeRangeFilter(utc_dt(2024, 10, 1), utc_dt(2024, 11, 1)),
    ),
    QueryDef(
        name="Specific hour window",
        category="Time Range",
        operation="count",
        time_range=TimeRangeFilter(utc_dt(2024, 10, 20, 4), utc_dt(2024, 10, 20, 5)),
    ),
    QueryDef(
        name="Bounding box: Gulf of Mexico",
        category="Bounding Box",
        operation="count",
        range_filters=(RangeFilter("LAT", 25.0, 31.0), RangeFilter("LON", -96.0, -80.0)),
    ),
    QueryDef(
        name="Bounding box: Pacific NW (Puget Sound)",
        category="Bounding Box",
        operation="count",
        range_filters=(RangeFilter("LAT", 47.0, 49.0), RangeFilter("LON", -124.0, -122.0)),
    ),
    QueryDef(
        name="Bounding box: New York Harbor",
        category="Bounding Box",
        operation="count",
        range_filters=(RangeFilter("LAT", 40.5, 41.0), RangeFilter("LON", -74.5, -73.5)),
    ),
    QueryDef(
        name="Bounding box: San Diego",
        category="Bounding Box",
        operation="count",
        range_filters=(RangeFilter("LAT", 32.5, 33.0), RangeFilter("LON", -117.5, -117.0)),
    ),
    QueryDef(
        name="MMSI + time range",
        category="Combined Queries",
        operation="rows",
        equality_filters=(EqualityFilter("MMSI", "563073700"),),
        time_range=TimeRangeFilter(utc_dt(2024, 10, 1), utc_dt(2024, 11, 1)),
    ),
    QueryDef(
        name="Bounding box + time range",
        category="Combined Queries",
        operation="count",
        range_filters=(RangeFilter("LAT", 29.0, 31.0), RangeFilter("LON", -95.0, -89.0)),
        time_range=TimeRangeFilter(utc_dt(2024, 10, 14), utc_dt(2024, 10, 21)),
    ),
    QueryDef(
        name="VesselName + time range",
        category="Combined Queries",
        operation="rows",
        equality_filters=(EqualityFilter("VesselName", "MAERSK MEMPHIS"),),
        time_range=TimeRangeFilter(utc_dt(2024, 10, 1), utc_dt(2024, 11, 1)),
    ),
    QueryDef(
        name="VesselType + bounding box",
        category="Combined Queries",
        operation="count",
        equality_filters=(EqualityFilter("VesselType", "70"),),
        range_filters=(RangeFilter("LAT", 25.0, 31.0), RangeFilter("LON", -96.0, -80.0)),
    ),
    QueryDef(name="COUNT all rows", category="Aggregations", operation="count"),
    QueryDef(
        name="COUNT by VesselType",
        category="Aggregations",
        operation="group_by",
        columns=("VesselType",),
        group_by="VesselType",
    ),
    QueryDef(
        name="COUNT by day",
        category="Aggregations",
        operation="count_by_day",
        columns=("BaseDateTime",),
    ),
    QueryDef(
        name="Distinct MMSI count",
        category="Aggregations",
        operation="distinct",
        columns=("MMSI",),
        distinct="MMSI",
    ),
    QueryDef(
        name="Bloom test: CallSign = exact (exists)",
        category="Bloom Filter Effectiveness",
        operation="count",
        equality_filters=(EqualityFilter("CallSign", "WDJ6270"),),
    ),
    QueryDef(
        name="Bloom test: CallSign = exact (not exists)",
        category="Bloom Filter Effectiveness",
        operation="count",
        equality_filters=(EqualityFilter("CallSign", "ZZZZZZZZ"),),
    ),
    QueryDef(
        name="Bloom test: VesselName = exact (exists)",
        category="Bloom Filter Effectiveness",
        operation="count",
        equality_filters=(EqualityFilter("VesselName", "MAERSK MEMPHIS"),),
    ),
    QueryDef(
        name="Bloom test: VesselName = exact (not exists)",
        category="Bloom Filter Effectiveness",
        operation="count",
        equality_filters=(EqualityFilter("VesselName", "NONEXISTENT SHIP NAME"),),
    ),
    QueryDef(
        name="Bloom test: IMO = exact (exists)",
        category="Bloom Filter Effectiveness",
        operation="count",
        equality_filters=(EqualityFilter("IMO", "IMO9348651"),),
    ),
    QueryDef(
        name="Bloom test: IMO = exact (not exists)",
        category="Bloom Filter Effectiveness",
        operation="count",
        equality_filters=(EqualityFilter("IMO", "IMO0000001"),),
    ),
    QueryDef(
        name="Bloom test: CallSign LIKE prefix",
        category="Bloom Filter Effectiveness",
        operation="count",
        columns=("CallSign",),
        string_match=StringMatch("CallSign", "WDK", "prefix"),
    ),
    QueryDef(
        name="Bloom test: VesselName LIKE substring",
        category="Bloom Filter Effectiveness",
        operation="count",
        columns=("VesselName",),
        string_match=StringMatch("VesselName", "SPIRIT", "substring"),
    ),
]


@dataclass(frozen=True)
class HilbertBounds:
    min_time_us: int
    max_time_us: int
    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float


class TimeoutError(Exception):
    pass


def timeout_handler(signum, frame):
    raise TimeoutError("Query timed out")


def make_dataset(fmt: FormatSpec) -> ds.Dataset:
    if fmt.partitioned:
        return ds.dataset(fmt.path, format="parquet", partitioning="hive")
    return ds.dataset(fmt.path, format="parquet")


def and_expr(expressions: Iterable[ds.Expression | None]) -> ds.Expression | None:
    values = [expr for expr in expressions if expr is not None]
    if not values:
        return None
    result = values[0]
    for expr in values[1:]:
        result = result & expr
    return result


def or_expr(expressions: Iterable[ds.Expression]) -> ds.Expression | None:
    values = list(expressions)
    if not values:
        return None
    result = values[0]
    for expr in values[1:]:
        result = result | expr
    return result


def schema_type(schema: pa.Schema, column: str) -> pa.DataType | None:
    try:
        return schema.field(column).type
    except KeyError:
        return None


def is_string_view_column(schema: pa.Schema, column: str) -> bool:
    field_type = schema_type(schema, column)
    return field_type is not None and pa.types.is_string_view(field_type)


def literal_value(schema: pa.Schema, column: str, value: str | int | float) -> str | int | float:
    field_type = schema_type(schema, column)
    if field_type is not None and (pa.types.is_string(field_type) or pa.types.is_large_string(field_type) or pa.types.is_string_view(field_type)):
        return str(value)
    return value


def literal_array(schema: pa.Schema, column: str, values: tuple[str | int | float, ...]) -> list[str | int | float]:
    field_type = schema_type(schema, column)
    if field_type is not None and (pa.types.is_string(field_type) or pa.types.is_large_string(field_type) or pa.types.is_string_view(field_type)):
        return [str(value) for value in values]
    return list(values)


def base_filter_expression(qdef: QueryDef, schema: pa.Schema) -> ds.Expression | None:
    parts: list[ds.Expression] = []
    for eq in qdef.equality_filters:
        if not is_string_view_column(schema, eq.column):
            parts.append(ds.field(eq.column) == literal_value(schema, eq.column, eq.value))
    for in_filter in qdef.in_filters:
        if not is_string_view_column(schema, in_filter.column):
            parts.append(ds.field(in_filter.column).isin(literal_array(schema, in_filter.column, in_filter.values)))
    for range_filter in qdef.range_filters:
        column = ds.field(range_filter.column)
        if range_filter.lower is not None:
            parts.append(column >= range_filter.lower if range_filter.lower_inclusive else column > range_filter.lower)
        if range_filter.upper is not None:
            parts.append(column <= range_filter.upper if range_filter.upper_inclusive else column < range_filter.upper)
    if qdef.time_range:
        parts.append(ds.field("BaseDateTime") >= qdef.time_range.start)
        parts.append(ds.field("BaseDateTime") < qdef.time_range.end)
    return and_expr(parts)


def partition_expression(qdef: QueryDef) -> ds.Expression | None:
    if not qdef.time_range:
        return None

    start = qdef.time_range.start.date()
    end = (qdef.time_range.end - timedelta(microseconds=1)).date()
    current = start
    parts = []
    while current <= end:
        parts.append(
            (ds.field("year") == current.year)
            & (ds.field("month") == current.month)
            & (ds.field("day") == current.day)
        )
        current += timedelta(days=1)
    return or_expr(parts)


def filter_chunked_array(column: pa.ChunkedArray) -> pa.ChunkedArray:
    if pa.types.is_string_view(column.type) or pa.types.is_large_string(column.type):
        return pc.cast(column, pa.string())
    return column


def build_post_filter(qdef: QueryDef, schema: pa.Schema) -> Callable[[pa.Table], pa.Table] | None:
    post_equality = [eq for eq in qdef.equality_filters if is_string_view_column(schema, eq.column)]
    post_in = [in_filter for in_filter in qdef.in_filters if is_string_view_column(schema, in_filter.column)]
    if not post_equality and not post_in and not qdef.string_match:
        return None

    def apply(table: pa.Table) -> pa.Table:
        for idx, field in enumerate(table.schema):
            if pa.types.is_string_view(field.type) or pa.types.is_large_string(field.type):
                table = table.set_column(idx, field.name, pc.cast(table[field.name], pa.string()))

        masks = []
        for eq in qdef.equality_filters:
            column = filter_chunked_array(table[eq.column])
            masks.append(pc.equal(column, literal_value(table.schema, eq.column, eq.value)))
        for in_filter in qdef.in_filters:
            column = filter_chunked_array(table[in_filter.column])
            field_type = schema_type(table.schema, in_filter.column)
            if field_type is not None and (pa.types.is_string(field_type) or pa.types.is_large_string(field_type) or pa.types.is_string_view(field_type)):
                values = pa.array([str(value) for value in in_filter.values], type=pa.string())
            else:
                values = pa.array(list(in_filter.values), type=field_type)
            masks.append(pc.is_in(column, value_set=values))
        for range_filter in qdef.range_filters:
            column = table[range_filter.column]
            if range_filter.lower is not None:
                masks.append(pc.greater_equal(column, range_filter.lower) if range_filter.lower_inclusive else pc.greater(column, range_filter.lower))
            if range_filter.upper is not None:
                masks.append(pc.less_equal(column, range_filter.upper) if range_filter.upper_inclusive else pc.less(column, range_filter.upper))
        if qdef.time_range:
            column = table["BaseDateTime"]
            masks.append(pc.greater_equal(column, qdef.time_range.start))
            masks.append(pc.less(column, qdef.time_range.end))
        if qdef.string_match:
            column = filter_chunked_array(table[qdef.string_match.column])
            if qdef.string_match.kind == "prefix":
                masks.append(pc.starts_with(column, pattern=qdef.string_match.pattern))
            elif qdef.string_match.kind == "substring":
                masks.append(pc.match_substring(column, pattern=qdef.string_match.pattern))
            else:
                raise ValueError(f"unknown string match kind: {qdef.string_match.kind}")

        mask = masks[0]
        for next_mask in masks[1:]:
            mask = pc.and_(mask, next_mask)
        mask = pc.fill_null(mask, False)
        return table.filter(mask)

    return apply


def build_query_plan(fmt: FormatSpec, qdef: QueryDef, hilbert_bounds: HilbertBounds | None, schema: pa.Schema) -> QueryPlan:
    exact_filter = base_filter_expression(qdef, schema)
    partition_filter = partition_expression(qdef) if fmt.partitioned else None
    hilbert_filter = hilbert_expression(qdef, hilbert_bounds) if fmt.hilbert and hilbert_bounds else None
    post_filter = build_post_filter(qdef, schema)
    has_string_view_post_filter = any(is_string_view_column(schema, eq.column) for eq in qdef.equality_filters) or any(
        is_string_view_column(schema, in_filter.column) for in_filter in qdef.in_filters
    )
    scan_filter = None if has_string_view_post_filter else and_expr([partition_filter, hilbert_filter, exact_filter])

    columns = set(qdef.columns)
    if post_filter is not None:
        for eq in qdef.equality_filters:
            columns.add(eq.column)
        for in_filter in qdef.in_filters:
            columns.add(in_filter.column)
        for range_filter in qdef.range_filters:
            columns.add(range_filter.column)
        if qdef.time_range:
            columns.add("BaseDateTime")
    if qdef.string_match:
        columns.add(qdef.string_match.column)
    if qdef.group_by:
        columns.add(qdef.group_by)
    if qdef.distinct:
        columns.add(qdef.distinct)

    description = [fmt.description]
    if partition_filter is not None:
        description.append("partition filter: year/month/day")
    if hilbert_filter is not None:
        description.append("hilbert_index interval prefilter")
    if exact_filter is not None:
        description.append("exact predicate refinement")
    if qdef.string_match:
        description.append(f"post-filter: {qdef.string_match.column} {qdef.string_match.kind} '{qdef.string_match.pattern}'")
    if any(is_string_view_column(schema, eq.column) for eq in qdef.equality_filters) or any(
        is_string_view_column(schema, in_filter.column) for in_filter in qdef.in_filters
    ):
        description.append("post-filter: string_view equality fallback")
        if partition_filter is not None or hilbert_filter is not None or exact_filter is not None:
            description.append("scan filter disabled for PyArrow string_view compatibility")

    return QueryPlan(
        scan_filter=scan_filter,
        exact_filter=exact_filter,
        columns=sorted(columns),
        operation_description="; ".join(description),
        post_filter=post_filter,
    )


def run_query(dataset: ds.Dataset, qdef: QueryDef, plan: QueryPlan) -> int:
    if qdef.operation in {"count", "rows"} and plan.post_filter is None:
        return dataset.count_rows(filter=plan.scan_filter)

    table = dataset.to_table(columns=plan.columns or None, filter=plan.scan_filter)
    if plan.post_filter:
        table = plan.post_filter(table)

    if qdef.operation in {"count", "rows"}:
        return table.num_rows
    if qdef.operation == "group_by":
        values = table[qdef.group_by].combine_chunks()
        if pa.types.is_string_view(values.type) or pa.types.is_large_string(values.type):
            values = pc.cast(values, pa.string())
        return len(pc.value_counts(values))
    if qdef.operation == "count_by_day":
        dates = pc.cast(table["BaseDateTime"].combine_chunks(), pa.date32())
        return len(pc.value_counts(dates))
    if qdef.operation == "distinct":
        values = table[qdef.distinct].combine_chunks()
        if pa.types.is_string_view(values.type) or pa.types.is_large_string(values.type):
            values = pc.cast(values, pa.string())
        return len(pc.unique(values))
    raise ValueError(f"unknown operation: {qdef.operation}")


def run_query_timed(dataset: ds.Dataset, qdef: QueryDef, plan: QueryPlan, timeout: int = QUERY_TIMEOUT) -> tuple[int, float, bool]:
    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(timeout)
    start = time.perf_counter()

    try:
        row_count = run_query(dataset, qdef, plan)
        elapsed = time.perf_counter() - start
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)
        return row_count, elapsed, False
    except TimeoutError:
        elapsed = time.perf_counter() - start
        signal.signal(signal.SIGALRM, old_handler)
        return 0, elapsed, True
    except Exception:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)
        raise


def warm_cache(dataset: ds.Dataset, fmt: FormatSpec) -> None:
    print(f"  Warming metadata/cache for {fmt.path}...")
    dataset.count_rows()


def benchmark_format(
    fmt: FormatSpec,
    queries: list[QueryDef],
    hilbert_bounds: HilbertBounds | None,
    warmup_runs: int = 1,
    timed_runs: int = 2,
) -> list[QueryResult]:
    print(f"\n{'=' * 60}")
    print(f"Benchmarking: {fmt.name}")
    print(f"Source: {fmt.path}")
    print(f"Method: {fmt.description}")
    print(f"{'=' * 60}")

    dataset = make_dataset(fmt)
    for _ in range(warmup_runs):
        warm_cache(dataset, fmt)

    results = []
    total_queries = len(queries)
    for idx, qdef in enumerate(queries, 1):
        plan = build_query_plan(fmt, qdef, hilbert_bounds, dataset.schema)
        print(f"  [{idx}/{total_queries}] Running: {qdef.name}...", end=" ", flush=True)
        result = QueryResult(name=qdef.name, row_count=0, operation_used=plan.operation_description)

        for _ in range(timed_runs):
            try:
                row_count, elapsed, timed_out = run_query_timed(dataset, qdef, plan)
                result.times.append(elapsed)
                result.row_count = row_count
                result.timed_out = timed_out
            except Exception as exc:
                print(f"ERROR: {exc}")
                result.times.append(0.0)

        status = "TIMEOUT" if result.timed_out else "OK"
        print(f"{status} result={result.row_count}, min={result.min_time:.4f}s, median={result.median_time:.4f}s")
        results.append(result)

    return results


def timestamp_to_us(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1_000_000)


def scale_i64(value: int, min_value: int, max_value: int) -> int:
    if max_value <= min_value:
        return 0
    ratio = (value - min_value) / (max_value - min_value)
    return round(max(0.0, min(1.0, ratio)) * HILBERT_MAX_AXIS)


def scale_f64(value: float, min_value: float, max_value: float) -> int:
    if max_value <= min_value:
        return 0
    ratio = (value - min_value) / (max_value - min_value)
    return round(max(0.0, min(1.0, ratio)) * HILBERT_MAX_AXIS)


def axes_to_hilbert_transpose(axes: list[int], bits: int = HILBERT_BITS) -> None:
    m = 1 << (bits - 1)
    q = m
    while q > 1:
        p = q - 1
        for axis in range(3):
            if axes[axis] & q:
                axes[0] ^= p
            else:
                t = (axes[0] ^ axes[axis]) & p
                axes[0] ^= t
                axes[axis] ^= t
        q >>= 1

    for axis in range(1, 3):
        axes[axis] ^= axes[axis - 1]

    t = 0
    q = m
    while q > 1:
        if axes[2] & q:
            t ^= q - 1
        q >>= 1

    for axis in range(3):
        axes[axis] ^= t


def hilbert_index_3d(point: tuple[int, int, int]) -> int:
    axes = [point[0], point[1], point[2]]
    axes_to_hilbert_transpose(axes)
    index = 0
    for bit in range(HILBERT_BITS - 1, -1, -1):
        for axis in axes:
            index = (index << 1) | ((axis >> bit) & 1)
    return index


def query_axis_bounds(qdef: QueryDef, bounds: HilbertBounds) -> tuple[tuple[int, int], tuple[int, int], tuple[int, int]] | None:
    has_hilbert_dimension = qdef.time_range is not None or any(r.column in {"LAT", "LON"} for r in qdef.range_filters)
    if not has_hilbert_dimension:
        return None

    time_min = bounds.min_time_us
    time_max = bounds.max_time_us
    if qdef.time_range:
        time_min = max(time_min, timestamp_to_us(qdef.time_range.start))
        time_max = min(time_max, timestamp_to_us(qdef.time_range.end) - 1)

    lat_min, lat_max = bounds.min_lat, bounds.max_lat
    lon_min, lon_max = bounds.min_lon, bounds.max_lon
    for range_filter in qdef.range_filters:
        if range_filter.column == "LAT":
            lat_min = max(lat_min, range_filter.lower if range_filter.lower is not None else bounds.min_lat)
            lat_max = min(lat_max, range_filter.upper if range_filter.upper is not None else bounds.max_lat)
        elif range_filter.column == "LON":
            lon_min = max(lon_min, range_filter.lower if range_filter.lower is not None else bounds.min_lon)
            lon_max = min(lon_max, range_filter.upper if range_filter.upper is not None else bounds.max_lon)

    if time_min > time_max or lat_min > lat_max or lon_min > lon_max:
        return None

    return (
        (scale_i64(time_min, bounds.min_time_us, bounds.max_time_us), scale_i64(time_max, bounds.min_time_us, bounds.max_time_us)),
        (scale_f64(lat_min, bounds.min_lat, bounds.max_lat), scale_f64(lat_max, bounds.min_lat, bounds.max_lat)),
        (scale_f64(lon_min, bounds.min_lon, bounds.max_lon), scale_f64(lon_max, bounds.min_lon, bounds.max_lon)),
    )


def cell_interval(x0: int, y0: int, z0: int, remaining_bits: int) -> tuple[int, int]:
    prefix_shift = 3 * remaining_bits
    prefix = hilbert_index_3d((x0, y0, z0)) >> prefix_shift
    start = prefix << prefix_shift
    end = start + (1 << prefix_shift) - 1
    return start, end


def hilbert_cover_intervals(axis_bounds: tuple[tuple[int, int], tuple[int, int], tuple[int, int]]) -> list[tuple[int, int]]:
    intervals: list[tuple[int, int]] = []
    query = axis_bounds

    def visit(x0: int, y0: int, z0: int, bits: int) -> None:
        size = 1 << bits
        x1, y1, z1 = x0 + size - 1, y0 + size - 1, z0 + size - 1
        if x1 < query[0][0] or x0 > query[0][1] or y1 < query[1][0] or y0 > query[1][1] or z1 < query[2][0] or z0 > query[2][1]:
            return

        fully_inside = (
            query[0][0] <= x0 <= x1 <= query[0][1]
            and query[1][0] <= y0 <= y1 <= query[1][1]
            and query[2][0] <= z0 <= z1 <= query[2][1]
        )
        if fully_inside or bits == 0 or len(intervals) >= HILBERT_MAX_INTERVALS:
            intervals.append(cell_interval(x0, y0, z0, bits))
            return

        child_bits = bits - 1
        child_size = 1 << child_bits
        children = []
        for dx in (0, child_size):
            for dy in (0, child_size):
                for dz in (0, child_size):
                    child = (x0 + dx, y0 + dy, z0 + dz)
                    children.append((hilbert_index_3d(child), child))
        for _, child in sorted(children):
            visit(child[0], child[1], child[2], child_bits)

    visit(0, 0, 0, HILBERT_BITS)
    return merge_intervals(intervals)


def merge_intervals(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    if not intervals:
        return []
    merged = []
    for start, end in sorted(intervals):
        if not merged or start > merged[-1][1] + 1:
            merged.append([start, end])
        else:
            merged[-1][1] = max(merged[-1][1], end)
    return [(start, end) for start, end in merged]


def hilbert_expression(qdef: QueryDef, bounds: HilbertBounds | None) -> ds.Expression | None:
    if bounds is None:
        return None
    axis_bounds = query_axis_bounds(qdef, bounds)
    if axis_bounds is None:
        return None
    intervals = hilbert_cover_intervals(axis_bounds)
    if not intervals:
        return None
    return or_expr((ds.field("hilbert_index") >= start) & (ds.field("hilbert_index") <= end) for start, end in intervals)


def parquet_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    return sorted(path.rglob("*.parquet"))


def metadata_bounds(path: Path) -> HilbertBounds:
    min_time_us: int | None = None
    max_time_us: int | None = None
    min_lat: float | None = None
    max_lat: float | None = None
    min_lon: float | None = None
    max_lon: float | None = None

    for file_path in parquet_files(path):
        metadata = pq.ParquetFile(file_path).metadata
        schema = metadata.schema
        column_indexes = {schema.column(i).name: i for i in range(metadata.num_columns)}
        for row_group in range(metadata.num_row_groups):
            rg = metadata.row_group(row_group)
            for column_name in ("BaseDateTime", "LAT", "LON"):
                if column_name not in column_indexes:
                    continue
                stats = rg.column(column_indexes[column_name]).statistics
                if stats is None or not stats.has_min_max:
                    continue
                if column_name == "BaseDateTime":
                    low = timestamp_to_us(stats.min)
                    high = timestamp_to_us(stats.max)
                    min_time_us = low if min_time_us is None else min(min_time_us, low)
                    max_time_us = high if max_time_us is None else max(max_time_us, high)
                elif column_name == "LAT":
                    min_lat = stats.min if min_lat is None else min(min_lat, stats.min)
                    max_lat = stats.max if max_lat is None else max(max_lat, stats.max)
                elif column_name == "LON":
                    min_lon = stats.min if min_lon is None else min(min_lon, stats.min)
                    max_lon = stats.max if max_lon is None else max(max_lon, stats.max)

    missing = [
        name
        for name, value in {
            "min_time_us": min_time_us,
            "max_time_us": max_time_us,
            "min_lat": min_lat,
            "max_lat": max_lat,
            "min_lon": min_lon,
            "max_lon": max_lon,
        }.items()
        if value is None
    ]
    if missing:
        raise RuntimeError(f"could not derive Hilbert bounds from Parquet metadata: {missing}")

    return HilbertBounds(min_time_us, max_time_us, min_lat, max_lat, min_lon, max_lon)


def verify_consistency(all_results: dict[str, list[QueryResult]]) -> None:
    print(f"\n{'=' * 60}")
    print("Consistency Check")
    print(f"{'=' * 60}")

    query_names = [r.name for r in next(iter(all_results.values()))]
    all_consistent = True
    for query_name in query_names:
        row_counts = {}
        for format_name, results in all_results.items():
            for result in results:
                if result.name == query_name:
                    row_counts[format_name] = result.row_count

        unique_counts = set(row_counts.values())
        if len(unique_counts) > 1:
            all_consistent = False
            print(f"  MISMATCH: {query_name}: {row_counts}")
        else:
            print(f"  OK: {query_name}: {row_counts}")

    if all_consistent:
        print("\nAll formats returned consistent results!")
    else:
        print("\nWARNING: Some formats returned different result counts!")


def generate_report(all_results: dict[str, list[QueryResult]], output_path: Path) -> None:
    formats = list(all_results.keys())
    queries = all_results[formats[0]]

    lines = []
    lines.append("# AIS Parquet Format Benchmark Report")
    lines.append("")
    lines.append("## Configuration")
    lines.append("")
    lines.append(f"- **Monolithic**: `{MONOLITHIC_PATH}`")
    lines.append(f"- **Partitioned**: `{PARTITIONED_PATH}` (year/month/day)")
    lines.append(f"- **Partitioned + Bloom**: `{PARTITIONED_BLOOM_PATH}` (year/month/day, bloom filters)")
    lines.append(
        f"- **Partitioned + Hilbert**: `{PARTITIONED_HILBERT_PATH}` "
        "(year/month/day, hilbert_index)"
    )
    lines.append(
        f"- **Partitioned + Hilbert + Bloom**: `{PARTITIONED_HILBERT_BLOOM_PATH}` "
        "(year/month/day, hilbert_index, bloom filters)"
    )
    lines.append("- **Engine**: PyArrow Dataset / PyArrow Compute")
    lines.append("- **Warmup runs**: 1 metadata/cache warmup")
    lines.append("- **Timed runs**: 2 per query")
    lines.append(f"- **Query timeout**: {QUERY_TIMEOUT}s")
    lines.append("- **Partitioned formats use Hive partition filters** on year/month/day")
    lines.append("- **Hilbert formats use hilbert_index interval prefilters** plus exact predicate refinement")
    lines.append("")

    lines.append("## Results (Median Time in Seconds)")
    lines.append("")
    lines.append("| Query | " + " | ".join(formats) + " | Best |")
    lines.append("|-------|" + "|".join(["------"] * len(formats)) + "|------|")

    for query in queries:
        row = [query.name]
        best_format = None
        best_time = float("inf")
        times = {}
        for fmt in formats:
            result = next(r for r in all_results[fmt] if r.name == query.name)
            t = result.median_time
            row.append(f"{t:.4f}s" if t > 0 else "TIMEOUT")
            if 0 < t < best_time:
                best_time = t
                best_format = fmt
            times[fmt] = t

        valid_times = {name: value for name, value in times.items() if value > 0}
        if len(valid_times) >= 2:
            fastest = min(valid_times.values())
            slowest = max(valid_times.values())
            row.append(f"{best_format} ({slowest / fastest:.1f}x)")
        else:
            row.append(best_format or "N/A")
        lines.append("| " + " | ".join(row) + " |")

    lines.append("")
    lines.append("## Detailed Results")
    lines.append("")
    for fmt in formats:
        lines.append(f"### {fmt}")
        lines.append("")
        lines.append("| Query | Result Count | Min (s) | Mean (s) | Median (s) | Max (s) |")
        lines.append("|-------|--------------|---------|----------|------------|---------|")
        for result in all_results[fmt]:
            timeout_marker = " timeout" if result.timed_out else ""
            lines.append(
                f"| {result.name}{timeout_marker} | {result.row_count} | {result.min_time:.4f} | "
                f"{result.mean_time:.4f} | {result.median_time:.4f} | {result.max_time:.4f} |"
            )
        lines.append("")

    lines.append("## PyArrow Operations Used Per Format")
    lines.append("")
    for fmt in formats:
        lines.append(f"### {fmt}")
        lines.append("")
        lines.append("| Query | Operation / Filter |")
        lines.append("|-------|--------------------|")
        for result in all_results[fmt]:
            lines.append(f"| {result.name} | `{result.operation_used}` |")
        lines.append("")

    lines.append("## Summary")
    lines.append("")
    lines.append("| Format | Total Median Time (s) | Avg Query Time (s) |")
    lines.append("|--------|-----------------------|--------------------|")
    for fmt in formats:
        total = sum(result.median_time for result in all_results[fmt])
        avg = total / len(all_results[fmt])
        lines.append(f"| {fmt} | {total:.4f} | {avg:.4f} |")
    lines.append("")

    categories = []
    for qdef in QUERIES:
        if qdef.category not in categories:
            categories.append(qdef.category)

    lines.append("## Results by Category")
    lines.append("")
    for category in categories:
        lines.append(f"### {category}")
        lines.append("")
        lines.append("| Query | " + " | ".join(formats) + " |")
        lines.append("|-------|" + "|".join(["------"] * len(formats)) + "|")
        for qdef in [query for query in QUERIES if query.category == category]:
            row = [qdef.name]
            for fmt in formats:
                result = next(r for r in all_results[fmt] if r.name == qdef.name)
                row.append(f"{result.median_time:.4f}s" if result.median_time > 0 else "TIMEOUT")
            lines.append("| " + " | ".join(row) + " |")
        lines.append("")

    lines.append("---")
    lines.append(f"*Generated on {time.strftime('%Y-%m-%d %H:%M:%S')}*")
    output_path.write_text("\n".join(lines))
    print(f"\nReport written to: {output_path}")


def main() -> None:
    print("AIS Parquet Format Benchmark")
    print("=" * 60)

    for fmt in FORMATS:
        if not fmt.path.exists():
            print(f"ERROR: Path not found for {fmt.name}: {fmt.path}")
            return

    hilbert_bounds = metadata_bounds(MONOLITHIC_PATH)
    print(
        "Hilbert bounds derived from Parquet metadata: "
        f"time_us=[{hilbert_bounds.min_time_us}, {hilbert_bounds.max_time_us}], "
        f"LAT=[{hilbert_bounds.min_lat}, {hilbert_bounds.max_lat}], "
        f"LON=[{hilbert_bounds.min_lon}, {hilbert_bounds.max_lon}]"
    )

    all_results = {}
    for fmt in FORMATS:
        all_results[fmt.name] = benchmark_format(fmt, QUERIES, hilbert_bounds)

    verify_consistency(all_results)
    generate_report(all_results, BASE_DIR / "benchmark_report.md")
    print("\nBenchmark complete!")


if __name__ == "__main__":
    main()
