use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{
    Array, Float32Array, Float64Array, Int64Array, LargeStringArray, RecordBatch, StringArray,
    StringViewArray, TimestampMicrosecondArray, UInt64Array,
};
use arrow::compute::kernels::cast::cast;
use arrow::datatypes::{DataType, TimeUnit};
use clap::Parser;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use parquet::basic::Type;
use parquet::bloom_filter::Sbbf;
use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData, RowGroupMetaData};
use parquet::file::properties::ReaderProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::serialized_reader::ReadOptionsBuilder;
use parquet::file::statistics::Statistics;

const HILBERT_BITS: u32 = 32;
const HILBERT_MAX_AXIS: u32 = u32::MAX;
const HILBERT_MAX_INTERVALS: usize = 512;

#[derive(Debug, Parser)]
#[command(about = "Benchmark AIS Parquet layouts with Rust Arrow/Parquet readers")]
struct Args {
    #[arg(long, default_value = "parquet/ais_2024.parquet")]
    source: PathBuf,

    #[arg(long, default_value = "partition_parquet")]
    partitioned: PathBuf,

    #[arg(long, default_value = "partition_bloom_parquet")]
    partitioned_bloom: PathBuf,

    #[arg(long, default_value = "partition_hilbert_parquet")]
    partitioned_hilbert: PathBuf,

    #[arg(long, default_value = "partition_hilbert_bloom_parquet")]
    partitioned_hilbert_bloom: PathBuf,

    #[arg(long, default_value = "rust_benchmark_report.md")]
    report: PathBuf,

    #[arg(long, default_value_t = 8192)]
    batch_size: usize,

    #[arg(long, default_value_t = 1)]
    warmup_runs: usize,

    #[arg(long, default_value_t = 2)]
    timed_runs: usize,
}

#[derive(Clone)]
struct FormatSpec {
    name: &'static str,
    path: PathBuf,
    partitioned: bool,
    bloom: bool,
    hilbert: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
struct PartitionKey {
    year: i32,
    month: i32,
    day: i32,
}

#[derive(Clone)]
struct FileEntry {
    path: PathBuf,
    partition: Option<PartitionKey>,
}

#[derive(Clone, Copy)]
struct TimeRange {
    start_us: i64,
    end_us: i64,
}

#[derive(Clone)]
struct RangeFilter {
    column: &'static str,
    lower: f64,
    upper: f64,
}

#[derive(Clone)]
struct EqualityFilter {
    column: &'static str,
    value: &'static str,
}

#[derive(Clone)]
struct InFilter {
    column: &'static str,
    values: &'static [&'static str],
}

#[derive(Clone)]
struct EqualityFilterI64 {
    column: &'static str,
    value: i64,
}

#[derive(Clone)]
struct InFilterI64 {
    column: &'static str,
    values: &'static [i64],
}

#[derive(Clone, Copy)]
enum StringMatchKind {
    Prefix,
    Substring,
}

#[derive(Clone)]
struct StringMatch {
    column: &'static str,
    pattern: &'static str,
    kind: StringMatchKind,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Operation {
    Count,
    Rows,
    GroupByVesselType,
    CountByDay,
    DistinctMmsi,
}

#[derive(Clone)]
struct QueryDef {
    name: &'static str,
    category: &'static str,
    operation: Operation,
    equalities: Vec<EqualityFilter>,
    in_filters: Vec<InFilter>,
    equalities_i64: Vec<EqualityFilterI64>,
    in_filters_i64: Vec<InFilterI64>,
    ranges: Vec<RangeFilter>,
    time_range: Option<TimeRange>,
    string_match: Option<StringMatch>,
}

#[derive(Clone, Copy)]
struct HilbertBounds {
    min_lat: f64,
    max_lat: f64,
    min_lon: f64,
    max_lon: f64,
}

#[derive(Default, Clone)]
struct Metrics {
    files_total: usize,
    files_skipped_partition: usize,
    files_scanned: usize,
    row_groups_total: usize,
    row_groups_skipped_stats: usize,
    row_groups_skipped_hilbert: usize,
    row_groups_skipped_bloom: usize,
    row_groups_decoded: usize,
    rows_decoded: usize,
    rows_matched: usize,
}

#[derive(Clone)]
struct QueryRun {
    count: usize,
    elapsed: Duration,
    metrics: Metrics,
}

struct QueryResult {
    name: &'static str,
    count: usize,
    times: Vec<Duration>,
    metrics: Metrics,
}

#[derive(Default)]
struct Accumulators {
    vessel_types: HashSet<String>,
    days: HashSet<i64>,
    mmsis: HashSet<i64>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let formats = vec![
        FormatSpec {
            name: "monolithic",
            path: args.source.clone(),
            partitioned: false,
            bloom: false,
            hilbert: false,
        },
        FormatSpec {
            name: "partitioned",
            path: args.partitioned.clone(),
            partitioned: true,
            bloom: false,
            hilbert: false,
        },
        FormatSpec {
            name: "partitioned_bloom",
            path: args.partitioned_bloom.clone(),
            partitioned: true,
            bloom: true,
            hilbert: false,
        },
        FormatSpec {
            name: "partition_hilbert",
            path: args.partitioned_hilbert.clone(),
            partitioned: true,
            bloom: false,
            hilbert: true,
        },
        FormatSpec {
            name: "partition_hilbert_bloom",
            path: args.partitioned_hilbert_bloom.clone(),
            partitioned: true,
            bloom: true,
            hilbert: true,
        },
    ];

    for format in &formats {
        if !format.path.exists() {
            bail!(
                "missing benchmark path for {}: {}",
                format.name,
                format.path.display()
            );
        }
    }

    let queries = queries();
    let bounds = metadata_bounds(&args.source)?;
    println!(
        "Hilbert bounds: LAT=[{}, {}] LON=[{}, {}]",
        bounds.min_lat, bounds.max_lat, bounds.min_lon, bounds.max_lon
    );

    let mut all_results = HashMap::new();
    for format in &formats {
        println!("\n{}", "=".repeat(80));
        println!("Benchmarking {} ({})", format.name, format.path.display());
        println!("{}", "=".repeat(80));
        let files = collect_parquet_files(&format.path, format.partitioned)?;
        println!("discovered {} parquet files", files.len());

        for _ in 0..args.warmup_runs {
            let _ = run_query(format, &files, &queries[0], bounds, args.batch_size)?;
        }

        let mut results = Vec::new();
        for (index, query) in queries.iter().enumerate() {
            print!("  [{}/{}] {}... ", index + 1, queries.len(), query.name);
            std::io::stdout().flush().ok();
            let mut runs = Vec::new();
            for _ in 0..args.timed_runs {
                runs.push(run_query(format, &files, query, bounds, args.batch_size)?);
            }
            let count = runs.last().map(|run| run.count).unwrap_or_default();
            let times = runs.iter().map(|run| run.elapsed).collect::<Vec<_>>();
            let metrics = runs
                .last()
                .map(|run| run.metrics.clone())
                .unwrap_or_default();
            println!(
                "count={} median={:.4}s decoded_row_groups={} stats_skip={} hilbert_skip={} bloom_skip={}",
                count,
                median_duration(&times).as_secs_f64(),
                metrics.row_groups_decoded,
                metrics.row_groups_skipped_stats,
                metrics.row_groups_skipped_hilbert,
                metrics.row_groups_skipped_bloom
            );
            results.push(QueryResult {
                name: query.name,
                count,
                times,
                metrics,
            });
        }
        all_results.insert(format.name, results);
    }

    verify_consistency(&formats, &queries, &all_results);
    write_report(&args.report, &formats, &queries, &all_results)?;
    println!("\nReport written to {}", args.report.display());
    Ok(())
}

fn queries() -> Vec<QueryDef> {
    vec![
        q("MMSI exact lookup", "Point Lookups", Operation::Rows).eq_i64("MMSI", 563073700),
        q(
            "MMSI multi lookup (5 MMSIs)",
            "Point Lookups",
            Operation::Count,
        )
        .isin_i64(
            "MMSI",
            &[563073700, 248669000, 367481310, 367776660, 319194000],
        ),
        q("IMO exact lookup", "Point Lookups", Operation::Rows).eq_i64("IMO", 9348651),
        q(
            "IMO multi lookup (4 IMOs)",
            "Point Lookups",
            Operation::Count,
        )
        .isin_i64("IMO", &[9348651, 9199323, 9826146, 9843807]),
        q(
            "VesselName exact match",
            "Vessel Name / Callsign",
            Operation::Rows,
        )
        .eq("VesselName", "MAERSK MEMPHIS"),
        q(
            "VesselName LIKE prefix",
            "Vessel Name / Callsign",
            Operation::Count,
        )
        .string_prefix("VesselName", "MAERSK"),
        q(
            "CallSign exact match",
            "Vessel Name / Callsign",
            Operation::Rows,
        )
        .eq("CallSign", "WDK6270"),
        q("Single day time range", "Time Range", Operation::Count)
            .time("2024-10-20 00", "2024-10-21 00"),
        q("Week-long time range", "Time Range", Operation::Count)
            .time("2024-10-14 00", "2024-10-21 00"),
        q("Month-long time range", "Time Range", Operation::Count)
            .time("2024-10-01 00", "2024-11-01 00"),
        q("Specific hour window", "Time Range", Operation::Count)
            .time("2024-10-20 04", "2024-10-20 05"),
        q(
            "Bounding box: Gulf of Mexico",
            "Bounding Box",
            Operation::Count,
        )
        .range("LAT", 25.0, 31.0)
        .range("LON", -96.0, -80.0),
        q(
            "Bounding box: Pacific NW (Puget Sound)",
            "Bounding Box",
            Operation::Count,
        )
        .range("LAT", 47.0, 49.0)
        .range("LON", -124.0, -122.0),
        q(
            "Bounding box: New York Harbor",
            "Bounding Box",
            Operation::Count,
        )
        .range("LAT", 40.5, 41.0)
        .range("LON", -74.5, -73.5),
        q("Bounding box: San Diego", "Bounding Box", Operation::Count)
            .range("LAT", 32.5, 33.0)
            .range("LON", -117.5, -117.0),
        q("MMSI + time range", "Combined Queries", Operation::Rows)
            .eq_i64("MMSI", 563073700)
            .time("2024-10-01 00", "2024-11-01 00"),
        q(
            "Bounding box + time range",
            "Combined Queries",
            Operation::Count,
        )
        .range("LAT", 29.0, 31.0)
        .range("LON", -95.0, -89.0)
        .time("2024-10-14 00", "2024-10-21 00"),
        q(
            "VesselName + time range",
            "Combined Queries",
            Operation::Rows,
        )
        .eq("VesselName", "MAERSK MEMPHIS")
        .time("2024-10-01 00", "2024-11-01 00"),
        q(
            "VesselType + bounding box",
            "Combined Queries",
            Operation::Count,
        )
        .eq("VesselType", "70")
        .range("LAT", 25.0, 31.0)
        .range("LON", -96.0, -80.0),
        q("COUNT all rows", "Aggregations", Operation::Count),
        q(
            "COUNT by VesselType",
            "Aggregations",
            Operation::GroupByVesselType,
        ),
        q("COUNT by day", "Aggregations", Operation::CountByDay),
        q(
            "Distinct MMSI count",
            "Aggregations",
            Operation::DistinctMmsi,
        ),
        q(
            "Bloom test: CallSign = exact (exists)",
            "Bloom Filter Effectiveness",
            Operation::Count,
        )
        .eq("CallSign", "WDJ6270"),
        q(
            "Bloom test: CallSign = exact (not exists)",
            "Bloom Filter Effectiveness",
            Operation::Count,
        )
        .eq("CallSign", "ZZZZZZZZ"),
        q(
            "Bloom test: VesselName = exact (exists)",
            "Bloom Filter Effectiveness",
            Operation::Count,
        )
        .eq("VesselName", "MAERSK MEMPHIS"),
        q(
            "Bloom test: VesselName = exact (not exists)",
            "Bloom Filter Effectiveness",
            Operation::Count,
        )
        .eq("VesselName", "NONEXISTENT SHIP NAME"),
        q(
            "Bloom test: IMO = exact (exists)",
            "Bloom Filter Effectiveness",
            Operation::Count,
        )
        .eq_i64("IMO", 9348651),
        q(
            "Bloom test: IMO = exact (not exists)",
            "Bloom Filter Effectiveness",
            Operation::Count,
        )
        .eq_i64("IMO", 1),
        q(
            "Bloom test: CallSign LIKE prefix",
            "Bloom Filter Effectiveness",
            Operation::Count,
        )
        .string_prefix("CallSign", "WDK"),
        q(
            "Bloom test: VesselName LIKE substring",
            "Bloom Filter Effectiveness",
            Operation::Count,
        )
        .string_substring("VesselName", "SPIRIT"),
    ]
}

fn q(name: &'static str, category: &'static str, operation: Operation) -> QueryDef {
    QueryDef {
        name,
        category,
        operation,
        equalities: Vec::new(),
        in_filters: Vec::new(),
        equalities_i64: Vec::new(),
        in_filters_i64: Vec::new(),
        ranges: Vec::new(),
        time_range: None,
        string_match: None,
    }
}

impl QueryDef {
    fn eq(mut self, column: &'static str, value: &'static str) -> Self {
        self.equalities.push(EqualityFilter { column, value });
        self
    }

    #[allow(dead_code)]
    fn isin(mut self, column: &'static str, values: &'static [&'static str]) -> Self {
        self.in_filters.push(InFilter { column, values });
        self
    }

    fn eq_i64(mut self, column: &'static str, value: i64) -> Self {
        self.equalities_i64
            .push(EqualityFilterI64 { column, value });
        self
    }

    fn isin_i64(mut self, column: &'static str, values: &'static [i64]) -> Self {
        self.in_filters_i64.push(InFilterI64 { column, values });
        self
    }

    fn range(mut self, column: &'static str, lower: f64, upper: f64) -> Self {
        self.ranges.push(RangeFilter {
            column,
            lower,
            upper,
        });
        self
    }

    fn time(mut self, start: &'static str, end: &'static str) -> Self {
        self.time_range = Some(TimeRange {
            start_us: parse_hour_us(start),
            end_us: parse_hour_us(end),
        });
        self
    }

    fn string_prefix(mut self, column: &'static str, pattern: &'static str) -> Self {
        self.string_match = Some(StringMatch {
            column,
            pattern,
            kind: StringMatchKind::Prefix,
        });
        self
    }

    fn string_substring(mut self, column: &'static str, pattern: &'static str) -> Self {
        self.string_match = Some(StringMatch {
            column,
            pattern,
            kind: StringMatchKind::Substring,
        });
        self
    }
}

fn run_query(
    format: &FormatSpec,
    files: &[FileEntry],
    query: &QueryDef,
    bounds: HilbertBounds,
    batch_size: usize,
) -> Result<QueryRun> {
    let started = Instant::now();
    let hilbert_intervals = if format.hilbert {
        query_hilbert_intervals(query, bounds)
    } else {
        Vec::new()
    };
    let mut metrics = Metrics::default();
    let mut matched = 0usize;
    let mut accumulators = Accumulators::default();

    for entry in files {
        metrics.files_total += 1;
        if format.partitioned && partition_skips(entry.partition, query.time_range) {
            metrics.files_skipped_partition += 1;
            continue;
        }
        metrics.files_scanned += 1;

        let file = File::open(&entry.path)
            .with_context(|| format!("failed to open {}", entry.path.display()))?;
        let reader = SerializedFileReader::new_with_options(
            file,
            ReadOptionsBuilder::new()
                .with_reader_properties(
                    ReaderProperties::builder()
                        .set_read_bloom_filter(format.bloom)
                        .build(),
                )
                .build(),
        )
        .with_context(|| {
            format!(
                "failed to read parquet metadata for {}",
                entry.path.display()
            )
        })?;
        let metadata = reader.metadata();
        let column_indexes = parquet_column_indexes(metadata);
        let mut selected = Vec::new();

        for row_group_index in 0..metadata.num_row_groups() {
            metrics.row_groups_total += 1;
            let row_group = metadata.row_group(row_group_index);
            if stats_skip_row_group(row_group, &column_indexes, query)? {
                metrics.row_groups_skipped_stats += 1;
                continue;
            }
            if !hilbert_intervals.is_empty()
                && hilbert_skip_row_group(row_group, &column_indexes, &hilbert_intervals)?
            {
                metrics.row_groups_skipped_hilbert += 1;
                continue;
            }
            if format.bloom
                && bloom_skip_row_group(
                    &reader,
                    row_group_index,
                    row_group,
                    &column_indexes,
                    query,
                )?
            {
                metrics.row_groups_skipped_bloom += 1;
                continue;
            }
            selected.push(row_group_index);
            metrics.row_groups_decoded += 1;
        }

        if selected.is_empty() {
            continue;
        }

        if query_has_no_row_filter(query) && matches!(query.operation, Operation::Count) {
            let rows = selected
                .iter()
                .map(|index| metadata.row_group(*index).num_rows() as usize)
                .sum::<usize>();
            metrics.rows_decoded += rows;
            matched += rows;
            continue;
        }

        let decode_file = File::open(&entry.path)
            .with_context(|| format!("failed to reopen {}", entry.path.display()))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(decode_file)
            .with_context(|| format!("failed to build reader for {}", entry.path.display()))?;
        let projection = projection_for_query(builder.metadata(), query);
        let mut batch_reader = builder
            .with_batch_size(batch_size)
            .with_row_groups(selected)
            .with_projection(projection)
            .build()
            .with_context(|| {
                format!("failed to build batch reader for {}", entry.path.display())
            })?;

        while let Some(batch) = batch_reader.next() {
            let batch = batch.context("failed to read record batch")?;
            metrics.rows_decoded += batch.num_rows();
            process_batch(&batch, query, &mut matched, &mut accumulators)?;
        }
    }

    let count = match query.operation {
        Operation::GroupByVesselType => accumulators.vessel_types.len(),
        Operation::CountByDay => accumulators.days.len(),
        Operation::DistinctMmsi => accumulators.mmsis.len(),
        Operation::Count | Operation::Rows => matched,
    };
    metrics.rows_matched = count;

    Ok(QueryRun {
        count,
        elapsed: started.elapsed(),
        metrics,
    })
}

fn process_batch(
    batch: &RecordBatch,
    query: &QueryDef,
    matched: &mut usize,
    accumulators: &mut Accumulators,
) -> Result<()> {
    for row in 0..batch.num_rows() {
        if !row_matches(batch, query, row)? {
            continue;
        }
        match query.operation {
            Operation::GroupByVesselType => {
                if let Some(value) = string_value(batch, "VesselType", row)? {
                    accumulators.vessel_types.insert(value.to_string());
                }
            }
            Operation::CountByDay => {
                if let Some(value) = timestamp_value(batch, "BaseDateTime", row)? {
                    accumulators.days.insert(value.div_euclid(86_400_000_000));
                }
            }
            Operation::DistinctMmsi => {
                if let Some(value) = int64_value(batch, "MMSI", row)? {
                    accumulators.mmsis.insert(value);
                }
            }
            Operation::Count | Operation::Rows => *matched += 1,
        }
    }
    Ok(())
}

fn row_matches(batch: &RecordBatch, query: &QueryDef, row: usize) -> Result<bool> {
    for equality in &query.equalities {
        if string_value(batch, equality.column, row)? != Some(equality.value) {
            return Ok(false);
        }
    }
    for in_filter in &query.in_filters {
        let Some(value) = string_value(batch, in_filter.column, row)? else {
            return Ok(false);
        };
        if !in_filter.values.contains(&value) {
            return Ok(false);
        }
    }
    for equality in &query.equalities_i64 {
        if int64_value(batch, equality.column, row)? != Some(equality.value) {
            return Ok(false);
        }
    }
    for in_filter in &query.in_filters_i64 {
        let Some(value) = int64_value(batch, in_filter.column, row)? else {
            return Ok(false);
        };
        if !in_filter.values.contains(&value) {
            return Ok(false);
        }
    }
    for range in &query.ranges {
        let Some(value) = f64_value(batch, range.column, row)? else {
            return Ok(false);
        };
        if value < range.lower || value > range.upper {
            return Ok(false);
        }
    }
    if let Some(time_range) = query.time_range {
        let Some(value) = timestamp_value(batch, "BaseDateTime", row)? else {
            return Ok(false);
        };
        if value < time_range.start_us || value >= time_range.end_us {
            return Ok(false);
        }
    }
    if let Some(string_match) = &query.string_match {
        let Some(value) = string_value(batch, string_match.column, row)? else {
            return Ok(false);
        };
        match string_match.kind {
            StringMatchKind::Prefix if !value.starts_with(string_match.pattern) => {
                return Ok(false)
            }
            StringMatchKind::Substring if !value.contains(string_match.pattern) => {
                return Ok(false)
            }
            _ => {}
        }
    }
    Ok(true)
}

fn projection_for_query(metadata: &ParquetMetaData, query: &QueryDef) -> ProjectionMask {
    let mut names = HashSet::new();
    for equality in &query.equalities {
        names.insert(equality.column);
    }
    for in_filter in &query.in_filters {
        names.insert(in_filter.column);
    }
    for equality in &query.equalities_i64 {
        names.insert(equality.column);
    }
    for in_filter in &query.in_filters_i64 {
        names.insert(in_filter.column);
    }
    for range in &query.ranges {
        names.insert(range.column);
    }
    if query.time_range.is_some() || query.operation == Operation::CountByDay {
        names.insert("BaseDateTime");
    }
    if let Some(string_match) = &query.string_match {
        names.insert(string_match.column);
    }
    match query.operation {
        Operation::GroupByVesselType => {
            names.insert("VesselType");
        }
        Operation::DistinctMmsi => {
            names.insert("MMSI");
        }
        _ => {}
    }

    if names.is_empty() {
        return ProjectionMask::leaves(metadata.file_metadata().schema_descr(), []);
    }

    let indexes = metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(index, column)| {
            names
                .contains(column.path().string().as_str())
                .then_some(index)
        });
    ProjectionMask::leaves(metadata.file_metadata().schema_descr(), indexes)
}

fn query_has_no_row_filter(query: &QueryDef) -> bool {
    query.equalities.is_empty()
        && query.in_filters.is_empty()
        && query.equalities_i64.is_empty()
        && query.in_filters_i64.is_empty()
        && query.ranges.is_empty()
        && query.time_range.is_none()
        && query.string_match.is_none()
}

fn collect_parquet_files(path: &Path, partitioned: bool) -> Result<Vec<FileEntry>> {
    let mut files = Vec::new();
    if path.is_file() {
        files.push(FileEntry {
            path: path.to_path_buf(),
            partition: None,
        });
    } else {
        collect_parquet_files_inner(path, partitioned, &mut files)?;
    }
    files.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(files)
}

fn collect_parquet_files_inner(
    path: &Path,
    partitioned: bool,
    files: &mut Vec<FileEntry>,
) -> Result<()> {
    for entry in fs::read_dir(path).with_context(|| format!("failed to read {}", path.display()))? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_parquet_files_inner(&path, partitioned, files)?;
        } else if path.extension().and_then(|value| value.to_str()) == Some("parquet") {
            files.push(FileEntry {
                partition: partitioned.then(|| parse_partition_key(&path)).flatten(),
                path,
            });
        }
    }
    Ok(())
}

fn parse_partition_key(path: &Path) -> Option<PartitionKey> {
    let mut year = None;
    let mut month = None;
    let mut day = None;
    for component in path.components() {
        let value = component.as_os_str().to_str()?;
        if let Some(rest) = value.strip_prefix("year=") {
            year = rest.parse().ok();
        } else if let Some(rest) = value.strip_prefix("month=") {
            month = rest.parse().ok();
        } else if let Some(rest) = value.strip_prefix("day=") {
            day = rest.parse().ok();
        }
    }
    Some(PartitionKey {
        year: year?,
        month: month?,
        day: day?,
    })
}

fn partition_skips(partition: Option<PartitionKey>, time_range: Option<TimeRange>) -> bool {
    let (Some(partition), Some(time_range)) = (partition, time_range) else {
        return false;
    };
    let start_day = day_number(partition.year, partition.month, partition.day);
    let partition_start = start_day * 86_400_000_000;
    let partition_end = partition_start + 86_400_000_000;
    partition_end <= time_range.start_us || partition_start >= time_range.end_us
}

fn parquet_column_indexes(metadata: &ParquetMetaData) -> HashMap<String, usize> {
    metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| (column.path().string(), index))
        .collect()
}

fn stats_skip_row_group(
    row_group: &RowGroupMetaData,
    indexes: &HashMap<String, usize>,
    query: &QueryDef,
) -> Result<bool> {
    if let Some(time_range) = query.time_range {
        if let Some((min, max)) = int64_stats(row_group, indexes, "BaseDateTime")? {
            if max < time_range.start_us || min >= time_range.end_us {
                return Ok(true);
            }
        }
    }
    for range in &query.ranges {
        if let Some((min, max)) = f64_stats(row_group, indexes, range.column)? {
            if max < range.lower || min > range.upper {
                return Ok(true);
            }
        }
    }
    for equality in &query.equalities {
        if let Some((min, max)) = byte_stats(row_group, indexes, equality.column) {
            let value = equality.value.as_bytes();
            if value < min || value > max {
                return Ok(true);
            }
        }
    }
    for in_filter in &query.in_filters {
        if let Some((min, max)) = byte_stats(row_group, indexes, in_filter.column) {
            let overlaps = in_filter.values.iter().any(|value| {
                let value = value.as_bytes();
                value >= min && value <= max
            });
            if !overlaps {
                return Ok(true);
            }
        }
    }
    for equality in &query.equalities_i64 {
        if let Some((min, max)) = int64_stats(row_group, indexes, equality.column)? {
            if equality.value < min || equality.value > max {
                return Ok(true);
            }
        }
    }
    for in_filter in &query.in_filters_i64 {
        if let Some((min, max)) = int64_stats(row_group, indexes, in_filter.column)? {
            let overlaps = in_filter
                .values
                .iter()
                .any(|value| *value >= min && *value <= max);
            if !overlaps {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn hilbert_skip_row_group(
    row_group: &RowGroupMetaData,
    indexes: &HashMap<String, usize>,
    intervals: &[(u64, u64)],
) -> Result<bool> {
    let Some((min, max)) = u64_stats(row_group, indexes, "hilbert_index")? else {
        return Ok(false);
    };
    Ok(!intervals
        .iter()
        .any(|(start, end)| max >= *start && min <= *end))
}

fn bloom_skip_row_group<R: parquet::file::reader::ChunkReader + 'static>(
    reader: &SerializedFileReader<R>,
    row_group_index: usize,
    row_group: &RowGroupMetaData,
    indexes: &HashMap<String, usize>,
    query: &QueryDef,
) -> Result<bool> {
    if query.equalities.is_empty()
        && query.in_filters.is_empty()
        && query.equalities_i64.is_empty()
        && query.in_filters_i64.is_empty()
    {
        return Ok(false);
    }
    let row_group_reader = reader
        .get_row_group(row_group_index)
        .context("failed to read row group bloom filters")?;

    for equality in &query.equalities {
        let Some(index) = indexes.get(equality.column).copied() else {
            continue;
        };
        if let Some(filter) = row_group_reader.get_column_bloom_filter(index) {
            if !check_bloom(filter, equality.value, row_group.column(index))? {
                return Ok(true);
            }
        }
    }
    for in_filter in &query.in_filters {
        let Some(index) = indexes.get(in_filter.column).copied() else {
            continue;
        };
        if let Some(filter) = row_group_reader.get_column_bloom_filter(index) {
            let mut any_present = false;
            for value in in_filter.values {
                any_present |= check_bloom(filter, value, row_group.column(index))?;
            }
            if !any_present {
                return Ok(true);
            }
        }
    }
    for equality in &query.equalities_i64 {
        let Some(index) = indexes.get(equality.column).copied() else {
            continue;
        };
        if let Some(filter) = row_group_reader.get_column_bloom_filter(index) {
            if !check_bloom(filter, &equality.value.to_string(), row_group.column(index))? {
                return Ok(true);
            }
        }
    }
    for in_filter in &query.in_filters_i64 {
        let Some(index) = indexes.get(in_filter.column).copied() else {
            continue;
        };
        if let Some(filter) = row_group_reader.get_column_bloom_filter(index) {
            let mut any_present = false;
            for value in in_filter.values {
                any_present |= check_bloom(filter, &value.to_string(), row_group.column(index))?;
            }
            if !any_present {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn check_bloom(filter: &Sbbf, value: &str, column: &ColumnChunkMetaData) -> Result<bool> {
    match column.column_type() {
        Type::INT32 => Ok(filter.check(&value.parse::<i32>()?)),
        Type::INT64 => Ok(filter.check(&value.parse::<i64>()?)),
        Type::BYTE_ARRAY => Ok(filter.check(&value)),
        other => Err(anyhow!("unsupported bloom column type {other:?}")),
    }
}

fn int64_stats(
    row_group: &RowGroupMetaData,
    indexes: &HashMap<String, usize>,
    column: &str,
) -> Result<Option<(i64, i64)>> {
    let Some(index) = indexes.get(column).copied() else {
        return Ok(None);
    };
    let Some(stats) = row_group.column(index).statistics() else {
        return Ok(None);
    };
    Ok(match stats {
        Statistics::Int64(values) => values
            .min_opt()
            .zip(values.max_opt())
            .map(|(min, max)| (*min, *max)),
        _ => None,
    })
}

fn u64_stats(
    row_group: &RowGroupMetaData,
    indexes: &HashMap<String, usize>,
    column: &str,
) -> Result<Option<(u64, u64)>> {
    Ok(int64_stats(row_group, indexes, column)?.map(|(min, max)| (min as u64, max as u64)))
}

fn f64_stats(
    row_group: &RowGroupMetaData,
    indexes: &HashMap<String, usize>,
    column: &str,
) -> Result<Option<(f64, f64)>> {
    let Some(index) = indexes.get(column).copied() else {
        return Ok(None);
    };
    let Some(stats) = row_group.column(index).statistics() else {
        return Ok(None);
    };
    Ok(match stats {
        Statistics::Double(values) => values
            .min_opt()
            .zip(values.max_opt())
            .map(|(min, max)| (*min, *max)),
        Statistics::Float(values) => values
            .min_opt()
            .zip(values.max_opt())
            .map(|(min, max)| (*min as f64, *max as f64)),
        _ => None,
    })
}

fn byte_stats<'a>(
    row_group: &'a RowGroupMetaData,
    indexes: &HashMap<String, usize>,
    column: &str,
) -> Option<(&'a [u8], &'a [u8])> {
    let index = indexes.get(column).copied()?;
    let stats = row_group.column(index).statistics()?;
    match stats {
        Statistics::ByteArray(values) => values.min_bytes_opt().zip(values.max_bytes_opt()),
        Statistics::FixedLenByteArray(values) => values.min_bytes_opt().zip(values.max_bytes_opt()),
        _ => None,
    }
}

fn metadata_bounds(path: &Path) -> Result<HilbertBounds> {
    let files = collect_parquet_files(path, false)?;
    let mut min_lat = f64::INFINITY;
    let mut max_lat = f64::NEG_INFINITY;
    let mut min_lon = f64::INFINITY;
    let mut max_lon = f64::NEG_INFINITY;

    for entry in files {
        let file = File::open(&entry.path)?;
        let reader = SerializedFileReader::new(file)?;
        let metadata = reader.metadata();
        let indexes = parquet_column_indexes(metadata);
        for row_group in metadata.row_groups() {
            if let Some((min, max)) = f64_stats(row_group, &indexes, "LAT")? {
                min_lat = min_lat.min(min);
                max_lat = max_lat.max(max);
            }
            if let Some((min, max)) = f64_stats(row_group, &indexes, "LON")? {
                min_lon = min_lon.min(min);
                max_lon = max_lon.max(max);
            }
        }
    }

    if !min_lat.is_finite() || !min_lon.is_finite() {
        bail!("failed to derive Hilbert bounds from metadata")
    }
    Ok(HilbertBounds {
        min_lat,
        max_lat,
        min_lon,
        max_lon,
    })
}

fn query_hilbert_intervals(query: &QueryDef, bounds: HilbertBounds) -> Vec<(u64, u64)> {
    let has_spatial = query
        .ranges
        .iter()
        .any(|range| range.column == "LAT" || range.column == "LON");
    if !has_spatial {
        return Vec::new();
    }

    let mut lat_min = bounds.min_lat;
    let mut lat_max = bounds.max_lat;
    let mut lon_min = bounds.min_lon;
    let mut lon_max = bounds.max_lon;
    for range in &query.ranges {
        match range.column {
            "LAT" => {
                lat_min = lat_min.max(range.lower);
                lat_max = lat_max.min(range.upper);
            }
            "LON" => {
                lon_min = lon_min.max(range.lower);
                lon_max = lon_max.min(range.upper);
            }
            _ => {}
        }
    }

    if lat_min > lat_max || lon_min > lon_max {
        return Vec::new();
    }

    let query = [
        (
            scale_f64(lat_min, bounds.min_lat, bounds.max_lat),
            scale_f64(lat_max, bounds.min_lat, bounds.max_lat),
        ),
        (
            scale_f64(lon_min, bounds.min_lon, bounds.max_lon),
            scale_f64(lon_max, bounds.min_lon, bounds.max_lon),
        ),
    ];
    let mut intervals = Vec::new();
    visit_hilbert_cell_2d(0, 0, HILBERT_BITS, &query, &mut intervals);
    merge_intervals(intervals)
}

fn visit_hilbert_cell_2d(
    a0: u32,
    b0: u32,
    bits: u32,
    query: &[(u32, u32); 2],
    intervals: &mut Vec<(u64, u64)>,
) {
    let size = 1u32 << bits;
    let a1 = a0 + size - 1;
    let b1 = b0 + size - 1;
    if a1 < query[0].0 || a0 > query[0].1 || b1 < query[1].0 || b0 > query[1].1 {
        return;
    }
    let fully_inside = query[0].0 <= a0 && a1 <= query[0].1 && query[1].0 <= b0 && b1 <= query[1].1;
    if fully_inside || bits == 0 || intervals.len() >= HILBERT_MAX_INTERVALS {
        intervals.push(cell_interval_2d(a0, b0, bits));
        return;
    }
    let child_bits = bits - 1;
    let child_size = 1u32 << child_bits;
    let mut children = Vec::with_capacity(4);
    for da in [0, child_size] {
        for db in [0, child_size] {
            let child = (a0 + da, b0 + db);
            children.push((hilbert_index_2d([child.0, child.1]), child));
        }
    }
    children.sort_by_key(|child| child.0);
    for (_, child) in children {
        visit_hilbert_cell_2d(child.0, child.1, child_bits, query, intervals);
    }
}

fn cell_interval_2d(a0: u32, b0: u32, remaining_bits: u32) -> (u64, u64) {
    let prefix_shift = 2 * remaining_bits;
    if prefix_shift == 0 {
        let code = hilbert_index_2d([a0, b0]);
        return (code, code);
    }
    if prefix_shift >= 64 {
        return (0, u64::MAX);
    }
    let prefix = hilbert_index_2d([a0, b0]) >> prefix_shift;
    let start = prefix << prefix_shift;
    let size = 1u64 << prefix_shift;
    let end = start.wrapping_add(size).wrapping_sub(1);
    (start, end)
}

fn merge_intervals(mut intervals: Vec<(u64, u64)>) -> Vec<(u64, u64)> {
    if intervals.is_empty() {
        return intervals;
    }
    intervals.sort_unstable();
    let mut merged = vec![intervals[0]];
    for (start, end) in intervals.into_iter().skip(1) {
        let last = merged.last_mut().expect("non-empty");
        if start > last.1.saturating_add(1) {
            merged.push((start, end));
        } else {
            last.1 = last.1.max(end);
        }
    }
    merged
}

fn scale_f64(value: f64, min: f64, max: f64) -> u32 {
    if max <= min || !value.is_finite() {
        return 0;
    }
    (((value - min) / (max - min)).clamp(0.0, 1.0) * HILBERT_MAX_AXIS as f64).round() as u32
}

fn hilbert_index_2d(point: [u32; 2]) -> u64 {
    let mut transpose = [point[0] as u64, point[1] as u64];
    axes_to_hilbert_transpose_2d(&mut transpose, HILBERT_BITS);
    let mut index = 0;
    for bit in (0..HILBERT_BITS).rev() {
        for axis in transpose {
            index = (index << 1) | ((axis >> bit) & 1);
        }
    }
    index
}

fn axes_to_hilbert_transpose_2d(axes: &mut [u64; 2], bits: u32) {
    let m = 1u64 << (bits - 1);
    let mut q = m;
    while q > 1 {
        let p = q - 1;
        for i in 0..2 {
            if axes[i] & q != 0 {
                axes[0] ^= p;
            } else {
                let t = (axes[0] ^ axes[i]) & p;
                axes[0] ^= t;
                axes[i] ^= t;
            }
        }
        q >>= 1;
    }
    axes[1] ^= axes[0];
}

fn string_value<'a>(batch: &'a RecordBatch, column: &str, row: usize) -> Result<Option<&'a str>> {
    let Some(index) = batch.schema().index_of(column).ok() else {
        return Ok(None);
    };
    let array = batch.column(index);
    if array.is_null(row) {
        return Ok(None);
    }
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(Some(values.value(row)));
    }
    if let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(Some(values.value(row)));
    }
    if let Some(values) = array.as_any().downcast_ref::<StringViewArray>() {
        return Ok(Some(values.value(row)));
    }
    Err(anyhow!("column {column} is not a string array"))
}

fn f64_value(batch: &RecordBatch, column: &str, row: usize) -> Result<Option<f64>> {
    let Some(index) = batch.schema().index_of(column).ok() else {
        return Ok(None);
    };
    let array = batch.column(index);
    if array.is_null(row) {
        return Ok(None);
    }
    if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
        return Ok(Some(values.value(row)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Float32Array>() {
        return Ok(Some(values.value(row) as f64));
    }
    Err(anyhow!("column {column} is not Float64 or Float32"))
}

fn int64_value(batch: &RecordBatch, column: &str, row: usize) -> Result<Option<i64>> {
    let Some(index) = batch.schema().index_of(column).ok() else {
        return Ok(None);
    };
    let array = batch.column(index);
    if array.is_null(row) {
        return Ok(None);
    }
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(Some(values.value(row)));
    }
    Err(anyhow!("column {column} is not Int64"))
}

fn timestamp_value(batch: &RecordBatch, column: &str, row: usize) -> Result<Option<i64>> {
    let Some(index) = batch.schema().index_of(column).ok() else {
        return Ok(None);
    };
    let array = batch.column(index);
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| anyhow!("BaseDateTime downcast failed"))?;
            Ok(Some(values.value(row)))
        }
        DataType::Timestamp(_, _) => {
            let micros = cast(array, &DataType::Timestamp(TimeUnit::Microsecond, None))
                .context("failed to cast timestamp to microseconds")?;
            let values = micros
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| anyhow!("BaseDateTime microsecond downcast failed"))?;
            Ok(Some(values.value(row)))
        }
        DataType::Int64 => {
            let values = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("Int64 downcast failed"))?;
            Ok(Some(values.value(row)))
        }
        other => Err(anyhow!("column {column} is not timestamp/int64: {other:?}")),
    }
}

#[allow(dead_code)]
fn u64_value(batch: &RecordBatch, column: &str, row: usize) -> Result<Option<u64>> {
    let Some(index) = batch.schema().index_of(column).ok() else {
        return Ok(None);
    };
    let array = batch.column(index);
    if array.is_null(row) {
        return Ok(None);
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
        return Ok(Some(values.value(row)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(Some(values.value(row) as u64));
    }
    Err(anyhow!("column {column} is not UInt64/Int64"))
}

fn parse_hour_us(value: &str) -> i64 {
    let year = value[0..4].parse::<i32>().expect("year");
    let month = value[5..7].parse::<i32>().expect("month");
    let day = value[8..10].parse::<i32>().expect("day");
    let hour = value[11..13].parse::<i64>().expect("hour");
    (day_number(year, month, day) * 86_400 + hour * 3_600) * 1_000_000
}

fn day_number(year: i32, month: i32, day: i32) -> i64 {
    days_from_civil(year, month, day)
}

// Days since Unix epoch, adapted from Howard Hinnant's civil calendar algorithm.
fn days_from_civil(year: i32, month: i32, day: i32) -> i64 {
    let year = year - i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = month as i64;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day as i64 - 1;
    let doe = yoe as i64 * 365 + yoe as i64 / 4 - yoe as i64 / 100 + doy;
    era as i64 * 146_097 + doe - 719_468
}

fn median_duration(values: &[Duration]) -> Duration {
    if values.is_empty() {
        return Duration::ZERO;
    }
    let mut values = values.to_vec();
    values.sort_unstable();
    values[values.len() / 2]
}

fn mean_duration(values: &[Duration]) -> Duration {
    if values.is_empty() {
        return Duration::ZERO;
    }
    Duration::from_secs_f64(
        values.iter().map(|value| value.as_secs_f64()).sum::<f64>() / values.len() as f64,
    )
}

fn verify_consistency(
    formats: &[FormatSpec],
    queries: &[QueryDef],
    results: &HashMap<&'static str, Vec<QueryResult>>,
) {
    println!("\nConsistency Check");
    println!("{}", "=".repeat(80));
    for (query_index, query) in queries.iter().enumerate() {
        let counts = formats
            .iter()
            .map(|format| (format.name, results[format.name][query_index].count))
            .collect::<Vec<_>>();
        let consistent = counts
            .iter()
            .map(|(_, count)| *count)
            .collect::<HashSet<_>>()
            .len()
            == 1;
        println!(
            "{}: {} {:?}",
            if consistent { "OK" } else { "MISMATCH" },
            query.name,
            counts
        );
    }
}

fn write_report(
    path: &Path,
    formats: &[FormatSpec],
    queries: &[QueryDef],
    results: &HashMap<&'static str, Vec<QueryResult>>,
) -> Result<()> {
    let mut out = String::new();
    out.push_str("# AIS Rust Parquet Benchmark Report\n\n");
    out.push_str("## Formats\n\n");
    for format in formats {
        out.push_str(&format!(
            "- **{}**: `{}`{}{}{}\n",
            format.name,
            format.path.display(),
            if format.partitioned {
                " partition-pruned"
            } else {
                ""
            },
            if format.hilbert {
                " hilbert-pruned"
            } else {
                ""
            },
            if format.bloom { " bloom-pruned" } else { "" }
        ));
    }
    out.push_str("\n## Median Time Summary\n\n");
    out.push_str("| Query | ");
    out.push_str(
        &formats
            .iter()
            .map(|f| f.name)
            .collect::<Vec<_>>()
            .join(" | "),
    );
    out.push_str(" | Best | 2nd Best | 3rd Best | 4th Best | 5th Best |\n|---|");
    out.push_str(&vec!["---"; formats.len()].join("|"));
    out.push_str("|---|---|---|---|---|\n");
    for (query_index, query) in queries.iter().enumerate() {
        out.push_str(&format!("| {} |", query.name));
        let mut ranked: Vec<(&str, Duration)> = formats
            .iter()
            .map(|f| (f.name, median_duration(&results[f.name][query_index].times)))
            .collect();
        ranked.sort_unstable_by_key(|(_, d)| *d);
        for format in formats {
            let median = median_duration(&results[format.name][query_index].times);
            out.push_str(&format!(" {:.4}s |", median.as_secs_f64()));
        }
        for i in 0..5 {
            if let Some((name, duration)) = ranked.get(i) {
                out.push_str(&format!(" {} ({:.4}s) |", name, duration.as_secs_f64()));
            } else {
                out.push_str(" - |");
            }
        }
        out.push('\n');
    }

    out.push_str("\n## Detailed Results\n\n");
    for format in formats {
        out.push_str(&format!("### {}\n\n", format.name));
        out.push_str("| Category | Query | Count | Min | Mean | Median | Max | Files Scanned | RG Total | RG Stats Skip | RG Hilbert Skip | RG Bloom Skip | RG Decoded | Rows Decoded |\n");
        out.push_str("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        for (query_index, result) in results[format.name].iter().enumerate() {
            let min = result.times.iter().min().copied().unwrap_or_default();
            let max = result.times.iter().max().copied().unwrap_or_default();
            let mean = mean_duration(&result.times);
            let median = median_duration(&result.times);
            let m = &result.metrics;
            out.push_str(&format!(
                "| {} | {} | {} | {:.4} | {:.4} | {:.4} | {:.4} | {} | {} | {} | {} | {} | {} | {} |\n",
                queries[query_index].category,
                result.name,
                result.count,
                min.as_secs_f64(),
                mean.as_secs_f64(),
                median.as_secs_f64(),
                max.as_secs_f64(),
                m.files_scanned,
                m.row_groups_total,
                m.row_groups_skipped_stats,
                m.row_groups_skipped_hilbert,
                m.row_groups_skipped_bloom,
                m.row_groups_decoded,
                m.rows_decoded
            ));
        }
        out.push('\n');
    }

    fs::write(path, out).with_context(|| format!("failed to write {}", path.display()))
}
