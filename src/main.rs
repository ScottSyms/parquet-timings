use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{
    Array, BooleanBuilder, Float32Array, Float32Builder, Float64Array, Int32Array, Int64Builder,
    LargeStringArray, RecordBatch, StringArray, StringBuilder, StringViewArray,
    TimestampMicrosecondBuilder,
};
use arrow::compute::kernels::cast::cast;
use arrow::csv::ReaderBuilder;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use clap::{Parser, ValueEnum};
use datafusion::datasource::MemTable;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::*;
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{BloomFilterPosition, EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;
use tokio::sync::Semaphore;

const SOURCE_TABLE: &str = "ais";
const STAGING_TABLE: &str = "hilbert_staging";
const PARTITION_COLUMNS: [&str; 3] = ["year", "month", "day"];
const HILBERT_COLUMN: &str = "hilbert_index";
const PROGRESS_INTERVAL: Duration = Duration::from_secs(5);

const DEFAULT_BLOOM_COLUMNS: [&str; 2] = ["MMSI", "IMO"];

fn parse_bloom_ndv_per_column(s: &str) -> Result<(String, u64)> {
    let parts: Vec<&str> = s.split('=').collect();
    if parts.len() != 2 {
        anyhow::bail!("invalid --bloom-ndv-per-column format: {s:?}. Expected COLUMN=NDV (e.g. MMSI=50000)")
    }
    let ndv: u64 = parts[1].parse()
        .map_err(|_| anyhow::anyhow!("invalid NDV value in --bloom-ndv-per-column: {s:?}"))?;
    Ok((parts[0].to_string(), ndv))
}

#[derive(Debug, Clone, Parser)]
#[command(about = "Create optimized Parquet layouts for AIS data")]
struct Args {
    #[arg(long, default_value = "parquet/ais_2024.parquet")]
    source: PathBuf,

    #[arg(long, default_value = "csv")]
    csv_dir: PathBuf,

    #[arg(long)]
    rebuild_source: bool,

    #[arg(long, default_value = "partition_parquet")]
    partition_output: PathBuf,

    #[arg(long, default_value = "partition_bloom_parquet")]
    bloom_output: PathBuf,

    #[arg(long, default_value = "partition_hilbert_parquet")]
    hilbert_output: PathBuf,

    #[arg(long, default_value = "partition_hilbert_bloom_parquet")]
    hilbert_bloom_output: PathBuf,

    #[arg(long, default_value = ".datafusion-spill")]
    spill_dir: PathBuf,

    #[arg(long, default_value = ".hilbert-staging-parquet")]
    hilbert_staging_dir: PathBuf,

    #[arg(long, default_value_t = 1_000_000)]
    row_group_size: usize,

    #[arg(long, default_value_t = 8192)]
    batch_size: usize,

    #[arg(long, value_enum, default_value_t = Only::All)]
    only: Only,

    #[arg(long)]
    overwrite: bool,

    #[arg(long)]
    jobs: Option<usize>,

    #[arg(long = "bloom-column")]
    bloom_columns: Vec<String>,

    #[arg(long)]
    in_memory: bool,

    /// ZSTD compression level (1-22)
    #[arg(long, default_value_t = 6)]
    compression: i32,

    /// Data page size limit in bytes (0 = parquet-rs default ~1MB)
    #[arg(long, default_value_t = 0)]
    data_page_size: usize,

    /// Bloom filter false positive rate
    #[arg(long, default_value_t = 0.01)]
    bloom_fpp: f64,

    /// Bloom filter distinct value count (default for all columns)
    #[arg(long, default_value_t = 1_000_000)]
    bloom_ndv: u64,

    /// Per-column bloom filter NDV overrides (format: COLUMN=NDV)
    #[arg(long = "bloom-ndv-per-column", value_parser = parse_bloom_ndv_per_column)]
    bloom_ndv_per_column: Vec<(String, u64)>,

    /// Cap row group size to average rows per partition
    #[arg(long, default_value_t = false)]
    smart_row_group_size: bool,

    /// Maximum CSV rows to process (default: all)
    #[arg(long)]
    max_rows: Option<usize>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Only {
    All,
    Source,
    Partition,
    Bloom,
    Hilbert,
    HilbertBloom,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PartitionKey {
    year: i32,
    month: i32,
    day: i32,
}

#[derive(Clone, Copy, Debug)]
struct HilbertBounds {
    min_lat: f64,
    max_lat: f64,
    min_lon: f64,
    max_lon: f64,
}

struct PartitionWriter {
    root: PathBuf,
    props: Arc<WriterProperties>,
    active: Option<ActivePartitionWriter>,
}

struct ActivePartitionWriter {
    key: PartitionKey,
    writer: ArrowWriter<File>,
}

struct Progress {
    label: String,
    started: Instant,
    last_report: Instant,
    rows: usize,
    batches: usize,
}

impl PartitionKey {
    fn label(self) -> String {
        format!("year={}/month={}/day={}", self.year, self.month, self.day)
    }
}

impl Progress {
    fn new(label: String) -> Self {
        let now = Instant::now();
        Self {
            label,
            started: now,
            last_report: now,
            rows: 0,
            batches: 0,
        }
    }

    fn record(&mut self, rows: usize) -> Result<()> {
        self.rows += rows;
        self.batches += 1;
        if self.last_report.elapsed() >= PROGRESS_INTERVAL {
            self.report("progress")?;
            self.last_report = Instant::now();
        }
        Ok(())
    }

    fn finish(&self) -> Result<()> {
        self.report("done")
    }

    fn report(&self, state: &str) -> Result<()> {
        let elapsed = self.started.elapsed().as_secs_f64();
        let rows_per_sec = if elapsed > 0.0 {
            self.rows as f64 / elapsed
        } else {
            0.0
        };
        status(format!(
            "{} {}: {} rows, {} batches, {:.1}s elapsed, {:.0} rows/s",
            self.label, state, self.rows, self.batches, elapsed, rows_per_sec
        ))
    }
}

fn status(message: impl AsRef<str>) -> Result<()> {
    println!("[ais-parquet-optimizer] {}", message.as_ref());
    io::stdout()
        .flush()
        .context("failed to flush status output")
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    status(format!(
        "starting optimizer: source={}, csv={}, only={:?}, row_group_size={}, batch_size={}",
        args.source.display(),
        args.csv_dir.display(),
        args.only,
        args.row_group_size,
        args.batch_size
    ))?;

    fs::create_dir_all(&args.spill_dir)
        .with_context(|| format!("failed to create spill dir {}", args.spill_dir.display()))?;

    let bloom_columns: Vec<String> = args
        .bloom_columns
        .clone()
        .into_iter()
        .chain(DEFAULT_BLOOM_COLUMNS.iter().map(|c| c.to_string()))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    let ctx = new_context(&args)?;

    if args.rebuild_source || !args.source.exists() {
        if !args.rebuild_source {
            bail!(
                "source parquet file does not exist: {}. Pass --rebuild-source to create from CSV",
                args.source.display()
            );
        }
        build_source_from_csv(&ctx, &args).await?;
    }

    if args.only == Only::Source {
        return Ok(());
    }

    register_source(&ctx, &args.source).await?;
    status("registered source parquet with DataFusion")?;

    let bloom_ndv_per_col: HashMap<String, u64> = args.bloom_ndv_per_column.iter().cloned().collect();

    let effective_rgs = if args.smart_row_group_size {
        let total: i64 = ctx
            .sql("SELECT COUNT(*) AS cnt FROM ais")
            .await?
            .collect()
            .await?
            .first()
            .and_then(|b| b.column(0).as_any().downcast_ref::<arrow::array::Int64Array>())
            .map(|a| a.value(0))
            .ok_or_else(|| anyhow!("failed to get total row count"))?;
        let keys = read_partition_keys(&ctx).await?;
        let avg = (total as usize) / keys.len().max(1);
        let capped = args.row_group_size.min(avg);
        status(format!(
            "smart-rgs: total_rows={total}, partitions={np}, avg/partition={avg}, user_rgs={ur}, effective_rgs={capped}",
            total = total, np = keys.len(), avg = avg, ur = args.row_group_size, capped = capped
        ))?;
        capped
    } else {
        args.row_group_size
    };

    match args.only {
        Only::All => {
            prepare_output_dir(&args.partition_output, args.overwrite)?;
            prepare_output_dir(&args.bloom_output, args.overwrite)?;
            prepare_output_dir(&args.hilbert_output, args.overwrite)?;
            prepare_output_dir(&args.hilbert_bloom_output, args.overwrite)?;

            status("discovering non-empty year/month/day partitions")?;
            let partition_keys = read_partition_keys(&ctx).await?;
            status(format!(
                "found {} non-empty partitions (jobs={})",
                partition_keys.len(),
                args.jobs.unwrap_or_else(num_cpus::get)
            ))?;

            status("reading Hilbert scaling bounds from source")?;
            let bounds = read_hilbert_bounds(&ctx).await?;
            status(format!(
                "Hilbert bounds: LAT=[{}, {}], LON=[{}, {}]",
                bounds.min_lat, bounds.max_lat, bounds.min_lon, bounds.max_lon
            ))?;

            let outputs = AllOutputSpecs::new(&args, &bloom_columns, effective_rgs, &bloom_ndv_per_col);
            let jobs = args.jobs.unwrap_or_else(num_cpus::get).max(1);

            if jobs <= 1 {
                for key in partition_keys {
                    process_partition_all(
                        Arc::new(ctx.clone()),
                        Arc::new(args.clone()),
                        bounds,
                        key,
                        outputs.clone(),
                    )
                    .await?;
                }
            } else {
                let sem = Arc::new(Semaphore::new(jobs));
                let mut handles = Vec::new();
                for key in partition_keys {
                    let permit = sem.clone().acquire_owned().await?;
                    let ctx = Arc::new(ctx.clone());
                    let args = Arc::new(args.clone());
                    let outputs = outputs.clone();
                    let handle = tokio::spawn(async move {
                        let _permit = permit;
                        process_partition_all(ctx, args, bounds, key, outputs).await
                    });
                    handles.push(handle);
                }
                for handle in handles {
                    handle.await??;
                }
            }
        }
        Only::Partition => {
            prepare_output_dir(&args.partition_output, args.overwrite)?;
            write_datetime_sorted_outputs(
                &ctx,
                &args,
                vec![OutputSpec::new(
                    &args.partition_output,
                    false,
                    &bloom_columns,
                    effective_rgs,
                    args.compression,
                    args.data_page_size,
                    args.bloom_fpp,
                    args.bloom_ndv,
                    &bloom_ndv_per_col,
                )],
            )
            .await?;
        }
        Only::Bloom => {
            prepare_output_dir(&args.bloom_output, args.overwrite)?;
            write_datetime_sorted_outputs(
                &ctx,
                &args,
                vec![OutputSpec::new(
                    &args.bloom_output,
                    true,
                    &bloom_columns,
                    effective_rgs,
                    args.compression,
                    args.data_page_size,
                    args.bloom_fpp,
                    args.bloom_ndv,
                    &bloom_ndv_per_col,
                )],
            )
            .await?;
        }
        Only::Hilbert => {
            prepare_output_dir(&args.hilbert_output, args.overwrite)?;
            write_hilbert_outputs(
                &ctx,
                &args,
                vec![OutputSpec::new(
                    &args.hilbert_output,
                    false,
                    &bloom_columns,
                    effective_rgs,
                    args.compression,
                    args.data_page_size,
                    args.bloom_fpp,
                    args.bloom_ndv,
                    &bloom_ndv_per_col,
                )],
            )
            .await?;
        }
        Only::HilbertBloom => {
            prepare_output_dir(&args.hilbert_bloom_output, args.overwrite)?;
            write_hilbert_outputs(
                &ctx,
                &args,
                vec![OutputSpec::new(
                    &args.hilbert_bloom_output,
                    true,
                    &bloom_columns,
                    effective_rgs,
                    args.compression,
                    args.data_page_size,
                    args.bloom_fpp,
                    args.bloom_ndv,
                    &bloom_ndv_per_col,
                )],
            )
            .await?;
        }
        Only::Source => {}
    }

    Ok(())
}

fn new_context(args: &Args) -> Result<SessionContext> {
    let mut config = SessionConfig::new()
        .with_target_partitions(num_cpus::get())
        .with_batch_size(args.batch_size)
        .with_parquet_pruning(true);

    config.options_mut().execution.sort_spill_reservation_bytes = 10 * 1024 * 1024;
    config.options_mut().execution.sort_in_place_threshold_bytes = 128 * 1024 * 1024;

    let runtime = Arc::new(
        RuntimeEnvBuilder::new()
            .with_temp_file_path(&args.spill_dir)
            .build()
            .context("failed to build DataFusion runtime")?,
    );

    Ok(SessionContext::new_with_config_rt(config, runtime))
}

async fn register_source(ctx: &SessionContext, source: &Path) -> Result<()> {
    ctx.register_parquet(
        SOURCE_TABLE,
        source
            .to_str()
            .ok_or_else(|| anyhow!("source path is not valid UTF-8"))?,
        ParquetReadOptions::default(),
    )
    .await
    .context("failed to register source parquet")
}

fn csv_raw_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("MMSI", DataType::Utf8, true),
        Field::new("BaseDateTime", DataType::Utf8, true),
        Field::new("LAT", DataType::Utf8, true),
        Field::new("LON", DataType::Utf8, true),
        Field::new("SOG", DataType::Utf8, true),
        Field::new("COG", DataType::Utf8, true),
        Field::new("Heading", DataType::Utf8, true),
        Field::new("VesselName", DataType::Utf8, true),
        Field::new("IMO", DataType::Utf8, true),
        Field::new("CallSign", DataType::Utf8, true),
        Field::new("VesselType", DataType::Utf8, true),
        Field::new("Status", DataType::Utf8, true),
        Field::new("Length", DataType::Utf8, true),
        Field::new("Width", DataType::Utf8, true),
        Field::new("Draft", DataType::Utf8, true),
        Field::new("Cargo", DataType::Utf8, true),
        Field::new("TransceiverClass", DataType::Utf8, true),
    ]))
}

fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("MMSI", DataType::Int64, true),
        Field::new(
            "BaseDateTime",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("LAT", DataType::Float32, true),
        Field::new("LON", DataType::Float32, true),
        Field::new("SOG", DataType::Float32, true),
        Field::new("COG", DataType::Float32, true),
        Field::new("Heading", DataType::Float32, true),
        Field::new("VesselName", DataType::Utf8, true),
        Field::new("IMO", DataType::Int64, true),
        Field::new("CallSign", DataType::Utf8, true),
        Field::new("VesselType", DataType::Utf8, true),
        Field::new("Status", DataType::Utf8, true),
        Field::new("Length", DataType::Float32, true),
        Field::new("Width", DataType::Float32, true),
        Field::new("Draft", DataType::Float32, true),
        Field::new("Cargo", DataType::Utf8, true),
        Field::new("TransceiverClass", DataType::Boolean, true),
    ]))
}

fn transform_csv_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    let rows = batch.num_rows();

    let get_str = |col: usize, row: usize| -> Option<&str> {
        let array = batch.column(col);
        if array.is_null(row) {
            return None;
        }
        let s = if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
            a.value(row)
        } else if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
            a.value(row)
        } else if let Some(a) = array.as_any().downcast_ref::<StringViewArray>() {
            a.value(row)
        } else {
            return None;
        };
        if s.is_empty() {
            None
        } else {
            Some(s)
        }
    };

    let mut mmsi = Int64Builder::with_capacity(rows);
    let mut base_dt = TimestampMicrosecondBuilder::with_capacity(rows);
    let mut lat = Float32Builder::with_capacity(rows);
    let mut lon = Float32Builder::with_capacity(rows);
    let mut sog = Float32Builder::with_capacity(rows);
    let mut cog = Float32Builder::with_capacity(rows);
    let mut heading = Float32Builder::with_capacity(rows);
    let mut vessel_name = StringBuilder::with_capacity(rows, 128);
    let mut imo = Int64Builder::with_capacity(rows);
    let mut call_sign = StringBuilder::with_capacity(rows, 64);
    let mut vessel_type = StringBuilder::with_capacity(rows, 16);
    let mut status = StringBuilder::with_capacity(rows, 16);
    let mut length = Float32Builder::with_capacity(rows);
    let mut width = Float32Builder::with_capacity(rows);
    let mut draft = Float32Builder::with_capacity(rows);
    let mut cargo = StringBuilder::with_capacity(rows, 16);
    let mut transceiver_class = BooleanBuilder::with_capacity(rows);

    for row in 0..rows {
        mmsi.append_option(get_str(0, row).and_then(|s| s.parse::<i64>().ok()));
        base_dt.append_option(get_str(1, row).and_then(|s| parse_base_dt_micros(s).ok()));
        lat.append_option(get_str(2, row).and_then(|s| s.parse::<f32>().ok()));
        lon.append_option(get_str(3, row).and_then(|s| s.parse::<f32>().ok()));
        sog.append_option(get_str(4, row).and_then(|s| s.parse::<f32>().ok()));
        cog.append_option(get_str(5, row).and_then(|s| s.parse::<f32>().ok()));
        heading.append_option(get_str(6, row).and_then(|s| s.parse::<f32>().ok()));
        vessel_name.append_option(get_str(7, row));
        imo.append_option(
            get_str(8, row)
                .map(|s| s.strip_prefix("IMO").unwrap_or(s))
                .and_then(|s| s.parse::<i64>().ok()),
        );
        call_sign.append_option(get_str(9, row));
        vessel_type.append_option(get_str(10, row));
        status.append_option(get_str(11, row));
        length.append_option(get_str(12, row).and_then(|s| s.parse::<f32>().ok()));
        width.append_option(get_str(13, row).and_then(|s| s.parse::<f32>().ok()));
        draft.append_option(get_str(14, row).and_then(|s| s.parse::<f32>().ok()));
        cargo.append_option(get_str(15, row));
        transceiver_class.append_option(get_str(16, row).map(|s| s == "B"));
    }

    let columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(mmsi.finish()),
        Arc::new(base_dt.finish()),
        Arc::new(lat.finish()),
        Arc::new(lon.finish()),
        Arc::new(sog.finish()),
        Arc::new(cog.finish()),
        Arc::new(heading.finish()),
        Arc::new(vessel_name.finish()),
        Arc::new(imo.finish()),
        Arc::new(call_sign.finish()),
        Arc::new(vessel_type.finish()),
        Arc::new(status.finish()),
        Arc::new(length.finish()),
        Arc::new(width.finish()),
        Arc::new(draft.finish()),
        Arc::new(cargo.finish()),
        Arc::new(transceiver_class.finish()),
    ];

    RecordBatch::try_new(output_schema(), columns).context("failed to create transformed batch")
}

fn parse_base_dt_micros(s: &str) -> Result<i64> {
    let s = s.trim();
    if s.len() < 19 {
        bail!("timestamp too short: {s}");
    }
    let year: i64 = s[0..4].parse()?;
    let month: i32 = s[5..7].parse()?;
    let day: i32 = s[8..10].parse()?;
    let hour: i32 = s[11..13].parse()?;
    let min: i32 = s[14..16].parse()?;
    let sec: i32 = s[17..19].parse()?;

    let offset_secs = if s.len() > 19 {
        let bytes = s.as_bytes();
        if bytes[19] == b'+' || bytes[19] == b'-' {
            let sign: i64 = if bytes[19] == b'-' { -1 } else { 1 };
            let tz_hours: i64 = s[20..22].parse()?;
            sign * tz_hours * 3600
        } else {
            0
        }
    } else {
        0
    };

    let micros = datetime_to_micros(year, month, day, hour, min, sec);
    Ok(micros - offset_secs * 1_000_000)
}

fn datetime_to_micros(year: i64, month: i32, day: i32, hour: i32, min: i32, sec: i32) -> i64 {
    let days = days_from_civil(year, month, day);
    (days * 86400 + hour as i64 * 3600 + min as i64 * 60 + sec as i64) * 1_000_000
}

fn days_from_civil(y: i64, m: i32, d: i32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (m as i64 + if m > 2 { -3 } else { 9 }) + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[allow(unused_variables)]
async fn build_source_from_csv(ctx: &SessionContext, args: &Args) -> Result<()> {
    let parent = args
        .source
        .parent()
        .ok_or_else(|| anyhow!("source has no parent directory"))?;
    if args.source.exists() {
        if !args.overwrite {
            bail!(
                "source already exists: {}. Pass --overwrite to replace",
                args.source.display()
            );
        }
        fs::remove_file(&args.source)
            .with_context(|| format!("failed to remove {}", args.source.display()))?;
    }
    fs::create_dir_all(parent).with_context(|| format!("failed to create {}", parent.display()))?;

    let mut csv_files: Vec<PathBuf> = fs::read_dir(&args.csv_dir)
        .with_context(|| format!("failed to read CSV dir {}", args.csv_dir.display()))?
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            if path.extension().and_then(|e| e.to_str()) == Some("csv") {
                Some(path)
            } else {
                None
            }
        })
        .collect();
    csv_files.sort();

    let total_files = csv_files.len();
    let source_props = writer_properties_for_source(args.row_group_size, args.compression, args.data_page_size);
    let csv_schema = csv_raw_schema();

    let mut writer: Option<ArrowWriter<File>> = None;
    let mut total_rows = 0usize;

    for (file_index, csv_path) in csv_files.iter().enumerate() {
        let file_name = csv_path
            .file_name()
            .map(|n| n.to_string_lossy())
            .unwrap_or_default();
        status(format!(
            "[{}/{}] reading {}",
            file_index + 1,
            total_files,
            file_name
        ))?;

        let file = File::open(csv_path)
            .with_context(|| format!("failed to open {}", csv_path.display()))?;
        let reader = io::BufReader::new(file);

        let expected_fields = csv_schema.fields().len(); // 17
        let mut file_rows = 0usize;
        let mut file_bad = 0usize;
        let mut csv_buf = Vec::new();
        let mut rows_in_buf = 0usize;

        for line_result in reader.lines() {
            if let Some(max) = args.max_rows {
                if total_rows >= max {
                    break;
                }
            }
            let line: String = match line_result {
                Ok(l) => l,
                Err(e) => {
                    status(format!("  io error reading line: {e}"))?;
                    continue;
                }
            };

            let comma_count = line.chars().filter(|&c| c == ',').count();

            // Detect and skip header lines and malformed lines across all files
            if line.starts_with("MMSI,")
                || (comma_count != expected_fields - 1 && comma_count != expected_fields)
            {
                file_bad += 1;
                if file_bad <= 5 {
                    status(format!(
                        "  skipped line {file_bad}: expected {expected_fields} cols, got {} cols",
                        comma_count + 1
                    ))?;
                }
            } else {
                csv_buf.extend_from_slice(line.as_bytes());
                csv_buf.push(b'\n');
                rows_in_buf += 1;
            }

            if rows_in_buf >= args.batch_size {
                let cursor = io::Cursor::new(&csv_buf);
                let inner_reader = ReaderBuilder::new(csv_schema.clone())
                    .with_header(false)
                    .with_batch_size(rows_in_buf)
                    .build(cursor)
                    .context("failed to create CSV reader")?;
                for batch_result in inner_reader {
                    let batch = batch_result.with_context(|| {
                        format!("failed to parse CSV batch in {}", csv_path.display())
                    })?;
                    let rows = batch.num_rows();
                    let transformed = transform_csv_batch(&batch)?;
                    if writer.is_none() {
                        let file = File::create(&args.source).with_context(|| {
                            format!("failed to create {}", args.source.display())
                        })?;
                        writer = Some(
                            ArrowWriter::try_new(
                                file,
                                transformed.schema(),
                                Some(source_props.clone()),
                            )
                            .context("failed to create ArrowWriter for source")?,
                        );
                    }
                    writer
                        .as_mut()
                        .expect("writer initialized")
                        .write(&transformed)
                        .context("failed to write source batch")?;
                    file_rows += rows;
                    total_rows += rows;
                }
                csv_buf.clear();
                rows_in_buf = 0;
            }
        }

        // flush remaining rows (skip if max_rows already reached)
        if rows_in_buf > 0 && args.max_rows.map_or(true, |max| total_rows < max) {
            let cursor = io::Cursor::new(&csv_buf);
            let inner_reader = ReaderBuilder::new(csv_schema.clone())
                .with_header(false)
                .with_batch_size(rows_in_buf)
                .build(cursor)
                .context("failed to create CSV reader")?;
            for batch_result in inner_reader {
                let batch = batch_result.with_context(|| {
                    format!("failed to parse CSV batch in {}", csv_path.display())
                })?;
                let rows = batch.num_rows();
                let transformed = transform_csv_batch(&batch)?;
                if writer.is_none() {
                    let file = File::create(&args.source)
                        .with_context(|| format!("failed to create {}", args.source.display()))?;
                    writer = Some(
                        ArrowWriter::try_new(
                            file,
                            transformed.schema(),
                            Some(source_props.clone()),
                        )
                        .context("failed to create ArrowWriter for source")?,
                    );
                }
                writer
                    .as_mut()
                    .expect("writer initialized")
                    .write(&transformed)
                    .context("failed to write source batch")?;
                file_rows += rows;
                total_rows += rows;
            }
        }

        if file_bad > 0 {
            status(format!("  rows: {file_rows}, skipped: {file_bad}"))?;
        } else {
            status(format!("  rows: {file_rows}"))?;
        }
    }

    if let Some(writer) = writer.take() {
        writer.close().context("failed to close source writer")?;
    }

    status(format!(
        "CSV source build done: {total_rows} rows across {total_files} files"
    ))?;

    Ok(())
}

fn prepare_output_dir(path: &Path, overwrite: bool) -> Result<()> {
    if path.exists() {
        if overwrite {
            fs::remove_dir_all(path)
                .with_context(|| format!("failed to remove {}", path.display()))?;
        } else {
            bail!(
                "output directory already exists: {}. Pass --overwrite to replace it",
                path.display()
            );
        }
    }

    fs::create_dir_all(path).with_context(|| format!("failed to create {}", path.display()))
}

#[derive(Clone)]
struct OutputSpec {
    root: PathBuf,
    props: Arc<WriterProperties>,
}

impl OutputSpec {
    fn new(root: &Path, bloom: bool, bloom_columns: &[String], row_group_size: usize, compression_level: i32, data_page_size: usize, bloom_fpp: f64, bloom_ndv: u64, bloom_ndv_per_column: &HashMap<String, u64>) -> Self {
        Self {
            root: root.to_path_buf(),
            props: Arc::new(writer_properties(bloom, bloom_columns, row_group_size, compression_level, data_page_size, bloom_fpp, bloom_ndv, bloom_ndv_per_column)),
        }
    }
}

#[derive(Clone)]
struct AllOutputSpecs {
    partition: OutputSpec,
    bloom: OutputSpec,
    hilbert: OutputSpec,
    hilbert_bloom: OutputSpec,
}

impl AllOutputSpecs {
    fn new(args: &Args, bloom_columns: &[String], effective_rgs: usize, bloom_ndv_per_column: &HashMap<String, u64>) -> Self {
        let c = args.compression;
        let dps = args.data_page_size;
        let fpp = args.bloom_fpp;
        let ndv = args.bloom_ndv;
        Self {
            partition: OutputSpec::new(
                &args.partition_output,
                false,
                bloom_columns,
                effective_rgs,
                c,
                dps,
                fpp,
                ndv,
                bloom_ndv_per_column,
            ),
            bloom: OutputSpec::new(
                &args.bloom_output,
                true,
                bloom_columns,
                effective_rgs,
                c,
                dps,
                fpp,
                ndv,
                bloom_ndv_per_column,
            ),
            hilbert: OutputSpec::new(
                &args.hilbert_output,
                false,
                bloom_columns,
                effective_rgs,
                c,
                dps,
                fpp,
                ndv,
                bloom_ndv_per_column,
            ),
            hilbert_bloom: OutputSpec::new(
                &args.hilbert_bloom_output,
                true,
                bloom_columns,
                effective_rgs,
                c,
                dps,
                fpp,
                ndv,
                bloom_ndv_per_column,
            ),
        }
    }
}

struct StagingCleanup(PathBuf);

impl StagingCleanup {
    fn new(path: PathBuf) -> Self {
        Self(path)
    }
}

impl Drop for StagingCleanup {
    fn drop(&mut self) {
        if self.0.exists() {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}

async fn write_datetime_sorted_outputs(
    ctx: &SessionContext,
    args: &Args,
    outputs: Vec<OutputSpec>,
) -> Result<()> {
    status("discovering non-empty year/month/day partitions")?;
    let partition_keys = read_partition_keys(ctx).await?;
    status(format!(
        "found {} non-empty partitions to write by BaseDateTime",
        partition_keys.len()
    ))?;

    let mut writers = outputs
        .into_iter()
        .map(|spec| PartitionWriter::new(spec.root, spec.props))
        .collect::<Vec<_>>();

    let mut total_rows = 0usize;
    for (index, key) in partition_keys.iter().copied().enumerate() {
        status(format!(
            "[{}/{}] sorting/writing partition {} by BaseDateTime",
            index + 1,
            partition_keys.len(),
            key.label()
        ))?;
        let sql = partition_select_sql(key, "\"BaseDateTime\"");
        let df = ctx
            .sql(&sql)
            .await
            .with_context(|| format!("failed to create sorted query for {}", key.label()))?;
        let mut stream = df
            .execute_stream()
            .await
            .with_context(|| format!("failed to execute sorted query for {}", key.label()))?;
        let mut progress = Progress::new(format!("BaseDateTime {}", key.label()));

        while let Some(batch) = stream.next().await {
            let batch =
                batch.with_context(|| format!("failed to read batch for {}", key.label()))?;
            let rows = batch.num_rows();
            write_partitioned_batch(&batch, &mut writers, false)?;
            progress.record(rows)?;
            total_rows += rows;
        }
        progress.finish()?;
    }

    for writer in &mut writers {
        writer.close()?;
    }

    println!(
        "wrote BaseDateTime-sorted outputs with row groups of {} ({} rows)",
        args.row_group_size, total_rows
    );
    Ok(())
}

async fn write_hilbert_outputs(
    ctx: &SessionContext,
    args: &Args,
    outputs: Vec<OutputSpec>,
) -> Result<()> {
    if !args.in_memory {
        if args.hilbert_staging_dir.exists() {
            fs::remove_dir_all(&args.hilbert_staging_dir).with_context(|| {
                format!(
                    "failed to remove staging dir {}",
                    args.hilbert_staging_dir.display()
                )
            })?;
        }
        fs::create_dir_all(&args.hilbert_staging_dir).with_context(|| {
            format!(
                "failed to create staging dir {}",
                args.hilbert_staging_dir.display()
            )
        })?;
    }

    status("reading Hilbert scaling bounds from source")?;
    let bounds = read_hilbert_bounds(ctx).await?;
    status(format!(
        "Hilbert bounds: LAT=[{}, {}], LON=[{}, {}]",
        bounds.min_lat, bounds.max_lat, bounds.min_lon, bounds.max_lon
    ))?;

    status("discovering non-empty year/month/day partitions for Hilbert output")?;
    let partition_keys = read_partition_keys(ctx).await?;
    status(format!(
        "found {} non-empty partitions to write by Hilbert index",
        partition_keys.len()
    ))?;

    let jobs = args.jobs.unwrap_or_else(num_cpus::get).max(1);
    let outputs = Arc::new(outputs);
    let ctx = Arc::new(ctx.clone());

    if jobs <= 1 {
        for key in partition_keys {
            process_partition_hilbert(
                ctx.clone(),
                Arc::new(args.clone()),
                bounds,
                key,
                outputs.clone(),
            )
            .await?;
        }
    } else {
        let sem = Arc::new(Semaphore::new(jobs));
        let mut handles = Vec::new();
        for key in partition_keys {
            let permit = sem.clone().acquire_owned().await?;
            let ctx = ctx.clone();
            let args = Arc::new(args.clone());
            let outputs = outputs.clone();
            let handle = tokio::spawn(async move {
                let _permit = permit;
                process_partition_hilbert(ctx, args, bounds, key, outputs).await
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.await??;
        }
    }

    if !args.in_memory {
        let _ = fs::remove_dir_all(&args.hilbert_staging_dir);
    }

    Ok(())
}

async fn process_partition_hilbert(
    ctx: Arc<SessionContext>,
    args: Arc<Args>,
    bounds: HilbertBounds,
    key: PartitionKey,
    outputs: Arc<Vec<OutputSpec>>,
) -> Result<()> {
    let table_name = format!("{STAGING_TABLE}_{}_{}_{}", key.year, key.month, key.day);

    let mut sorted_stream = if args.in_memory {
        let mut collected: Vec<RecordBatch> = Vec::new();

        let sql = partition_select_sql(key, "\"BaseDateTime\"");
        let df = ctx
            .sql(&sql)
            .await
            .with_context(|| format!("failed to create query for {}", key.label()))?;
        let mut src_stream = df
            .execute_stream()
            .await
            .with_context(|| format!("failed to execute query for {}", key.label()))?;

        let mut progress = Progress::new(format!("Hilbert in-memory {}", key.label()));
        while let Some(batch) = src_stream.next().await {
            let batch = batch
                .with_context(|| format!("failed to read batch for {}", key.label()))?;
            let hilbert_batch = append_hilbert_index(&batch, bounds)?;
            progress.record(batch.num_rows())?;
            collected.push(hilbert_batch);
        }
        progress.finish()?;

        if collected.is_empty() {
            return Ok(());
        }

        let schema = collected[0].schema();
        let mem_table =
            MemTable::try_new(schema, vec![collected]).context("failed to create MemTable")?;
        ctx.register_table(&table_name, Arc::new(mem_table))
            .context("failed to register MemTable")?;

        let sql = format!("SELECT * FROM {table_name} ORDER BY {HILBERT_COLUMN}");
        ctx
            .sql(&sql)
            .await
            .with_context(|| format!("failed to create sorted query for {}", key.label()))?
            .execute_stream()
            .await
            .with_context(|| format!("failed to execute sorted query for {}", key.label()))?
    } else {
        let staging_dir = args.hilbert_staging_dir.join(format!(
            "year={}/month={}/day={}",
            key.year, key.month, key.day
        ));

        if staging_dir.exists() {
            fs::remove_dir_all(&staging_dir)?;
        }
        fs::create_dir_all(&staging_dir)?;
        let _cleanup = StagingCleanup::new(staging_dir.clone());
        std::mem::forget(_cleanup); // Keep staging dir alive until stream is consumed; cleaned up on next run

        write_hilbert_staging(ctx.as_ref(), bounds, key, &staging_dir).await?;

        let staging_glob = staging_dir.join("*.parquet");
        ctx
            .read_parquet(
                staging_glob.to_str().ok_or_else(|| anyhow!("glob path UTF-8"))?,
                ParquetReadOptions::default(),
            )
            .await
            .with_context(|| format!("failed to read staging parquet for {}", key.label()))?
            .sort(vec![col(HILBERT_COLUMN).sort(true, true)])
            .with_context(|| format!("failed to sort staging data for {}", key.label()))?
            .execute_stream()
            .await
            .with_context(|| format!("failed to execute sorted staging for {}", key.label()))?
    };

    let mut writers = outputs
        .iter()
        .map(|spec| PartitionWriter::new(spec.root.clone(), spec.props.clone()))
        .collect::<Vec<_>>();

    let mut progress = Progress::new(format!("Hilbert sort/write {}", key.label()));
    while let Some(batch) = sorted_stream.next().await {
        let batch =
            batch.with_context(|| format!("failed to read sorted batch for {}", key.label()))?;
        let rows = batch.num_rows();
        write_partitioned_batch(&batch, &mut writers, true)?;
        progress.record(rows)?;
    }
    progress.finish()?;

    for w in &mut writers {
        w.close()?;
    }

    Ok(())
}

async fn process_partition_all(
    ctx: Arc<SessionContext>,
    args: Arc<Args>,
    bounds: HilbertBounds,
    key: PartitionKey,
    outputs: AllOutputSpecs,
) -> Result<()> {
    let sql = partition_select_sql(key, "\"BaseDateTime\"");
    let df = ctx
        .sql(&sql)
        .await
        .with_context(|| format!("failed to create query for {}", key.label()))?;
    let mut stream = df
        .execute_stream()
        .await
        .with_context(|| format!("failed to execute query for {}", key.label()))?;

    let mut dt_writers = vec![
        PartitionWriter::new(outputs.partition.root, outputs.partition.props),
        PartitionWriter::new(outputs.bloom.root, outputs.bloom.props),
    ];

    let staging_props = Arc::new(writer_properties(false, &[], args.row_group_size, args.compression, args.data_page_size, args.bloom_fpp, args.bloom_ndv, &HashMap::new()));
    let max_rows_per_staging_file = args.row_group_size * 10;

    let mut progress = Progress::new(format!("all-outputs {}", key.label()));

    let mut hilbert_batches: Vec<RecordBatch> = Vec::new();
    let mut staging_file_number = 0usize;
    let mut staging_writer: Option<ArrowWriter<File>> = None;
    let mut staging_rows_in_file = 0usize;
    let mut staging_dir_created = false;

    while let Some(batch) = stream.next().await {
        let batch = batch.with_context(|| format!("failed to read batch for {}", key.label()))?;
        let rows = batch.num_rows();

        write_partitioned_batch(&batch, &mut dt_writers, false)?;

        let hilbert_batch = append_hilbert_index(&batch, bounds)?;

        if args.in_memory {
            hilbert_batches.push(hilbert_batch);
        } else {
            if !staging_dir_created {
                let staging_dir = args.hilbert_staging_dir.join(format!(
                    "year={}/month={}/day={}",
                    key.year, key.month, key.day
                ));
                if staging_dir.exists() {
                    fs::remove_dir_all(&staging_dir)?;
                }
                fs::create_dir_all(&staging_dir)?;
                staging_dir_created = true;
            }
            if staging_writer.is_none() || staging_rows_in_file >= max_rows_per_staging_file {
                if let Some(w) = staging_writer.take() {
                    w.close()
                        .context("failed to close Hilbert staging writer")?;
                }
                let staging_dir = args.hilbert_staging_dir.join(format!(
                    "year={}/month={}/day={}",
                    key.year, key.month, key.day
                ));
                let file =
                    File::create(staging_dir.join(format!("part-{staging_file_number:05}.parquet")))
                        .context("failed to create Hilbert staging file")?;
                staging_writer = Some(
                    ArrowWriter::try_new(
                        file,
                        hilbert_batch.schema(),
                        Some((*staging_props).clone()),
                    )
                    .context("failed to create Hilbert staging writer")?,
                );
                staging_rows_in_file = 0;
                staging_file_number += 1;
            }
            staging_writer
                .as_mut()
                .expect("staging writer initialized")
                .write(&hilbert_batch)
                .context("failed to write Hilbert staging batch")?;
            staging_rows_in_file += rows;
        }

        progress.record(rows)?;
    }
    progress.finish()?;

    for w in &mut dt_writers {
        w.close()?;
    }

    if args.in_memory {
        if hilbert_batches.is_empty() {
            return Ok(());
        }
        let schema = hilbert_batches[0].schema();
        let table_name = format!("{STAGING_TABLE}_{}_{}_{}", key.year, key.month, key.day);
        let mem_table =
            MemTable::try_new(schema, vec![hilbert_batches]).context("failed to create MemTable")?;
        ctx.register_table(&table_name, Arc::new(mem_table))
            .context("failed to register MemTable")?;

        let sort_sql = format!("SELECT * FROM {table_name} ORDER BY {HILBERT_COLUMN}");
        let sort_df = ctx
            .sql(&sort_sql)
            .await
            .with_context(|| format!("failed to create Hilbert sorted query for {}", key.label()))?;
        let mut sort_stream = sort_df
            .execute_stream()
            .await
            .with_context(|| format!("failed to execute Hilbert sorted query for {}", key.label()))?;

        let mut hilbert_writers = vec![
            PartitionWriter::new(outputs.hilbert.root, outputs.hilbert.props),
            PartitionWriter::new(outputs.hilbert_bloom.root, outputs.hilbert_bloom.props),
        ];

        let mut hilbert_progress = Progress::new(format!("Hilbert sort/write {}", key.label()));
        while let Some(batch) = sort_stream.next().await {
            let batch = batch.with_context(|| {
                format!("failed to read Hilbert sorted batch for {}", key.label())
            })?;
            let rows = batch.num_rows();
            write_partitioned_batch(&batch, &mut hilbert_writers, true)?;
            hilbert_progress.record(rows)?;
        }
        hilbert_progress.finish()?;

        for w in &mut hilbert_writers {
            w.close()?;
        }
    } else {
        if let Some(w) = staging_writer.take() {
            w.close()
                .context("failed to close final Hilbert staging writer")?;
        }

        let staging_dir = args.hilbert_staging_dir.join(format!(
            "year={}/month={}/day={}",
            key.year, key.month, key.day
        ));
        let _cleanup = StagingCleanup::new(staging_dir.clone());
        std::mem::forget(_cleanup); // Keep staging dir alive until stream is consumed; cleaned up on next run

        let staging_glob = staging_dir.join("*.parquet");
        let mut sort_stream = ctx
            .read_parquet(
                staging_glob.to_str().ok_or_else(|| anyhow!("glob path UTF-8"))?,
                ParquetReadOptions::default(),
            )
            .await
            .with_context(|| format!("failed to read staging parquet for {}", key.label()))?
            .sort(vec![col(HILBERT_COLUMN).sort(true, true)])
            .with_context(|| format!("failed to sort staging data for {}", key.label()))?
            .execute_stream()
            .await
            .with_context(|| format!("failed to execute sorted staging for {}", key.label()))?;

        let mut hilbert_writers = vec![
            PartitionWriter::new(outputs.hilbert.root, outputs.hilbert.props),
            PartitionWriter::new(outputs.hilbert_bloom.root, outputs.hilbert_bloom.props),
        ];

        let mut hilbert_progress = Progress::new(format!("Hilbert sort/write {}", key.label()));
        while let Some(batch) = sort_stream.next().await {
            let batch = batch.with_context(|| {
                format!("failed to read Hilbert sorted batch for {}", key.label())
            })?;
            let rows = batch.num_rows();
            write_partitioned_batch(&batch, &mut hilbert_writers, true)?;
            hilbert_progress.record(rows)?;
        }
        hilbert_progress.finish()?;

        for w in &mut hilbert_writers {
            w.close()?;
        }
    }

    Ok(())
}

fn partition_select_sql(key: PartitionKey, order_column: &str) -> String {
    let (prev_year, prev_month, prev_day) = previous_day(key.year, key.month, key.day);
    let (next_year, next_month, next_day_value) = next_day(key.year, key.month, key.day);
    let (after_next_year, after_next_month, after_next_day) =
        next_day(next_year, next_month, next_day_value);
    format!(
        "SELECT *, \
            CAST(EXTRACT(YEAR FROM \"BaseDateTime\") AS INT) AS year, \
            CAST(EXTRACT(MONTH FROM \"BaseDateTime\") AS INT) AS month, \
            CAST(EXTRACT(DAY FROM \"BaseDateTime\") AS INT) AS day \
         FROM {SOURCE_TABLE} \
         WHERE \"BaseDateTime\" >= '{}' \
           AND \"BaseDateTime\" < '{}' \
           AND CAST(EXTRACT(YEAR FROM \"BaseDateTime\") AS INT) = {} \
           AND CAST(EXTRACT(MONTH FROM \"BaseDateTime\") AS INT) = {} \
           AND CAST(EXTRACT(DAY FROM \"BaseDateTime\") AS INT) = {} \
         ORDER BY {order_column}",
        date_literal(prev_year, prev_month, prev_day),
        date_literal(after_next_year, after_next_month, after_next_day),
        key.year,
        key.month,
        key.day
    )
}

fn date_literal(year: i32, month: i32, day: i32) -> String {
    format!("{year:04}-{month:02}-{day:02}T00:00:00Z")
}

fn next_day(year: i32, month: i32, day: i32) -> (i32, i32, i32) {
    let days = days_in_month(year, month);
    if day < days {
        (year, month, day + 1)
    } else if month < 12 {
        (year, month + 1, 1)
    } else {
        (year + 1, 1, 1)
    }
}

fn previous_day(year: i32, month: i32, day: i32) -> (i32, i32, i32) {
    if day > 1 {
        (year, month, day - 1)
    } else if month > 1 {
        let previous_month = month - 1;
        (year, previous_month, days_in_month(year, previous_month))
    } else {
        (year - 1, 12, 31)
    }
}

fn days_in_month(year: i32, month: i32) -> i32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 31,
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

async fn read_partition_keys(ctx: &SessionContext) -> Result<Vec<PartitionKey>> {
    let df = ctx
        .sql(&format!(
            "SELECT \
                CAST(EXTRACT(YEAR FROM \"BaseDateTime\") AS INT) AS year, \
                CAST(EXTRACT(MONTH FROM \"BaseDateTime\") AS INT) AS month, \
                CAST(EXTRACT(DAY FROM \"BaseDateTime\") AS INT) AS day \
             FROM {SOURCE_TABLE} \
             GROUP BY year, month, day \
             ORDER BY year, month, day"
        ))
        .await
        .context("failed to create partition discovery query")?;
    let batches = df
        .collect()
        .await
        .context("failed to collect partition keys")?;

    let mut keys = Vec::new();
    for batch in batches {
        let years = as_int32_array(
            batch.column(column_index(batch.schema_ref(), "year")?),
            "year",
        )?;
        let months = as_int32_array(
            batch.column(column_index(batch.schema_ref(), "month")?),
            "month",
        )?;
        let days = as_int32_array(
            batch.column(column_index(batch.schema_ref(), "day")?),
            "day",
        )?;
        for row in 0..batch.num_rows() {
            keys.push(PartitionKey {
                year: years.value(row),
                month: months.value(row),
                day: days.value(row),
            });
        }
    }

    Ok(keys)
}

async fn read_hilbert_bounds(ctx: &SessionContext) -> Result<HilbertBounds> {
    let df = ctx
        .sql(&format!(
            "SELECT \
                MIN(\"LAT\") AS min_lat, \
                MAX(\"LAT\") AS max_lat, \
                MIN(\"LON\") AS min_lon, \
                MAX(\"LON\") AS max_lon \
             FROM {SOURCE_TABLE}"
        ))
        .await
        .context("failed to create Hilbert bounds query")?;
    let batches = df
        .collect()
        .await
        .context("failed to collect Hilbert bounds")?;
    let batch = batches
        .first()
        .ok_or_else(|| anyhow!("bounds query returned no batches"))?;

    Ok(HilbertBounds {
        min_lat: scalar_f64(batch, "min_lat")?,
        max_lat: scalar_f64(batch, "max_lat")?,
        min_lon: scalar_f64(batch, "min_lon")?,
        max_lon: scalar_f64(batch, "max_lon")?,
    })
}

async fn write_hilbert_staging(
    ctx: &SessionContext,
    bounds: HilbertBounds,
    key: PartitionKey,
    staging_dir: &Path,
) -> Result<()> {
    let label = key.label();
    let staging_table_name = format!("{STAGING_TABLE}_{}_{}_{}_tmp", key.year, key.month, key.day);
    let sql = partition_select_sql(key, "\"BaseDateTime\"");
    let df = ctx
        .sql(&sql)
        .await
        .with_context(|| format!("failed to create Hilbert staging query for {label}"))?;
    let mut stream = df.execute_stream().await.with_context(|| {
        format!("failed to execute Hilbert staging query for {label}")
    })?;

    let mut progress = Progress::new(format!("Hilbert staging {label}"));
    let mut collected: Vec<RecordBatch> = Vec::new();

    while let Some(batch) = stream.next().await {
        let batch = batch
            .with_context(|| format!("failed to read Hilbert staging record batch for {label}"))?;
        let batch = append_hilbert_index(&batch, bounds)?;
        progress.record(batch.num_rows())?;
        collected.push(batch);
    }
    progress.finish()?;

    if collected.is_empty() {
        return Ok(());
    }

    let schema = collected[0].schema();
    let mem_table = MemTable::try_new(schema, vec![collected])
        .context("failed to create staging MemTable")?;
    ctx.register_table(&staging_table_name, Arc::new(mem_table))
        .context("failed to register staging MemTable")?;

    let write_df = ctx
        .sql(&format!("SELECT * FROM {staging_table_name}"))
        .await
        .context("failed to create staging write query")?;

    let write_path = staging_dir
        .to_str()
        .ok_or_else(|| anyhow!("staging path is not valid UTF-8"))?;
    ctx.write_parquet(
        write_df.create_physical_plan().await?,
        write_path,
        None::<WriterProperties>,
    )
    .await
    .context("failed to write Hilbert staging parquet")?;

    ctx.deregister_table(&staging_table_name)?;
    Ok(())
}

fn writer_properties(
    bloom: bool,
    bloom_columns: &[String],
    row_group_size: usize,
    compression_level: i32,
    data_page_size: usize,
    bloom_fpp: f64,
    bloom_ndv: u64,
    bloom_ndv_per_column: &HashMap<String, u64>,
) -> WriterProperties {
    let zstd_level = ZstdLevel::try_new(compression_level).unwrap_or_default();
    let mut builder = WriterProperties::builder()
        .set_compression(Compression::ZSTD(zstd_level))
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_max_row_group_size(row_group_size)
        .set_key_value_metadata(Some(vec![KeyValue {
            key: "created_by".to_string(),
            value: Some("ais-parquet-optimizer".to_string()),
        }]));

    if data_page_size > 0 {
        builder = builder.set_data_page_size_limit(data_page_size);
    }

    if bloom {
        builder = builder.set_bloom_filter_position(BloomFilterPosition::End);
        for column in bloom_columns {
            let ndv = bloom_ndv_per_column.get(column.as_str()).copied().unwrap_or(bloom_ndv);
            builder = builder
                .set_column_bloom_filter_enabled(ColumnPath::from(column.as_str()), true)
                .set_column_bloom_filter_fpp(ColumnPath::from(column.as_str()), bloom_fpp)
                .set_column_bloom_filter_ndv(ColumnPath::from(column.as_str()), ndv);
        }
    }

    builder.build()
}

fn writer_properties_for_source(
    row_group_size: usize,
    compression_level: i32,
    data_page_size: usize,
) -> WriterProperties {
    let zstd_level = ZstdLevel::try_new(compression_level).unwrap_or_default();
    let mut builder = WriterProperties::builder()
        .set_compression(Compression::ZSTD(zstd_level))
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_max_row_group_size(row_group_size)
        .set_key_value_metadata(Some(vec![KeyValue {
            key: "created_by".to_string(),
            value: Some("ais-parquet-optimizer".to_string()),
        }]));

    if data_page_size > 0 {
        builder = builder.set_data_page_size_limit(data_page_size);
    }

    builder.build()
}

fn write_partitioned_batch(
    batch: &RecordBatch,
    writers: &mut [PartitionWriter],
    keep_hilbert_column: bool,
) -> Result<()> {
    if batch.num_rows() == 0 {
        return Ok(());
    }

    let year_idx = column_index(batch.schema_ref(), "year")?;
    let month_idx = column_index(batch.schema_ref(), "month")?;
    let day_idx = column_index(batch.schema_ref(), "day")?;

    let years = as_int32_array(batch.column(year_idx), "year")?;
    let months = as_int32_array(batch.column(month_idx), "month")?;
    let days = as_int32_array(batch.column(day_idx), "day")?;

    let mut offset = 0usize;
    while offset < batch.num_rows() {
        let key = PartitionKey {
            year: years.value(offset),
            month: months.value(offset),
            day: days.value(offset),
        };
        let mut end = offset + 1;
        while end < batch.num_rows()
            && years.value(end) == key.year
            && months.value(end) == key.month
            && days.value(end) == key.day
        {
            end += 1;
        }

        let partition_batch =
            strip_partition_columns(&batch.slice(offset, end - offset), keep_hilbert_column)?;
        for writer in writers.iter_mut() {
            writer.write(key, &partition_batch)?;
        }

        offset = end;
    }

    Ok(())
}

impl PartitionWriter {
    fn new(root: PathBuf, props: Arc<WriterProperties>) -> Self {
        Self {
            root,
            props,
            active: None,
        }
    }

    fn write(&mut self, key: PartitionKey, batch: &RecordBatch) -> Result<()> {
        if self.active.as_ref().map(|active| active.key) != Some(key) {
            self.close()?;
            self.active = Some(self.open(key, batch.schema())?);
        }

        self.active
            .as_mut()
            .expect("active writer exists")
            .writer
            .write(batch)
            .with_context(|| format!("failed to write partition {:?}", key))?;
        Ok(())
    }

    fn open(&self, key: PartitionKey, schema: SchemaRef) -> Result<ActivePartitionWriter> {
        let dir = self
            .root
            .join(format!("year={}", key.year))
            .join(format!("month={}", key.month))
            .join(format!("day={}", key.day));
        fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create partition dir {}", dir.display()))?;
        let path = dir.join("part-00000.parquet");
        let file = File::create(&path)
            .with_context(|| format!("failed to create parquet file {}", path.display()))?;
        let writer = ArrowWriter::try_new(file, schema, Some((*self.props).clone()))
            .with_context(|| format!("failed to create writer for {}", path.display()))?;
        Ok(ActivePartitionWriter { key, writer })
    }

    fn close(&mut self) -> Result<()> {
        if let Some(active) = self.active.take() {
            active
                .writer
                .close()
                .with_context(|| format!("failed to close partition {:?}", active.key))?;
        }
        Ok(())
    }
}

fn strip_partition_columns(batch: &RecordBatch, keep_hilbert_column: bool) -> Result<RecordBatch> {
    let excluded = PARTITION_COLUMNS.iter().copied().collect::<HashSet<_>>();
    let mut fields = Vec::new();
    let mut columns = Vec::new();

    for (idx, field) in batch.schema().fields().iter().enumerate() {
        let name = field.name().as_str();
        if excluded.contains(name) {
            continue;
        }
        if !keep_hilbert_column && name == HILBERT_COLUMN {
            continue;
        }
        fields.push(Field::new(
            field.name(),
            field.data_type().clone(),
            field.is_nullable(),
        ));
        columns.push(batch.column(idx).clone());
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .context("failed to strip partition columns")
}

const HILBERT_BITS: u32 = 32;

fn append_hilbert_index(batch: &RecordBatch, bounds: HilbertBounds) -> Result<RecordBatch> {
    let lat_idx = column_index(batch.schema_ref(), "LAT")?;
    let lon_idx = column_index(batch.schema_ref(), "LON")?;

    let lat_values = read_f64_values(batch.column(lat_idx), "LAT")?;
    let lon_values = read_f64_values(batch.column(lon_idx), "LON")?;

    let mut values = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let lat = lat_values[row].unwrap_or(bounds.min_lat);
        let lon = lon_values[row].unwrap_or(bounds.min_lon);

        let point = [
            scale_f64(lat, bounds.min_lat, bounds.max_lat),
            scale_f64(lon, bounds.min_lon, bounds.max_lon),
        ];
        values.push(hilbert_index_2d(point));
    }

    let mut fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| Field::new(field.name(), field.data_type().clone(), field.is_nullable()))
        .collect::<Vec<_>>();
    fields.push(Field::new(HILBERT_COLUMN, DataType::UInt64, false));

    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(arrow::array::UInt64Array::from(values)) as Arc<dyn Array>);

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .context("failed to append Hilbert index")
}

fn scale_f64(value: f64, min: f64, max: f64) -> u32 {
    if !value.is_finite() || max <= min {
        return 0;
    }
    (((value - min) / (max - min)).clamp(0.0, 1.0) * ((1u64 << HILBERT_BITS) - 1) as f64).round()
        as u32
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

fn column_index(schema: &SchemaRef, name: &str) -> Result<usize> {
    schema
        .fields()
        .iter()
        .position(|field| field.name() == name)
        .ok_or_else(|| anyhow!("column not found: {name}"))
}

fn as_int32_array<'a>(array: &'a Arc<dyn Array>, name: &str) -> Result<&'a Int32Array> {
    array
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| anyhow!("column {name} is not Int32"))
}

fn read_f64_values(array: &Arc<dyn Array>, name: &str) -> Result<Vec<Option<f64>>> {
    match array.data_type() {
        DataType::Float64 => {
            let values = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow!("{name} Float64 downcast failed"))?;
            Ok((0..array.len())
                .map(|i| {
                    if values.is_null(i) {
                        None
                    } else {
                        Some(values.value(i))
                    }
                })
                .collect())
        }
        DataType::Float32 => {
            let values = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| anyhow!("{name} Float32 downcast failed"))?;
            Ok((0..array.len())
                .map(|i| {
                    if values.is_null(i) {
                        None
                    } else {
                        Some(values.value(i) as f64)
                    }
                })
                .collect())
        }
        other => bail!("column {name} is not Float32 or Float64: {other:?}"),
    }
}

fn scalar_f64(batch: &RecordBatch, name: &str) -> Result<f64> {
    let idx = column_index(batch.schema_ref(), name)?;
    let array = cast(batch.column(idx), &DataType::Float64)
        .with_context(|| format!("failed to cast {name} to Float64"))?;
    let values = array
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| anyhow!("{name} is not Float64"))?;
    Ok(values.value(0))
}
