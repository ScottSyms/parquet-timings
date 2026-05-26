use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use arrow::array::*;
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use clap::Parser;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::*;
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

const SOURCE_TABLE: &str = "ais";
const PROGRESS_INTERVAL_SECS: u64 = 5;
const FLUSH_GROUPS: usize = 50000;

#[derive(Debug, Parser)]
#[command(name = "ships", about = "Build ship identities parquet from AIS data")]
struct Args {
    #[arg(long, default_value = "parquet/ais_2024.parquet")]
    source: PathBuf,

    #[arg(long, default_value = "ships")]
    ships_dir: PathBuf,

    #[arg(long, default_value = ".datafusion-spill")]
    spill_dir: PathBuf,

    #[arg(long, default_value_t = 262144)]
    batch_size: usize,

    #[arg(long, default_value_t = 1_000_000)]
    row_group_size: usize,

    #[arg(long, default_value_t = 6)]
    compression: i32,

    #[arg(long)]
    overwrite: bool,
}

struct IdentityGroup {
    mmsi: Option<i64>,
    imo: Option<i64>,
    call_sign: Option<String>,
    vessel_name: Option<String>,
    transceiver_class: Option<bool>,
    draft: Option<f32>,
    length: Option<f32>,
    width: Option<f32>,
    vessel_type: Option<String>,
    last_known_date: Option<i64>,
    last_lat: Option<f32>,
    last_lon: Option<f32>,
    positions_base_dt: Vec<Option<i64>>,
    positions_lat: Vec<Option<f32>>,
    positions_lon: Vec<Option<f32>>,
    positions_sog: Vec<Option<f32>>,
    positions_cog: Vec<Option<f32>>,
    positions_heading: Vec<Option<f32>>,
}

impl IdentityGroup {
    fn new(mmsi: Option<i64>, imo: Option<i64>, call_sign: Option<String>, vessel_name: Option<String>) -> Self {
        Self {
            mmsi,
            imo,
            call_sign,
            vessel_name,
            transceiver_class: None,
            draft: None,
            length: None,
            width: None,
            vessel_type: None,
            last_known_date: None,
            last_lat: None,
            last_lon: None,
            positions_base_dt: Vec::new(),
            positions_lat: Vec::new(),
            positions_lon: Vec::new(),
            positions_sog: Vec::new(),
            positions_cog: Vec::new(),
            positions_heading: Vec::new(),
        }
    }

    fn add_position(&mut self, base_dt: Option<i64>, lat: Option<f32>, lon: Option<f32>, sog: Option<f32>, cog: Option<f32>, heading: Option<f32>) {
        self.positions_base_dt.push(base_dt);
        self.positions_lat.push(lat);
        self.positions_lon.push(lon);
        self.positions_sog.push(sog);
        self.positions_cog.push(cog);
        self.positions_heading.push(heading);
    }
}

struct ShipsWriter {
    root: PathBuf,
    props: Arc<WriterProperties>,
    writers: HashMap<i32, ArrowWriter<File>>,
}

impl ShipsWriter {
    fn new(root: PathBuf, props: WriterProperties) -> Self {
        Self { root, props: Arc::new(props), writers: HashMap::new() }
    }

    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let prefix_idx = batch
            .schema()
            .index_of("mmsi_prefix")
            .map_err(|_| anyhow!("mmsi_prefix column not found in batch"))?;
        let prefixes = batch
            .column(prefix_idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| anyhow!("mmsi_prefix is not Int32"))?;

        let num_rows = batch.num_rows();
        let mut start = 0usize;
        while start < num_rows {
            let prefix = if prefixes.is_null(start) { -1 } else { prefixes.value(start) };
            let mut end = start + 1;
            while end < num_rows {
                let p = if prefixes.is_null(end) { -1 } else { prefixes.value(end) };
                if p != prefix { break; }
                end += 1;
            }

            let sliced = batch.slice(start, end - start);

            if !self.writers.contains_key(&prefix) {
                let dir = if prefix >= 0 {
                    self.root.join(format!("mmsi_prefix={}", prefix))
                } else {
                    self.root.join("mmsi_prefix=null")
                };
                fs::create_dir_all(&dir)
                    .with_context(|| format!("failed to create {}", dir.display()))?;
                let file = File::create(dir.join("data.parquet"))
                    .with_context(|| format!("failed to create file for prefix={}", prefix))?;
                let writer = ArrowWriter::try_new(file, sliced.schema(), Some((*self.props).clone()))
                    .with_context(|| format!("failed to create writer for prefix={}", prefix))?;
                self.writers.insert(prefix, writer);
            }

            self.writers
                .get_mut(&prefix)
                .expect("writer exists")
                .write(&sliced)
                .with_context(|| format!("failed to write slice for prefix={}", prefix))?;

            start = end;
        }

        Ok(())
    }

    fn close_all(&mut self) -> Result<()> {
        let writers: Vec<(i32, ArrowWriter<File>)> = self.writers.drain().collect();
        for (prefix, writer) in writers {
            writer.close().with_context(|| format!("failed to close writer for prefix={}", prefix))?;
        }
        Ok(())
    }
}

impl Drop for ShipsWriter {
    fn drop(&mut self) {
        for (_, writer) in self.writers.drain() {
            let _ = writer.close();
        }
    }
}

fn status(message: impl AsRef<str>) -> Result<()> {
    println!("[ships] {}", message.as_ref());
    io::stdout().flush().ok();
    Ok(())
}

fn output_schema() -> &'static SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        Arc::new(Schema::new(vec![
            Field::new("MMSI", DataType::Int64, true),
            Field::new("IMO", DataType::Int64, true),
            Field::new("CallSign", DataType::Utf8, true),
            Field::new("VesselName", DataType::Utf8, true),
            Field::new("TransceiverClass", DataType::Boolean, true),
            Field::new("Draft", DataType::Float32, true),
            Field::new("Length", DataType::Float32, true),
            Field::new("Width", DataType::Float32, true),
            Field::new("VesselType", DataType::Utf8, true),
            Field::new("LastKnownDate", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("LastKnownLAT", DataType::Float32, true),
            Field::new("LastKnownLON", DataType::Float32, true),
            Field::new("mmsi_prefix", DataType::Int32, true),
            Field::new(
                "Positions",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(Fields::from(vec![
                        Field::new("BaseDateTime", DataType::Timestamp(TimeUnit::Microsecond, None), true),
                        Field::new("LAT", DataType::Float32, true),
                        Field::new("LON", DataType::Float32, true),
                        Field::new("SOG", DataType::Float32, true),
                        Field::new("COG", DataType::Float32, true),
                        Field::new("Heading", DataType::Float32, true),
                    ])),
                    true,
                ))),
                true,
            ),
        ]))
    })
}

fn mmsi_prefix(mmsi: Option<i64>) -> Option<i32> {
    mmsi.and_then(|m| {
        let s = m.to_string();
        s.as_bytes().first().and_then(|&c| (c as char).to_digit(10).map(|d| d as i32))
    })
}

fn flush_batch(groups: &[IdentityGroup]) -> Result<RecordBatch> {
    let n = groups.len();

    let mut col_mmsi: Vec<Option<i64>> = Vec::with_capacity(n);
    let mut col_imo: Vec<Option<i64>> = Vec::with_capacity(n);
    let mut col_call_sign: Vec<Option<String>> = Vec::with_capacity(n);
    let mut col_vessel_name: Vec<Option<String>> = Vec::with_capacity(n);
    let mut col_transceiver: Vec<Option<bool>> = Vec::with_capacity(n);
    let mut col_draft: Vec<Option<f32>> = Vec::with_capacity(n);
    let mut col_length: Vec<Option<f32>> = Vec::with_capacity(n);
    let mut col_width: Vec<Option<f32>> = Vec::with_capacity(n);
    let mut col_vessel_type: Vec<Option<String>> = Vec::with_capacity(n);
    let mut col_last_known: Vec<Option<i64>> = Vec::with_capacity(n);
    let mut col_last_lat: Vec<Option<f32>> = Vec::with_capacity(n);
    let mut col_last_lon: Vec<Option<f32>> = Vec::with_capacity(n);
    let mut col_prefix: Vec<Option<i32>> = Vec::with_capacity(n);

    let mut positions_dt: Vec<Option<i64>> = Vec::new();
    let mut positions_lat: Vec<Option<f32>> = Vec::new();
    let mut positions_lon: Vec<Option<f32>> = Vec::new();
    let mut positions_sog: Vec<Option<f32>> = Vec::new();
    let mut positions_cog: Vec<Option<f32>> = Vec::new();
    let mut positions_heading: Vec<Option<f32>> = Vec::new();
    let mut offsets: Vec<i32> = Vec::with_capacity(n + 1);
    offsets.push(0);

    for g in groups {
        col_mmsi.push(g.mmsi);
        col_imo.push(g.imo);
        col_call_sign.push(g.call_sign.clone());
        col_vessel_name.push(g.vessel_name.clone());
        col_transceiver.push(g.transceiver_class);
        col_draft.push(g.draft);
        col_length.push(g.length);
        col_width.push(g.width);
        col_vessel_type.push(g.vessel_type.clone());
        col_last_known.push(g.last_known_date);
        col_last_lat.push(g.last_lat);
        col_last_lon.push(g.last_lon);
        col_prefix.push(mmsi_prefix(g.mmsi));

        positions_dt.extend(g.positions_base_dt.iter().copied());
        positions_lat.extend(g.positions_lat.iter().copied());
        positions_lon.extend(g.positions_lon.iter().copied());
        positions_sog.extend(g.positions_sog.iter().copied());
        positions_cog.extend(g.positions_cog.iter().copied());
        positions_heading.extend(g.positions_heading.iter().copied());
        offsets.push(positions_dt.len() as i32);
    }

    let item_field = match output_schema().field(13).data_type() {
        DataType::List(f) => f,
        _ => unreachable!(),
    };
    let struct_type = item_field.data_type();
    let struct_fields = match struct_type {
        DataType::Struct(fields) => fields.clone(),
        _ => unreachable!(),
    };

    let struct_arr = StructArray::new(
        struct_fields,
        vec![
            Arc::new(TimestampMicrosecondArray::from(positions_dt)) as ArrayRef,
            Arc::new(Float32Array::from(positions_lat)) as ArrayRef,
            Arc::new(Float32Array::from(positions_lon)) as ArrayRef,
            Arc::new(Float32Array::from(positions_sog)) as ArrayRef,
            Arc::new(Float32Array::from(positions_cog)) as ArrayRef,
            Arc::new(Float32Array::from(positions_heading)) as ArrayRef,
        ],
        None,
    );

    let num_groups = groups.len();
    let list_arr = ListArray::new(
        item_field.clone(),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Arc::new(struct_arr),
        Some(NullBuffer::new_valid(num_groups)),
    );

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(col_mmsi)),
        Arc::new(Int64Array::from(col_imo)),
        Arc::new(StringArray::from_iter(col_call_sign.iter().map(|s| s.as_deref()))),
        Arc::new(StringArray::from_iter(col_vessel_name.iter().map(|s| s.as_deref()))),
        Arc::new(BooleanArray::from(col_transceiver)),
        Arc::new(Float32Array::from(col_draft)),
        Arc::new(Float32Array::from(col_length)),
        Arc::new(Float32Array::from(col_width)),
        Arc::new(StringArray::from_iter(col_vessel_type.iter().map(|s| s.as_deref()))),
        Arc::new(TimestampMicrosecondArray::from(col_last_known)),
        Arc::new(Float32Array::from(col_last_lat)),
        Arc::new(Float32Array::from(col_last_lon)),
        Arc::new(Int32Array::from(col_prefix)),
        Arc::new(list_arr),
    ];

    RecordBatch::try_new((*output_schema()).clone(), columns).context("failed to create output batch")
}

fn as_i64(arr: &dyn Array, row: usize) -> Option<i64> {
    let a = arr.as_any().downcast_ref::<Int64Array>()?;
    if a.is_null(row) { None } else { Some(a.value(row)) }
}

fn as_f32(arr: &dyn Array, row: usize) -> Option<f32> {
    let a = arr.as_any().downcast_ref::<Float32Array>()?;
    if a.is_null(row) { None } else { Some(a.value(row)) }
}

fn as_timestamp(arr: &dyn Array, row: usize) -> Option<i64> {
    let a = arr.as_any().downcast_ref::<TimestampMicrosecondArray>()?;
    if a.is_null(row) { None } else { Some(a.value(row)) }
}

fn as_bool(arr: &dyn Array, row: usize) -> Option<bool> {
    let a = arr.as_any().downcast_ref::<BooleanArray>()?;
    if a.is_null(row) { None } else { Some(a.value(row)) }
}

fn as_string(arr: &dyn Array, row: usize) -> Option<&str> {
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        if a.is_null(row) { None } else { Some(a.value(row)) }
    } else if let Some(a) = arr.as_any().downcast_ref::<LargeStringArray>() {
        if a.is_null(row) { None } else { Some(a.value(row)) }
    } else if let Some(a) = arr.as_any().downcast_ref::<StringViewArray>() {
        if a.is_null(row) { None } else { Some(a.value(row)) }
    } else {
        None
    }
}

async fn run_pipeline(ctx: &SessionContext, writer: &mut ShipsWriter, source: &str) -> Result<usize> {
    let df = ctx
        .sql(&format!(
            "SELECT * FROM {source} ORDER BY \"MMSI\", \"IMO\", \"CallSign\", \"VesselName\", \"BaseDateTime\""
        ))
        .await
        .context("failed to create sorted query")?;

    let mut stream = df.execute_stream().await.context("failed to execute sorted query")?;
    let started = Instant::now();
    let mut total_groups = 0usize;
    let mut total_input_rows = 0usize;
    let mut last_report = Instant::now();

    let mut current: Option<IdentityGroup> = None;
    let mut pending: Vec<IdentityGroup> = Vec::with_capacity(FLUSH_GROUPS);

    while let Some(batch) = stream.next().await {
        let batch = batch.context("failed to read batch")?;
        let num_rows = batch.num_rows();

        let col_mmsi = batch.column(0);
        let col_base_dt = batch.column(1);
        let col_lat = batch.column(2);
        let col_lon = batch.column(3);
        let col_sog = batch.column(4);
        let col_cog = batch.column(5);
        let col_heading = batch.column(6);
        let col_vessel_name = batch.column(7);
        let col_imo = batch.column(8);
        let col_call_sign = batch.column(9);
        let col_vessel_type = batch.column(10);
        let col_length = batch.column(12);
        let col_width = batch.column(13);
        let col_draft = batch.column(14);
        let col_transceiver = batch.column(16);

        for i in 0..num_rows {
            let mmsi = as_i64(col_mmsi, i);
            let imo = as_i64(col_imo, i);
            let call_sign = as_string(col_call_sign, i).map(String::from);
            let vessel_name = as_string(col_vessel_name, i).map(String::from);

            let is_new = match current.as_ref() {
                Some(g) => {
                    mmsi != g.mmsi
                        || imo != g.imo
                        || call_sign.as_deref() != g.call_sign.as_deref()
                        || vessel_name.as_deref() != g.vessel_name.as_deref()
                }
                None => true,
            };

            if is_new {
                if let Some(group) = current.take() {
                    pending.push(group);
                    if pending.len() >= FLUSH_GROUPS {
                        let batch = flush_batch(&pending)?;
                        writer.write_batch(&batch)?;
                        total_groups += pending.len();
                        pending.clear();
                    }
                }
                current = Some(IdentityGroup::new(mmsi, imo, call_sign, vessel_name));
            }

            if let Some(ref mut g) = current {
                let base_dt = as_timestamp(col_base_dt, i);
                let lat = as_f32(col_lat, i);
                let lon = as_f32(col_lon, i);
                let sog = as_f32(col_sog, i);
                let cog = as_f32(col_cog, i);
                let heading = as_f32(col_heading, i);

                g.add_position(base_dt, lat, lon, sog, cog, heading);
                g.last_known_date = base_dt.or(g.last_known_date);

                if let Some(v) = as_bool(col_transceiver, i) { g.transceiver_class = Some(v); }
                if let Some(v) = as_f32(col_draft, i) { g.draft = Some(v); }
                if let Some(v) = as_f32(col_length, i) { g.length = Some(v); }
                if let Some(v) = as_f32(col_width, i) { g.width = Some(v); }
                if let Some(v) = as_string(col_vessel_type, i) { g.vessel_type = Some(v.to_string()); }
                if let Some(v) = lat { g.last_lat = Some(v); }
                if let Some(v) = lon { g.last_lon = Some(v); }
            }

            total_input_rows += 1;
        }

        if last_report.elapsed().as_secs() >= PROGRESS_INTERVAL_SECS {
            let elapsed = started.elapsed().as_secs_f64();
            let rate = if elapsed > 0.0 { total_input_rows as f64 / elapsed } else { 0.0 };
            status(format!(
                "sorted {:.0}M rows, {} groups so far ({:.0} rows/s)",
                total_input_rows as f64 / 1_000_000.0,
                total_groups + pending.len(),
                rate,
            ))?;
            last_report = Instant::now();
        }
    }

    // Flush final group
    if let Some(group) = current.take() {
        pending.push(group);
    }
    if !pending.is_empty() {
        let batch = flush_batch(&pending)?;
        writer.write_batch(&batch)?;
        total_groups += pending.len();
    }

    let elapsed = started.elapsed().as_secs_f64();
    let rate = if elapsed > 0.0 { total_input_rows as f64 / elapsed } else { 0.0 };
    status(format!(
        "done sorting: {:.0}M rows processed, {} identities found ({:.0} rows/s, {:.1}s)",
        total_input_rows as f64 / 1_000_000.0,
        total_groups,
        rate,
        elapsed,
    ))?;

    Ok(total_groups)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    status(format!(
        "source={}, ships_dir={}, row_group_size={}, batch_size={}",
        args.source.display(), args.ships_dir.display(), args.row_group_size, args.batch_size,
    ))?;

    if args.overwrite && args.ships_dir.exists() {
        fs::remove_dir_all(&args.ships_dir)
            .with_context(|| format!("failed to remove {}", args.ships_dir.display()))?;
    }

    fs::create_dir_all(&args.spill_dir)
        .with_context(|| format!("failed to create {}", args.spill_dir.display()))?;

    let mut config = SessionConfig::new()
        .with_target_partitions(num_cpus::get() * 2)
        .with_batch_size(args.batch_size)
        .with_parquet_pruning(true);

    config.options_mut().execution.sort_spill_reservation_bytes = usize::MAX;
    config.options_mut().execution.sort_in_place_threshold_bytes = usize::MAX;


    let runtime = Arc::new(
        RuntimeEnvBuilder::new()
            .with_temp_file_path(&args.spill_dir)
            .build()
            .context("failed to build DataFusion runtime")?,
    );
    let ctx = SessionContext::new_with_config_rt(config, runtime);

    status(format!("registering {}", args.source.display()))?;
    ctx.register_parquet(
        SOURCE_TABLE,
        args.source.to_str().ok_or_else(|| anyhow!("source path UTF-8"))?,
        ParquetReadOptions::default(),
    )
    .await
    .context("failed to register source parquet")?;

    status("building writer properties")?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(args.compression).unwrap_or_default()))
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_max_row_group_size(args.row_group_size)
        .set_key_value_metadata(Some(vec![KeyValue {
            key: "created_by".to_string(),
            value: Some("ships".to_string()),
        }]))
        .build();

    let mut writer = ShipsWriter::new(args.ships_dir.clone(), props);
    let total_groups = run_pipeline(&ctx, &mut writer, SOURCE_TABLE).await?;
    writer.close_all()?;

    status(format!("done: {} identities written to {}", total_groups, args.ships_dir.display()))?;
    Ok(())
}
