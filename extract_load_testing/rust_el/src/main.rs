use anyhow::{anyhow, Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use chrono::{Datelike, NaiveDate};
use clap::Parser;
use futures::stream::{self, StreamExt};
use futures::TryStreamExt;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rand::Rng;
use regex::Regex;
use serde::Serialize;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::fs;
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

/// A struct to hold the result of processing a single file.
#[derive(Debug, Clone, Copy, Default)]
struct ProcessedFileResult {
    rows: u64,
    bytes: u64,
}

/// A struct that holds the configuration for the ETL pipeline.
struct EtlPipeline {
    s3_client: S3Client,
    source_bucket: String,
    target_bucket: String,
    worker_count: usize,
    temp_dir: PathBuf,
}

/// Defines the schema for the Parquet file records.
/// Note: No lifetimes needed in a sync context like this.
#[derive(Debug, Serialize)]
struct ParquetRecord {
    date: String,
    year: i32,
    quarter: String,
    month: i32,
    raw_data: String,
    source_file: String,
}

/// A struct to hold the final ETL statistics.
#[derive(Debug, Serialize)]
struct EtlStats {
    total_execution_time: String,
    total_files_found: usize,
    files_processed: usize,
    files_failed: usize,
    total_rows_processed: u64,
    total_bytes_processed: u64,
    processing_throughput_gb_per_sec: f64,
}

/// Metadata extracted from a file path.
#[derive(Debug, Clone)]
struct FileMetadata {
    date: String,
    quarter_str: String,
    year: i32,
    month: u32,
}

/// Command-line arguments for the application.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The source S3 bucket
    source_bucket: String,

    /// The target S3 bucket
    target_bucket: String,
}

impl EtlPipeline {
    /// Creates a new ETL pipeline instance.
    async fn new(source_bucket: String, target_bucket: String) -> Result<Self> {
        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let s3_client = S3Client::new(&config);

        let temp_dir = std::env::temp_dir().join("parquet_temp_rust");
        fs::create_dir_all(&temp_dir)
            .await
            .context("Failed to create temp directory")?;

        Ok(Self {
            s3_client,
            source_bucket,
            target_bucket,
            worker_count: num_cpus::get() * 2,
            temp_dir,
        })
    }

    /// Cleans up the temporary directory.
    async fn cleanup(&self) -> Result<()> {
        info!("Cleaning up temp directory: {:?}", self.temp_dir);
        if self.temp_dir.exists() {
            fs::remove_dir_all(&self.temp_dir)
                .await
                .context("Failed to clean up temp directory")?;
        }
        Ok(())
    }

    /// Tests access to S3 by uploading and deleting a test file.
    async fn test_s3_access(&self) -> Result<()> {
        info!("Testing S3 access...");
        let test_key = "connection-test.txt";
        let test_content = "S3 connection test successful";
        let body = ByteStream::from(test_content.as_bytes().to_vec());

        self.s3_client
            .put_object()
            .bucket(&self.target_bucket)
            .key(test_key)
            .body(body)
            .send()
            .await
            .context("S3 upload test failed")?;
        info!("S3 upload test SUCCESSFUL");

        self.s3_client
            .delete_object()
            .bucket(&self.target_bucket)
            .key(test_key)
            .send()
            .await
            .context("Failed to clean up test file")?;

        Ok(())
    }

    /// Processes all CSV files found in the source S3 bucket.
    async fn process_all_files(&self) -> Result<()> {
        info!("Starting ETL pipeline...");
        let start_time = Instant::now();

        self.test_s3_access()
            .await
            .context("S3 access test failed")?;

        info!("Listing files in bucket: {}", self.source_bucket);
        let mut all_keys = Vec::new();
        let mut paginator = self
            .s3_client
            .list_objects_v2()
            .bucket(&self.source_bucket)
            .into_paginator()
            .send();

        while let Some(result) = paginator.try_next().await? {
            for obj in result.contents() {
                if let Some(key) = obj.key() {
                    if key.ends_with(".csv") {
                        all_keys.push(key.to_string());
                    }
                }
            }
        }

        let total_files = all_keys.len();
        info!("Found {} CSV files to process", total_files);
        if total_files == 0 {
            info!("No CSV files found in source bucket.");
            return Ok(());
        }

        let processed = Arc::new(AtomicUsize::new(0));
        let failed = Arc::new(AtomicUsize::new(0));
        let total_rows = Arc::new(AtomicU64::new(0));
        let total_bytes = Arc::new(AtomicU64::new(0));

        stream::iter(all_keys)
            .for_each_concurrent(self.worker_count, |key| {
                let processed = Arc::clone(&processed);
                let failed = Arc::clone(&failed);
                let total_rows = Arc::clone(&total_rows);
                let total_bytes = Arc::clone(&total_bytes);
                async move {
                    match self.process_csv_file(&key).await {
                        Ok(result) => {
                            let processed_count = processed.fetch_add(1, Ordering::SeqCst) + 1;
                            total_rows.fetch_add(result.rows as u64, Ordering::SeqCst);
                            total_bytes.fetch_add(result.bytes, Ordering::SeqCst);

                            let failed_count = failed.load(Ordering::SeqCst);
                            if processed_count % 10 == 0 || processed_count <= 5 {
                                info!(
                                    "Progress: {} processed, {} failed, {} remaining",
                                    processed_count,
                                    failed_count,
                                    total_files - processed_count - failed_count
                                );
                            }
                        }
                        Err(e) => {
                            error!("ERROR processing {}: {:?}", key, e);
                            failed.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
            })
            .await;

        let duration = start_time.elapsed();
        let processed_count = processed.load(Ordering::SeqCst);
        let failed_count = failed.load(Ordering::SeqCst);
        let total_rows_processed = total_rows.load(Ordering::SeqCst);
        let total_bytes_processed = total_bytes.load(Ordering::SeqCst);

        info!("ETL pipeline completed in {:?}", duration);
        info!(
            "Final stats: {} processed, {} failed, {} total",
            processed_count, failed_count, total_files
        );
        if failed_count > 0 {
            warn!("{} files failed to process", failed_count);
        }

        // Create and write stats
        let duration_sec = duration.as_secs_f64();
        let stats = EtlStats {
            total_execution_time: format!("{:?}", duration),
            total_files_found: total_files,
            files_processed: processed_count,
            files_failed: failed_count,
            total_rows_processed,
            total_bytes_processed,
            processing_throughput_gb_per_sec: if duration_sec > 0.0 {
                (total_bytes_processed as f64 / 1e9) / duration_sec
            } else {
                0.0
            },
        };

        let stats_json = serde_json::to_string_pretty(&stats)?;
        fs::write("etl_stats.json", stats_json)
            .await
            .context("Failed to write stats file")?;
        info!("Successfully wrote stats to etl_stats.json");

        Ok(())
    }

    /// Processes a single CSV file from S3.
    async fn process_csv_file(&self, key: &str) -> Result<ProcessedFileResult> {
        info!("Processing file: {}", key);

        let metadata = match extract_date_and_metadata(key) {
            Ok(meta) => meta,
            Err(e) => {
                warn!("Skipping file {}: {}", key, e);
                return Ok(ProcessedFileResult { rows: 0, bytes: 0 }); // Skip file as per Go logic
            }
        };

        info!(
            "Extracted: date={}, year={}, quarter={}, month={}",
            metadata.date, metadata.year, metadata.quarter_str, metadata.month
        );

        info!("Downloading {} from S3...", key);
        let mut object = self
            .s3_client
            .get_object()
            .bucket(&self.source_bucket)
            .key(key)
            .send()
            .await
            .context(format!("Failed to download {}", key))?;

        let mut csv_data = Vec::new();
        while let Some(chunk) = object.body.try_next().await? {
            csv_data.extend_from_slice(&chunk);
        }
        let bytes_processed = csv_data.len() as u64;
        info!("Downloaded {} bytes, parsing CSV...", bytes_processed);

        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(csv_data.as_slice());

        let mut records = Vec::new();
        for result in reader.records() {
            let record = result.context("Error reading CSV record")?;
            if record.is_empty()
                || (record.len() == 1 && record.get(0).unwrap_or("").is_empty())
            {
                continue;
            }
            let data_string = record.iter().collect::<Vec<_>>().join("|");

            let parquet_record = ParquetRecord {
                date: metadata.date.clone(),
                year: metadata.year,
                quarter: metadata.quarter_str.clone(),
                month: metadata.month as i32,
                raw_data: data_string,
                source_file: key.to_string(),
            };
            records.push(parquet_record);
        }

        if records.is_empty() {
            info!("No valid records found in {}", key);
            return Ok(ProcessedFileResult {
                rows: 0,
                bytes: bytes_processed,
            });
        }

        info!("Parsed {} records from {}", records.len(), key);

        self.write_parquet_file_and_upload(&records, &metadata)
            .await?;

        Ok(ProcessedFileResult {
            rows: records.len() as u64,
            bytes: bytes_processed,
        })
    }

    /// Writes records to a local Parquet file and uploads it to S3.
    async fn write_parquet_file_and_upload<'a>(
        &self,
        records: &[ParquetRecord],
        metadata: &FileMetadata,
    ) -> Result<()> {
        let s3_output_key =
            format!("{}/{}/{}.parquet", metadata.year, metadata.quarter_str, metadata.date);

        let timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let rand_num: u32 = rand::thread_rng().gen();
        let local_file_name = format!(
            "temp_{}_{}_{}_{}_{}.parquet",
            metadata.year, metadata.quarter_str, metadata.date, timestamp, rand_num
        );
        let local_path = self.temp_dir.join(&local_file_name);

        info!("Creating local parquet file: {:?}", local_path);

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(&local_path)?;
        let schema = ParquetRecord::to_arrow_schema()?;
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props))?;

        let batch = ParquetRecord::to_arrow_batch(records)?;
        writer.write(&batch)?;
        writer.close()?;

        let file_meta = std::fs::metadata(&local_path)?;
        info!(
            "Local parquet file created successfully: {:?}, size: {} bytes",
            local_path,
            file_meta.len()
        );

        info!("Uploading to S3: {}", s3_output_key);
        let body = ByteStream::from_path(&local_path).await?;

        self.s3_client
            .put_object()
            .bucket(&self.target_bucket)
            .key(&s3_output_key)
            .body(body)
            .send()
            .await
            .with_context(|| format!("Failed to upload {:?} to S3", local_path))?;

        info!("Successfully uploaded to S3: {}", s3_output_key);

        tokio::fs::remove_file(&local_path)
            .await
            .with_context(|| format!("Failed to remove temp file {:?}", local_path))?;

        self.s3_client
            .head_object()
            .bucket(&self.target_bucket)
            .key(&s3_output_key)
            .send()
            .await
            .with_context(|| format!("Upload verification failed for {}", s3_output_key))?;

        info!("Upload verified successfully: {}", s3_output_key);

        Ok(())
    }
}

/// Helper functions to convert struct to Arrow for Parquet writing.
impl ParquetRecord {
    fn to_arrow_schema() -> Result<arrow_schema::Schema> {
        use arrow_schema::{DataType, Field, Schema};
        Ok(Schema::new(vec![
            Field::new("date", DataType::Utf8, false),
            Field::new("year", DataType::Int32, false),
            Field::new("quarter", DataType::Utf8, false),
            Field::new("month", DataType::Int32, false),
            Field::new("raw_data", DataType::Utf8, false),
            Field::new("source_file", DataType::Utf8, false),
        ]))
    }

    fn to_arrow_batch(records: &[Self]) -> Result<arrow_array::RecordBatch> {
        use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
        use std::sync::Arc;

        let batch = RecordBatch::try_new(
            Arc::new(Self::to_arrow_schema()?),
            vec![
                Arc::new(StringArray::from_iter_values(records.iter().map(|r| &r.date))) as ArrayRef,
                Arc::new(Int32Array::from_iter_values(records.iter().map(|r| r.year))) as ArrayRef,
                Arc::new(StringArray::from_iter_values(records.iter().map(|r| &r.quarter)))
                    as ArrayRef,
                Arc::new(Int32Array::from_iter_values(records.iter().map(|r| r.month)))
                    as ArrayRef,
                Arc::new(StringArray::from_iter_values(
                    records.iter().map(|r| &r.raw_data),
                )) as ArrayRef,
                Arc::new(StringArray::from_iter_values(
                    records.iter().map(|r| &r.source_file),
                )) as ArrayRef,
            ],
        )?;
        Ok(batch)
    }
}

/// Extracts date and metadata from a file path, mimicking the Go implementation.
fn extract_date_and_metadata(file_path: &str) -> Result<FileMetadata> {
    let filename = Path::new(file_path)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    let filename_no_ext = Path::new(filename)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(filename);

    let mut parsed_date: Option<NaiveDate> = None;

    let date_formats = [
        "%Y%m%d",     // 20240315
        "%Y-%m-%d",   // 2024-03-15
        "%Y_%m_%d",   // 2024_03_15
        "%m-%d-%Y",   // 03-15-2024
        "%m_%d_%Y",   // 03_15_2024
        "%Y-%-m-%-d", // 2024-3-15 (single digit month/day)
        "%-m-%-d-%Y", // 3-15-2024
    ];

    for format in date_formats {
        if let Ok(date) = NaiveDate::parse_from_str(filename_no_ext, format) {
            parsed_date = Some(date);
            break;
        }
    }

    if parsed_date.is_none() {
        info!(
            "Could not parse date from filename '{}', trying path extraction",
            filename
        );
        let year_regex = Regex::new(r"data_(?:Q[1-4]_)?(\d{4})").unwrap();
        if let Some(captures) = year_regex.captures(file_path) {
            if let Some(year_match) = captures.get(1) {
                if let Ok(year) = year_match.as_str().parse::<i32>() {
                    // Use January 1st as default
                    parsed_date = NaiveDate::from_ymd_opt(year, 1, 1);
                    info!("Extracted year {} from path, using default date", year);
                }
            }
        }
    }

    let parsed_date = parsed_date.ok_or_else(|| {
        anyhow!(
            "Unable to parse date from filename '{}' or path '{}'",
            filename,
            file_path
        )
    })?;

    let year = parsed_date.year();
    let month = parsed_date.month();
    let quarter = ((month - 1) / 3) + 1;
    let quarter_str = format!("q{}", quarter);
    let date_str = parsed_date.format("%Y-%m-%d").to_string();

    Ok(FileMetadata {
        date: date_str,
        quarter_str,
        year,
        month,
    })
}

fn get_s3_key(metadata: &FileMetadata) -> String {
    format!("{}/{}/{}.parquet", metadata.year, metadata.quarter_str, metadata.date)
}

fn get_local_path(metadata: &FileMetadata, temp_dir: &tempfile::TempDir, suffix: &str) -> PathBuf {
    let rand_num: u32 = rand::thread_rng().gen();
    let local_file_name = format!(
        "temp_{}_{}_{}_{}_{}.parquet",
        metadata.year, metadata.quarter_str, metadata.date, rand_num, suffix
    );
    temp_dir.path().join(local_file_name)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    let args = Args::parse();

    info!("Starting ETL Pipeline");
    info!("Source bucket: {}", args.source_bucket);
    info!("Target bucket: {}", args.target_bucket);
    info!("Worker count: {}", num_cpus::get() * 2);

    let pipeline = EtlPipeline::new(args.source_bucket, args.target_bucket).await?;

    let pipeline_result = pipeline.process_all_files().await;

    pipeline.cleanup().await?;

    if let Err(e) = pipeline_result {
        error!("Pipeline failed: {:?}", e);
        // Return a non-zero exit code on failure
        return Err(e);
    }

    info!("ETL pipeline completed successfully!");
    Ok(())
} 