using System.CommandLine;
using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Extensions.Logging;
using Parquet;
using Parquet.Data;

namespace csharp_etl;

class Program
{
    static async Task<int> Main(string[] args)
    {
        var sourceBucketOption = new Option<string>(
            name: "--source-bucket",
            description: "The source S3 bucket.")
        { IsRequired = true };

        var targetBucketOption = new Option<string>(
            name: "--target-bucket",
            description: "The target S3 bucket.")
        { IsRequired = true };

        var rootCommand = new RootCommand("C# ETL job to process CSV files from S3 to Parquet.")
        {
            sourceBucketOption,
            targetBucketOption
        };

        rootCommand.SetHandler(async (sourceBucket, targetBucket) =>
        {
            await RunEtlPipeline(sourceBucket, targetBucket);
        }, sourceBucketOption, targetBucketOption);

        return await rootCommand.InvokeAsync(args);
    }

    private static async Task RunEtlPipeline(string sourceBucket, string targetBucket)
    {
        using var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddSimpleConsole(options =>
            {
                options.IncludeScopes = true;
                options.SingleLine = true;
                options.TimestampFormat = "yyyy-MM-dd HH:mm:ss ";
            });
        });
        var logger = loggerFactory.CreateLogger<Program>();

        var pipeline = new EtlPipeline(logger, sourceBucket, targetBucket);
        try
        {
            await pipeline.ProcessAllFilesAsync();
            logger.LogInformation("ETL pipeline completed successfully!");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Pipeline failed: {Message}", ex.Message);
        }
        finally
        {
            pipeline.Cleanup();
        }
    }
}

// Represents the structure of the data in the Parquet file.
public class ParquetRecord
{
    public string Date { get; set; } = "";
    public int Year { get; set; }
    public string Quarter { get; set; } = "";
    public int Month { get; set; }
    public string RawData { get; set; } = "";
    public string SourceFile { get; set; } = "";
}

// A struct to hold the final ETL statistics.
public class EtlStats
{
    public string TotalExecutionTime { get; set; } = "";
    public int TotalFilesFound { get; set; }
    public int FilesProcessed { get; set; }
    public int FilesFailed { get; set; }
    public long TotalRowsProcessed { get; set; }
    public long TotalBytesProcessed { get; set; }
    public double ProcessingThroughputGBs { get; set; }
}

// Holds the result of processing a single file.
public record ProcessedFileResult(long Rows, long Bytes);

// Holds the metadata extracted from the file path.
public record FileMetadata(string Date, string QuarterStr, int Year, int Quarter, int Month);

public class EtlPipeline
{
    private readonly ILogger _logger;
    private readonly IAmazonS3 _s3Client;
    private readonly TransferUtility _transferUtility;
    private readonly string _sourceBucket;
    private readonly string _targetBucket;
    private readonly int _workerCount;
    private readonly string _tempDir;

    public EtlPipeline(ILogger logger, string sourceBucket, string targetBucket)
    {
        _logger = logger;
        // Ensure AWS SDK uses the default region if not specified elsewhere (e.g., environment variables)
        var s3Config = new AmazonS3Config { RegionEndpoint = RegionEndpoint.USEast1 };
        _s3Client = new AmazonS3Client(s3Config);
        _transferUtility = new TransferUtility(_s3Client);
        _sourceBucket = sourceBucket;
        _targetBucket = targetBucket;
        _workerCount = Environment.ProcessorCount * 2;
        _tempDir = Path.Combine(Path.GetTempPath(), "parquet_temp_csharp");
        Directory.CreateDirectory(_tempDir);
    }

    public void Cleanup()
    {
        _logger.LogInformation("Cleaning up temp directory: {TempDir}", _tempDir);
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, true);
        }
    }
    
    public async Task TestS3AccessAsync()
    {
        _logger.LogInformation("Testing S3 access...");
        const string testKey = "connection-test.txt";
        const string testContent = "S3 connection test successful";

        try
        {
            await _s3Client.PutObjectAsync(new PutObjectRequest
            {
                BucketName = _targetBucket,
                Key = testKey,
                ContentBody = testContent
            });
            _logger.LogInformation("S3 upload test SUCCESSFUL");

            await _s3Client.DeleteObjectAsync(new DeleteObjectRequest
            {
                BucketName = _targetBucket,
                Key = testKey
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "S3 access test FAILED");
            throw;
        }
    }

    public async Task ProcessAllFilesAsync()
    {
        _logger.LogInformation("Starting ETL pipeline...");
        var stopwatch = Stopwatch.StartNew();

        await TestS3AccessAsync();

        _logger.LogInformation("Listing files in bucket: {SourceBucket}", _sourceBucket);
        var allKeys = new List<string>();
        var paginator = _s3Client.Paginators.ListObjectsV2(new ListObjectsV2Request { BucketName = _sourceBucket });
        await foreach (var response in paginator.Responses)
        {
            allKeys.AddRange(response.S3Objects.Where(o => o.Key.EndsWith(".csv")).Select(o => o.Key));
        }
        
        int totalFiles = allKeys.Count;
        _logger.LogInformation("Found {TotalFiles} CSV files to process", totalFiles);
        if (totalFiles == 0) return;

        int processed = 0;
        long failed = 0;
        long totalRowsProcessed = 0;
        long totalBytesProcessed = 0;

        var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = _workerCount };
        await Parallel.ForEachAsync(allKeys, parallelOptions, async (key, cancellationToken) =>
        {
            try
            {
                var result = await ProcessCsvFileAsync(key);
                int processedCount = Interlocked.Increment(ref processed);
                Interlocked.Add(ref totalRowsProcessed, result.Rows);
                Interlocked.Add(ref totalBytesProcessed, result.Bytes);

                if (processedCount % 10 == 0 || processedCount <= 5)
                {
                    _logger.LogInformation("Progress: {Processed} processed, {Failed} failed, {Remaining} remaining",
                        processedCount, Interlocked.Read(ref failed), totalFiles - processedCount - Interlocked.Read(ref failed));
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ERROR processing {Key}", key);
                Interlocked.Increment(ref failed);
            }
        });
        
        stopwatch.Stop();
        _logger.LogInformation("ETL pipeline completed in {Duration}", stopwatch.Elapsed);
        _logger.LogInformation("Final stats: {Processed} processed, {Failed} failed, {Total} total",
            processed, failed, totalFiles);
        
        if (failed > 0)
        {
            _logger.LogWarning("{FailedCount} files failed to process", failed);
        }

        var stats = new EtlStats
        {
            TotalExecutionTime = stopwatch.Elapsed.ToString(),
            TotalFilesFound = totalFiles,
            FilesProcessed = processed,
            FilesFailed = (int)failed,
            TotalRowsProcessed = totalRowsProcessed,
            TotalBytesProcessed = totalBytesProcessed,
            ProcessingThroughputGBs = stopwatch.Elapsed.TotalSeconds > 0 ? (double)totalBytesProcessed / 1e9 / stopwatch.Elapsed.TotalSeconds : 0
        };

        var options = new JsonSerializerOptions { WriteIndented = true };
        var statsJson = JsonSerializer.Serialize(stats, options);
        await File.WriteAllTextAsync("etl_stats.json", statsJson);
        _logger.LogInformation("Successfully wrote stats to etl_stats.json");
    }
    
    private async Task<ProcessedFileResult> ProcessCsvFileAsync(string key)
    {
        _logger.LogInformation("Processing file: {Key}", key);

        var metadata = ExtractDateAndMetadata(key);
        if (metadata == null)
        {
            _logger.LogWarning("Skipping file {Key}: could not extract metadata", key);
            return new ProcessedFileResult(0, 0);
        }
        
        _logger.LogInformation("Extracted: date={Date}, year={Year}, quarter={Quarter}, month={Month}",
            metadata.Date, metadata.Year, metadata.QuarterStr, metadata.Month);
            
        _logger.LogInformation("Downloading and streaming {Key} from S3...", key);
        
        // Open the stream from S3. We will read from this directly.
        using var responseStream = await _transferUtility.OpenStreamAsync(_sourceBucket, key);
        long bytesProcessed = responseStream.Length; // Get the size from the stream's metadata

        _logger.LogInformation("Downloaded {Bytes} bytes, parsing CSV...", bytesProcessed);
        
        // Pass the S3 stream directly to the reader, avoiding an in-memory copy.
        using var reader = new StreamReader(responseStream);
        var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture) { HasHeaderRecord = false, MissingFieldFound = null };
        using var csv = new CsvReader(reader, csvConfig);
        
        var records = new List<ParquetRecord>();
        while (await csv.ReadAsync())
        {
            var row = csv.Context.Parser.Record;
            if (row == null || row.All(string.IsNullOrEmpty)) continue;
            
            var dataString = string.Join("|", row);
            records.Add(new ParquetRecord
            {
                Date = metadata.Date,
                Year = metadata.Year,
                Quarter = metadata.QuarterStr,
                Month = metadata.Month,
                RawData = dataString,
                SourceFile = key
            });
        }
        
        if (records.Count == 0)
        {
            _logger.LogInformation("No valid records found in {Key}", key);
            return new ProcessedFileResult(0, bytesProcessed);
        }
        
        _logger.LogInformation("Parsed {RecordCount} records from {Key}", records.Count, key);

        await WriteParquetFileAndUploadAsync(records, metadata);
        return new ProcessedFileResult(records.Count, bytesProcessed);
    }
    
     private FileMetadata? ExtractDateAndMetadata(string filePath)
    {
        var fileName = Path.GetFileNameWithoutExtension(filePath);
        DateTime? parsedDate = null;
        
        string[] dateFormats = {
            "yyyyMMdd", "yyyy-MM-dd", "yyyy_MM_dd", 
            "MM-dd-yyyy", "MM_dd_yyyy", 
            "yyyy-M-d", "M-d-yyyy"
        };
        
        foreach (var format in dateFormats)
        {
            if (DateTime.TryParseExact(fileName, format, CultureInfo.InvariantCulture, DateTimeStyles.None, out var date))
            {
                parsedDate = date;
                break;
            }
        }
        
        if (parsedDate == null)
        {
            _logger.LogInformation("Could not parse date from filename '{FileName}', trying path extraction", fileName);
            var yearRegex = new Regex(@"data_(?:Q[1-4]_)?(\d{4})");
            var match = yearRegex.Match(filePath);
            if (match.Success && int.TryParse(match.Groups[1].Value, out int year))
            {
                parsedDate = new DateTime(year, 1, 1);
                _logger.LogInformation("Extracted year {Year} from path, using default date", year);
            }
        }
        
        if (parsedDate == null)
        {
            _logger.LogError("Unable to parse date from filename '{FileName}' or path '{FilePath}'", fileName, filePath);
            return null;
        }
        
        var finalDate = parsedDate.Value;
        int month = finalDate.Month;
        int quarter = (month - 1) / 3 + 1;
        
        return new FileMetadata(
            finalDate.ToString("yyyy-MM-dd"),
            $"q{quarter}",
            finalDate.Year,
            quarter,
            month
        );
    }
    
    private async Task WriteParquetFileAndUploadAsync(List<ParquetRecord> records, FileMetadata metadata)
    {
        var s3OutputKey = $"{metadata.Year}/{metadata.QuarterStr}/{metadata.Date}.parquet";
        var localFileName = $"temp_{metadata.Year}_{metadata.QuarterStr}_{metadata.Date}_{DateTime.Now.Ticks}.parquet";
        var localPath = Path.Combine(_tempDir, localFileName);
        
        _logger.LogInformation("Creating local parquet file: {LocalPath}", localPath);
        
        // Create Parquet file schema
        var schema = new Schema(
            new DataField<string>("date"),
            new DataField<int>("year"),
            new DataField<string>("quarter"),
            new DataField<int>("month"),
            new DataField<string>("raw_data"),
            new DataField<string>("source_file")
        );
        
        using (var fileStream = new FileStream(localPath, FileMode.Create, FileAccess.Write))
        {
            using (var parquetWriter = await ParquetWriter.CreateAsync(schema, fileStream))
            {
                parquetWriter.CompressionMethod = CompressionMethod.Snappy;
                
                using (var rowGroupWriter = parquetWriter.CreateRowGroup())
                {
                    await rowGroupWriter.WriteColumnAsync(new DataColumn(
                        (DataField)schema.Fields[0], records.Select(r => r.Date).ToArray()));
                    await rowGroupWriter.WriteColumnAsync(new DataColumn(
                        (DataField)schema.Fields[1], records.Select(r => r.Year).ToArray()));
                    await rowGroupWriter.WriteColumnAsync(new DataColumn(
                        (DataField)schema.Fields[2], records.Select(r => r.Quarter).ToArray()));
                    await rowGroupWriter.WriteColumnAsync(new DataColumn(
                        (DataField)schema.Fields[3], records.Select(r => r.Month).ToArray()));
                    await rowGroupWriter.WriteColumnAsync(new DataColumn(
                        (DataField)schema.Fields[4], records.Select(r => r.RawData).ToArray()));
                    await rowGroupWriter.WriteColumnAsync(new DataColumn(
                        (DataField)schema.Fields[5], records.Select(r => r.SourceFile).ToArray()));
                }
            }
        }
        
        var fileInfo = new FileInfo(localPath);
        _logger.LogInformation("Local parquet file created successfully: {LocalPath}, size: {Size} bytes", localPath, fileInfo.Length);
        
        _logger.LogInformation("Uploading to S3: {S3Key}", s3OutputKey);
        await _transferUtility.UploadAsync(localPath, _targetBucket, s3OutputKey);
        _logger.LogInformation("Successfully uploaded to S3: {S3Key}", s3OutputKey);
        
        File.Delete(localPath);

        // Verify upload
        await _s3Client.GetObjectMetadataAsync(_targetBucket, s3OutputKey);
        _logger.LogInformation("Upload verified successfully: {S3Key}", s3OutputKey);
    }
} 