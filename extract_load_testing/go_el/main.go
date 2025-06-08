package main

import (
    "context"
    "encoding/csv"
    "encoding/json"
    "fmt"
    "io"
    "log"
    "os"
    "regexp"
    "runtime"
    "strconv"
    "strings"
    "sync"
    "time"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/s3"
    "github.com/aws/aws-sdk-go/service/s3/s3manager"
    "github.com/xitongsys/parquet-go/parquet"
    "github.com/xitongsys/parquet-go/writer"
    "github.com/xitongsys/parquet-go-source/local"
)

// Parquet record structure
type ParquetRecord struct {
    Date    string `parquet:"name=date, type=BYTE_ARRAY, convertedtype=UTF8"`
    Year    int32  `parquet:"name=year, type=INT32"`
    Quarter string `parquet:"name=quarter, type=BYTE_ARRAY, convertedtype=UTF8"`
    Month   int32  `parquet:"name=month, type=INT32"`
    Data    string `parquet:"name=raw_data, type=BYTE_ARRAY, convertedtype=UTF8"`
    Source  string `parquet:"name=source_file, type=BYTE_ARRAY, convertedtype=UTF8"`
}

// ETLStats holds the performance metrics of the ETL job.
type ETLStats struct {
    TotalExecutionTime      string  `json:"total_execution_time"`
    TotalFilesFound         int     `json:"total_files_found"`
    FilesProcessed          int     `json:"files_processed"`
    FilesFailed             int     `json:"files_failed"`
    TotalRowsProcessed      int64   `json:"total_rows_processed"`
    TotalBytesProcessed     int64   `json:"total_bytes_processed"`
    ProcessingThroughputGBs float64 `json:"processing_throughput_gb_per_sec"`
}

type ETLPipeline struct {
    s3Client     *s3.S3
    downloader   *s3manager.Downloader
    uploader     *s3manager.Uploader
    sourceBucket string
    targetBucket string
    workerCount  int
    tempDir      string
}

func NewETLPipeline(sourceBucket, targetBucket string) *ETLPipeline {
    sess := session.Must(session.NewSession(&aws.Config{
        Region: aws.String("us-east-1"), // Change to your region
    }))

    // Create temp directory for parquet files
    tempDir := "/tmp/parquet_temp"
    if err := os.MkdirAll(tempDir, 0755); err != nil {
        log.Fatalf("Failed to create temp directory: %v", err)
    }

    return &ETLPipeline{
        s3Client:     s3.New(sess),
        downloader:   s3manager.NewDownloader(sess),
        uploader:     s3manager.NewUploader(sess),
        sourceBucket: sourceBucket,
        targetBucket: targetBucket,
        workerCount:  runtime.NumCPU() * 2,
        tempDir:      tempDir,
    }
}

// Test S3 connectivity
func (etl *ETLPipeline) testS3Access() error {
    log.Println("Testing S3 access...")

    testKey := "connection-test.txt"
    testContent := "S3 connection test successful"

    // Test upload
    _, err := etl.uploader.Upload(&s3manager.UploadInput{
        Bucket: aws.String(etl.targetBucket),
        Key:    aws.String(testKey),
        Body:   strings.NewReader(testContent),
    })

    if err != nil {
        log.Printf("S3 upload test FAILED: %v", err)
        return fmt.Errorf("S3 upload test failed: %w", err)
    }

    log.Printf("S3 upload test SUCCESSFUL")

    // Clean up test file
    _, err = etl.s3Client.DeleteObject(&s3.DeleteObjectInput{
        Bucket: aws.String(etl.targetBucket),
        Key:    aws.String(testKey),
    })
    if err != nil {
        log.Printf("Warning: Failed to clean up test file: %v", err)
    }

    return nil
}

// Extract date and metadata from file path and filename
func extractDateAndMetadata(filePath string) (date, quarterStr string, year int, quarter, month int, err error) {
    // Extract filename from path
    pathParts := strings.Split(filePath, "/")
    filename := pathParts[len(pathParts)-1]
    filename = strings.TrimSuffix(filename, ".csv")

    var parsedDate time.Time

    // Try different date formats in filename
    dateFormats := []string{
        "20060102",     // 20240315
        "2006-01-02",   // 2024-03-15
        "2006_01_02",   // 2024_03_15
        "01-02-2006",   // 03-15-2024
        "01_02_2006",   // 03_15_2024
        "2006-1-2",     // 2024-3-15 (single digit month/day)
        "1-2-2006",     // 3-15-2024
    }

    for _, format := range dateFormats {
        if t, err := time.Parse(format, filename); err == nil {
            parsedDate = t
            break
        }
    }

    // If filename parsing failed, try to extract year from path and use default date
    if parsedDate.IsZero() {
        log.Printf("Could not parse date from filename '%s', trying path extraction", filename)

        // Extract year from path patterns
        yearRegex := regexp.MustCompile(`data_(?:Q[1-4]_)?(\d{4})`)
        if matches := yearRegex.FindStringSubmatch(filePath); len(matches) > 1 {
            if y, err := strconv.Atoi(matches[1]); err == nil {
                // Use January 1st as default
                parsedDate = time.Date(y, 1, 1, 0, 0, 0, 0, time.UTC)
                log.Printf("Extracted year %d from path, using default date", y)
            }
        }
    }

    if parsedDate.IsZero() {
        return "", "", 0, 0, 0, fmt.Errorf("unable to parse date from filename '%s' or path '%s'", filename, filePath)
    }

    // Extract year, quarter, month from parsed date
    year = parsedDate.Year()
    month = int(parsedDate.Month())
    quarter = ((month - 1) / 3) + 1
    quarterStr = fmt.Sprintf("q%d", quarter)
    date = parsedDate.Format("2006-01-02")

    return date, quarterStr, year, quarter, month, nil
}

func (etl *ETLPipeline) processCSVFile(ctx context.Context, key string) (int, int64, error) {
    log.Printf("Processing file: %s", key)

    // Extract date and metadata
    date, quarterStr, year, _, month, err := extractDateAndMetadata(key)
    if err != nil {
        log.Printf("Skipping file %s: %s", key, err)
        return 0, 0, nil // Return 0 rows and bytes, but no error to indicate skipping
    }

    log.Printf("Extracted: date=%s, year=%d, quarter=%s, month=%d", date, year, quarterStr, month)

    // Download file from S3
    log.Printf("Downloading %s from S3...", key)
    buffer := &aws.WriteAtBuffer{}
    _, err = etl.downloader.DownloadWithContext(ctx, buffer,
        &s3.GetObjectInput{
            Bucket: aws.String(etl.sourceBucket),
            Key:    aws.String(key),
        })
    if err != nil {
        return 0, 0, fmt.Errorf("failed to download %s: %w", key, err)
    }
    bytesProcessed := int64(len(buffer.Bytes()))

    log.Printf("Downloaded %d bytes, parsing CSV...", bytesProcessed)

    // Parse CSV
    reader := csv.NewReader(strings.NewReader(string(buffer.Bytes())))
    reader.FieldsPerRecord = -1 // Allow variable number of fields

    var records []ParquetRecord
    rowNum := 0

    for {
        record, err := reader.Read()
        if err == io.EOF {
            break
        }
        if err != nil {
            log.Printf("Error reading CSV row %d in %s: %s", rowNum, key, err)
            continue
        }

        // Skip empty rows
        if len(record) == 0 || (len(record) == 1 && record[0] == "") {
            continue
        }

        // Convert CSV row to delimited string for storage
        dataString := strings.Join(record, "|") // Using pipe separator

        parquetRecord := ParquetRecord{
            Date:    date,
            Year:    int32(year),
            Quarter: quarterStr,
            Month:   int32(month),
            Data:    dataString,
            Source:  key,
        }
        records = append(records, parquetRecord)
        rowNum++
    }

    if len(records) == 0 {
        log.Printf("No valid records found in %s", key)
        return 0, bytesProcessed, nil
    }

    log.Printf("Parsed %d records from %s", len(records), key)

    // Write to Parquet and upload to S3
    err = etl.writeParquetFileAndUpload(ctx, records, year, quarterStr, date)
    if err != nil {
        return 0, bytesProcessed, err
    }

    return len(records), bytesProcessed, nil
}

func (etl *ETLPipeline) writeParquetFileAndUpload(ctx context.Context, records []ParquetRecord, year int, quarter, date string) error {
    // Create S3 output path: year/quarter/date.parquet
    s3OutputKey := fmt.Sprintf("%d/%s/%s.parquet", year, quarter, date)

    // Create unique local temp file
    timestamp := time.Now().UnixNano()
    localFileName := fmt.Sprintf("%s/temp_%d_%s_%s_%d.parquet", etl.tempDir, year, quarter, date, timestamp)

    log.Printf("Creating local parquet file: %s", localFileName)

    // Create local parquet file
    fw, err := local.NewLocalFileWriter(localFileName)
    if err != nil {
        return fmt.Errorf("failed to create local file writer: %w", err)
    }

    // Create Parquet writer
    pw, err := writer.NewParquetWriter(fw, new(ParquetRecord), 4)
    if err != nil {
        fw.Close()
        os.Remove(localFileName)
        return fmt.Errorf("failed to create parquet writer: %w", err)
    }

    // Configure compression
    pw.CompressionType = parquet.CompressionCodec_SNAPPY

    log.Printf("Writing %d records to parquet file...", len(records))

    // Write all records
    for i, record := range records {
        if err := pw.Write(record); err != nil {
            log.Printf("Error writing record %d: %s", i, err)
            continue
        }

        // Flush periodically for large files
        if (i+1)%100000 == 0 {
            log.Printf("Written %d/%d records", i+1, len(records))
            pw.Flush(true)
        }
    }

    log.Printf("Finalizing parquet file...")

    // Close parquet writer
    if err := pw.WriteStop(); err != nil {
        fw.Close()
        os.Remove(localFileName)
        return fmt.Errorf("error in WriteStop: %w", err)
    }

    if err := fw.Close(); err != nil {
        os.Remove(localFileName)
        return fmt.Errorf("error closing file writer: %w", err)
    }

    log.Printf("Local parquet file created successfully: %s", localFileName)

    // Get file info
    fileInfo, err := os.Stat(localFileName)
    if err != nil {
        os.Remove(localFileName)
        return fmt.Errorf("failed to get file info: %w", err)
    }

    log.Printf("Parquet file size: %d bytes", fileInfo.Size())

    // Upload to S3
    log.Printf("Uploading to S3: %s", s3OutputKey)

    file, err := os.Open(localFileName)
    if err != nil {
        os.Remove(localFileName)
        return fmt.Errorf("failed to open temp file for upload: %w", err)
    }
    defer file.Close()

    // Upload with progress tracking
    result, err := etl.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
        Bucket: aws.String(etl.targetBucket),
        Key:    aws.String(s3OutputKey),
        Body:   file,
        Metadata: map[string]*string{
            "record-count": aws.String(strconv.Itoa(len(records))),
            "source-file":  aws.String(records[0].Source),
        },
    })

    if err != nil {
        os.Remove(localFileName)
        return fmt.Errorf("failed to upload to S3: %w", err)
    }

    log.Printf("Successfully uploaded to S3: %s (Location: %s)", s3OutputKey, result.Location)

    // Clean up local file
    if err := os.Remove(localFileName); err != nil {
        log.Printf("Warning: Failed to remove temp file %s: %v", localFileName, err)
    }

    // Verify upload
    _, err = etl.s3Client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
        Bucket: aws.String(etl.targetBucket),
        Key:    aws.String(s3OutputKey),
    })
    if err != nil {
        log.Printf("Warning: Upload verification failed for %s: %v", s3OutputKey, err)
        return fmt.Errorf("upload verification failed: %w", err)
    }

    log.Printf("Upload verified successfully: %s", s3OutputKey)
    return nil
}

func (etl *ETLPipeline) ProcessAllFiles(ctx context.Context) error {
    log.Println("Starting ETL pipeline...")
    start := time.Now()

    // Test S3 access first
    if err := etl.testS3Access(); err != nil {
        return fmt.Errorf("S3 access test failed: %w", err)
    }

    // List all CSV files
    log.Printf("Listing files in bucket: %s", etl.sourceBucket)

    var allKeys []string
    err := etl.s3Client.ListObjectsPagesWithContext(ctx,
        &s3.ListObjectsInput{Bucket: aws.String(etl.sourceBucket)},
        func(page *s3.ListObjectsOutput, lastPage bool) bool {
            for _, obj := range page.Contents {
                if strings.HasSuffix(*obj.Key, ".csv") {
                    allKeys = append(allKeys, *obj.Key)
                }
            }
            return !lastPage
        })
    if err != nil {
        return fmt.Errorf("failed to list objects: %w", err)
    }

    log.Printf("Found %d CSV files to process", len(allKeys))

    if len(allKeys) == 0 {
        log.Println("No CSV files found in source bucket")
        return nil
    }

    // Process files concurrently
    semaphore := make(chan struct{}, etl.workerCount)
    var wg sync.WaitGroup
    var mu sync.Mutex
    processed := 0
    failed := 0
    var totalRowsProcessed int64
    var totalBytesProcessed int64

    for _, key := range allKeys {
        wg.Add(1)
        go func(k string) {
            defer wg.Done()
            semaphore <- struct{}{}        // Acquire
            defer func() { <-semaphore }() // Release

            rows, bytes, err := etl.processCSVFile(ctx, k)
            mu.Lock()
            totalBytesProcessed += bytes
            mu.Unlock()

            if err != nil {
                log.Printf("ERROR processing %s: %s", k, err)
                mu.Lock()
                failed++
                mu.Unlock()
            } else {
                mu.Lock()
                processed++
                totalRowsProcessed += int64(rows)
                if processed%10 == 0 || processed <= 5 {
                    log.Printf("Progress: %d processed, %d failed, %d remaining",
                              processed, failed, len(allKeys)-processed-failed)
                }
                mu.Unlock()
            }
        }(key)
    }

    wg.Wait()

    duration := time.Since(start)
    log.Printf("ETL pipeline completed in %v", duration)
    log.Printf("Final stats: %d processed, %d failed, %d total", processed, failed, len(allKeys))

    if failed > 0 {
        log.Printf("Warning: %d files failed to process", failed)
    }

    // Create and write stats
    stats := ETLStats{
        TotalExecutionTime:      duration.String(),
        TotalFilesFound:         len(allKeys),
        FilesProcessed:          processed,
        FilesFailed:             failed,
        TotalRowsProcessed:      totalRowsProcessed,
        TotalBytesProcessed:     totalBytesProcessed,
    }
    if duration.Seconds() > 0 {
        stats.ProcessingThroughputGBs = float64(totalBytesProcessed) / 1e9 / duration.Seconds()
    }

    statsJSON, err := json.MarshalIndent(stats, "", "  ")
    if err != nil {
        log.Printf("Warning: Failed to serialize stats: %v", err)
    } else if err := os.WriteFile("etl_stats.json", statsJSON, 0644); err != nil {
        log.Printf("Warning: Failed to write stats file: %v", err)
    } else {
        log.Println("Successfully wrote stats to etl_stats.json")
    }

    return nil
}

// Cleanup function
func (etl *ETLPipeline) Cleanup() {
    log.Printf("Cleaning up temp directory: %s", etl.tempDir)
    if err := os.RemoveAll(etl.tempDir); err != nil {
        log.Printf("Warning: Failed to clean up temp directory: %v", err)
    }
}

func main() {
    if len(os.Args) != 3 {
        log.Fatal("Usage: go run main.go <source-bucket> <target-bucket>")
    }

    sourceBucket := os.Args[1]
    targetBucket := os.Args[2]

    log.Printf("Starting ETL Pipeline")
    log.Printf("Source bucket: %s", sourceBucket)
    log.Printf("Target bucket: %s", targetBucket)
    log.Printf("Worker count: %d", runtime.NumCPU()*2)

    pipeline := NewETLPipeline(sourceBucket, targetBucket)
    defer pipeline.Cleanup()

    ctx, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
    defer cancel()

    if err := pipeline.ProcessAllFiles(ctx); err != nil {
        log.Fatalf("Pipeline failed: %s", err)
    }

    log.Println("ETL pipeline completed successfully!")
}
