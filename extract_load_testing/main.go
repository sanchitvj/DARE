package main

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/csv"
)

func (etl *ETLPipeline) processCSVFile(ctx context.Context, key string) (int, int64, error) {
	log.Printf("Processing file: %s", key)

	// Extract date and metadata
	date, quarterStr, year, _, month, err := extractDateAndMetadata(key)
	if err != nil {
		log.Printf("Skipping file %s: %s", key, err)
		return 0, 0, nil // Return 0 rows and bytes, but no error to indicate skipping
	}

	log.Printf("Extracted: date=%s, year=%d, quarter=%s, month=%d", date, year, quarterStr, month)

	// Download file from S3 as a stream, which is more memory-efficient.
	log.Printf("Streaming %s from S3...", key)
	s3Object, err := etl.s3Client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(etl.sourceBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, 0, fmt.Errorf("failed to start download stream for %s: %w", key, err)
	}
	defer s3Object.Body.Close()

	// Use ContentLength for accurate byte count without reading all into memory first.
	bytesProcessed := *s3Object.ContentLength

	log.Printf("Downloaded %d bytes, parsing CSV from stream...", bytesProcessed)

	// Parse CSV directly from the S3 object stream.
	reader := csv.NewReader(s3Object.Body)
	reader.FieldsPerRecord = -1 // Allow variable number of fields

	var records []ParquetRecord
	rowNum := 0

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, bytesProcessed, fmt.Errorf("error reading CSV record: %w", err)
		}
		rowNum++
		records = append(records, ParquetRecord{
			Date:    date,
			Quarter: quarterStr,
			Year:    year,
			Month:   month,
			Row:     record,
		})
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
	// ... existing code ...
	return nil
}

func extractDateAndMetadata(key string) (string, string, int, string, int, error) {
	// ... existing code ...
	return "", "", 0, "", 0, fmt.Errorf("unable to parse date from filename '%s' or path '%s'", filename, filePath)
} 