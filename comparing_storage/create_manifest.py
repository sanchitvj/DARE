"""
Scans a source S3 bucket for CSV files and generates a manifest file.

This script addresses the challenge of an inconsistent and messy directory
structure in a source S3 bucket. It automatically discovers top-level 
"data_*" directories, finds all '.csv' files within them, and intelligently 
extracts 'year' and 'quarter' metadata from their file paths.

How it works:
1.  It first lists all top-level directories in the bucket that match a
    given scan prefix (e.g., "data_").
2.  It uses the AWS Boto3 library to efficiently list all objects under
    each discovered directory, handling pagination for very large numbers of files.
3.  For each file, it attempts to extract the year and quarter using a
    series of regular expression patterns designed to match the varied
    directory structures (e.g., 'data_Q1_2016/', 'data_2013/').
4.  If the quarter is not explicitly found in the path, it falls back to
    parsing the filename (assuming a 'yyyy-mm-dd.csv' format) to derive
    the quarter from the month.
5.  The final output is a clean CSV file ('manifest.csv') containing the
    full S3 path, the extracted year, and the extracted quarter for every
    source file.
6.  This manifest file is then uploaded to a specified S3 location, ready
    to be used as a predictable, structured input for a Spark ingestion job.

Usage:
    python create_manifest.py \\
        --bucket your-source-s3-bucket \\
        --output-path s3://your-processed-bucket/manifests/source_manifest.csv \\
        # Optional: --scan-prefix your_prefix_

"""
import argparse
import boto3
import re
import csv
import io

def parse_arguments():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description="Create a manifest file from S3 source data.")
    parser.add_argument("--bucket", required=True, help="The source S3 bucket name.")
    parser.add_argument("--scan-prefix", default="data_", help="The base prefix to scan for top-level data directories (default: 'data_').")
    parser.add_argument("--output-path", required=True, help="The full S3 path to save the output manifest CSV file (e.g., s3://bucket/path/manifest.csv).")
    args = parser.parse_args()
    return args

def extract_metadata_from_path(s3_path):
    """
    Extracts year and quarter from a given S3 file path.
    Handles various known path formats and derives quarter from filename if needed.
    """
    year, quarter = None, None

    # Pattern 1: Look for '.../data_Qx_YYYY/...'
    match_q_year = re.search(r'data_Q(\d)_(\d{4})', s3_path)
    if match_q_year:
        quarter = f"Q{match_q_year.group(1)}"
        year = match_q_year.group(2)
        return year, quarter

    # Pattern 2: Look for '.../data_YYYY/...'
    match_year_only = re.search(r'data_(\d{4})', s3_path)
    if match_year_only:
        year = match_year_only.group(1)

    # If quarter is still unknown, try to derive it from the filename (e.g., yyyy-mm-dd.csv)
    if not quarter:
        filename_match = re.search(r'(\d{4})-(\d{2})-(\d{2})\.csv', s3_path)
        if filename_match:
            if not year:  # Also grab the year from the filename if it wasn't in the path
                year = filename_match.group(1)
            month = int(filename_match.group(2))
            if 1 <= month <= 3:
                quarter = "Q1"
            elif 4 <= month <= 6:
                quarter = "Q2"
            elif 7 <= month <= 9:
                quarter = "Q3"
            elif 10 <= month <= 12:
                quarter = "Q4"

    return year, quarter


def main():
    """Main function to generate and upload the manifest."""
    args = parse_arguments()
    s3_client = boto3.client('s3')

    # 1. Automatically discover prefixes starting with the scan_prefix
    print(f"Discovering top-level directories in bucket '{args.bucket}' with prefix '{args.scan_prefix}'...")
    discovered_prefixes = []
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=args.bucket, Prefix=args.scan_prefix, Delimiter='/')
        for page in pages:
            if page.get('CommonPrefixes'):
                for prefix_info in page.get('CommonPrefixes'):
                    discovered_prefixes.append(prefix_info.get('Prefix'))
        
        if not discovered_prefixes:
            print(f"Error: No top-level directories found matching prefix '{args.scan_prefix}'.")
            print("Please check the bucket name and that your source directories start with that prefix.")
            return
        
        print(f"Successfully discovered {len(discovered_prefixes)} directories to scan.")
    except Exception as e:
        print(f"An error occurred while trying to list directories in bucket '{args.bucket}'.")
        print(f"Please check your AWS credentials and permissions. Details: {e}")
        return

    # 2. Scan each discovered prefix for CSV files
    paginator = s3_client.get_paginator('list_objects_v2')
    manifest_data = []
    
    for prefix in discovered_prefixes:
        print(f"--> Scanning prefix: {prefix}")
        page_iterator = paginator.paginate(Bucket=args.bucket, Prefix=prefix)
        file_count = 0
        for page in page_iterator:
            if "Contents" in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if key.endswith('.csv'):
                        file_count += 1
                        full_s3_path = f"s3://{args.bucket}/{key}"
                        year, quarter = extract_metadata_from_path(key)

                        if year and quarter:
                            manifest_data.append({
                                'file_path': full_s3_path,
                                'year': year,
                                'quarter': quarter
                            })
                        else:
                            print(f"    [Warning] Could not extract metadata from: {full_s3_path}")
        print(f"    Found {file_count} CSV files in this prefix.")

    if not manifest_data:
        print("\\nNo CSV files found across all discovered directories. Cannot create manifest. Exiting.")
        return

    print(f"\\nTotal files for manifest: {len(manifest_data)}")
    
    # Write to an in-memory CSV file
    csv_buffer = io.StringIO()
    fieldnames = ['file_path', 'year', 'quarter']
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(manifest_data)

    # Upload the manifest to the specified S3 path
    try:
        output_bucket, output_key = args.output_path.replace("s3://", "").split('/', 1)
        s3_client.put_object(
            Bucket=output_bucket,
            Key=output_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        print(f"Successfully generated and uploaded manifest to: {args.output_path}")
    except Exception as e:
        print(f"An error occurred while uploading the manifest to '{args.output_path}'.")
        print(f"Please check the path and your S3 write permissions. Details: {e}")


if __name__ == "__main__":
    main() 