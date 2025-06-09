#!/bin/bash

# =============================================================================
# Shell script to launch an AWS EMR cluster and run a Spark job.
#
# This script creates a transient EMR cluster, submits the Iceberg ingestion
# PySpark script as a step. The cluster remains running on success or failure
# for manual inspection.
#
# Before running:
# 1. Make sure the AWS CLI is installed and configured.
# 2. Ensure the IAM roles (Service Role, EC2 Instance Profile) exist.
# 3. Ensure the EC2 Instance Profile has permissions for CloudWatch Logs.
# 4. Upload your PySpark script and manifest file to the correct S3 paths.
# =============================================================================

# Exit script if any command fails
set -e
# Exit script if an unset variable is used
set -u

# --- Configuration ---
# Fill in these variables with your specific values.

# AWS and EMR General Config
AWS_REGION="us-east-1"
CLUSTER_NAME="Iceberg Ingestion Job"
EMR_RELEASE_LABEL="emr-7.1.0"
# EC2_KEY_PAIR="your-ec2-key-pair-name" # Optional: For SSH access. Uncomment and set if needed.

# IAM Roles (Use the exact names you created)
SERVICE_ROLE="EMR_default_role"
EC2_INSTANCE_PROFILE="ec2-instane-profile"

# EMR Cluster Hardware Config
MASTER_INSTANCE_TYPE="c6a.4xlarge"
WORKER_INSTANCE_TYPE="c6a.8xlarge"
WORKER_INSTANCE_COUNT=2

# S3 Paths & Logging
S3_BUCKET_CODE="pdb-scripts"
S3_BUCKET_WAREHOUSE="hdd-iceberg-std"
S3_BUCKET_LOGS="logs-pdb"
S3_LOG_URI="s3://${S3_BUCKET_LOGS}/emr-iceberg-exp/"
CLOUDWATCH_LOG_GROUP_NAME="/aws/emr/${CLUSTER_NAME}"

# Job-specific Arguments
PYSPARK_SCRIPT_S3_PATH="s3://${S3_BUCKET_CODE}/emr-iceberg-scripts/create_iceberg_s3.py"
MANIFEST_FILE_S3_PATH="s3://${S3_BUCKET_CODE}/manifests/hdd_manifest.csv"
WAREHOUSE_S3_PATH="s3://${S3_BUCKET_WAREHOUSE}/warehouse"
CATALOG_NAME="glue_catalog"
DB_NAME="hdd_iceberg_std"
TABLE_NAME="hard_drive_data"

# --- Build EMR Configurations as a JSON string (HEREDOC for readability) ---
read -r -d '' EMR_CONFIGS <<EOF
[
    {
        "Classification": "iceberg-defaults",
        "Properties": { "iceberg.enabled": "true" }
    },
    {
        "Classification": "emrfs-site",
        "Properties": { "fs.s3.region": "${AWS_REGION}" }
    }
]
EOF

# --- Build EMR Instance Groups JSON ---
read -r -d '' EMR_INSTANCE_GROUPS <<EOF
[
    {
        "Name": "Master-Node",
        "InstanceGroupType": "MASTER",
        "InstanceType": "${MASTER_INSTANCE_TYPE}",
        "InstanceCount": 1
    },
    {
        "Name": "Worker-Nodes",
        "InstanceGroupType": "CORE",
        "InstanceType": "${WORKER_INSTANCE_TYPE}",
        "InstanceCount": ${WORKER_INSTANCE_COUNT}
    }
]
EOF

# --- Build EMR Steps JSON ---
read -r -d '' EMR_STEPS <<EOF
[
    {
        "Name": "Run Iceberg Spark Ingestion Job",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "Type": "CUSTOM_JAR",
        "Jar": "command-runner.jar",
        "Args": [
            "spark-submit",
            "--deploy-mode", "cluster",
            "'"${PYSPARK_SCRIPT_S3_PATH}"'",
            "--warehouse-path", "'"${WAREHOUSE_S3_PATH}"'",
            "--manifest-path", "'"${MANIFEST_FILE_S3_PATH}"'",
            "--catalog-name", "'"${CATALOG_NAME}"'",
            "--db-name", "'"${DB_NAME}"'",
            "--table-name", "'"${TABLE_NAME}"'"
        ]
    }
]
EOF

# --- Prepare EC2 Attributes ---
# This logic handles the optional EC2_KEY_PAIR
if [ -v EC2_KEY_PAIR ] && [ -n "${EC2_KEY_PAIR}" ]; then
    EC2_ATTRIBUTES="InstanceProfile=${EC2_INSTANCE_PROFILE},KeyName=${EC2_KEY_PAIR}"
else
    EC2_ATTRIBUTES="InstanceProfile=${EC2_INSTANCE_PROFILE}"
fi

# --- EMR Launch Command ---
echo "Launching EMR cluster '${CLUSTER_NAME}' in region ${AWS_REGION}..."
echo "Cluster will remain running after job completion or failure."
echo "Logs will be archived to: ${S3_LOG_URI}"
echo "Real-time logs will be available in CloudWatch."

aws emr create-cluster \
--name "${CLUSTER_NAME}" \
--release-label "${EMR_RELEASE_LABEL}" \
--applications Name=Spark Name=AmazonCloudWatchAgent \
--service-role "${SERVICE_ROLE}" \
--region "${AWS_REGION}" \
--log-uri "${S3_LOG_URI}" \
--configurations "${EMR_CONFIGS}" \
--ec2-attributes "${EC2_ATTRIBUTES}" \
--instance-groups "${EMR_INSTANCE_GROUPS}" \
--steps "${EMR_STEPS}"

echo "Cluster creation command has been submitted."
echo "You can monitor the progress in the AWS EMR console."
echo "The cluster ID will be printed below. Use it to check status or terminate." 
