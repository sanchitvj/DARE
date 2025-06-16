#!/bin/bash
# Download Iceberg JAR files for Spark

set -e

echo "Downloading Iceberg JAR files..."

# Create jars directory
mkdir -p jars

# Iceberg version and Spark version
ICEBERG_VERSION="1.6.1"
SPARK_VERSION="3.5"
HADOOP_VERSION="3.3.4"

# Download Iceberg Spark runtime JAR
echo "Downloading iceberg-spark-runtime..."
wget -O jars/iceberg-spark-runtime-${SPARK_VERSION}_2.12-${ICEBERG_VERSION}.jar \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_VERSION}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_VERSION}_2.12-${ICEBERG_VERSION}.jar"

# Download AWS SDK bundle (for S3)
echo "Downloading AWS SDK bundle..."
wget -O jars/bundle-2.20.18.jar \
    "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.18/bundle-2.20.18.jar"

# Download URL connection client
echo "Downloading URL connection client..."
wget -O jars/url-connection-client-2.20.18.jar \
    "https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.20.18/url-connection-client-2.20.18.jar"

# Download Hadoop AWS JAR (REQUIRED for S3 filesystem support)
echo "Downloading Hadoop AWS JAR..."
wget -O jars/hadoop-aws-${HADOOP_VERSION}.jar \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar"

# Download AWS SDK v1 JAR (REQUIRED by hadoop-aws)
echo "Downloading AWS SDK v1 JAR..."
wget -O jars/aws-java-sdk-bundle-1.12.262.jar \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"

echo "JAR files downloaded successfully!"
echo "JAR files are in the 'jars' directory:"
ls -la jars/

echo ""
echo "Usage: Add these JARs to your Spark session with:"
echo "spark = SparkSession.builder \\"
echo "  .config('spark.jars', 'jars/iceberg-spark-runtime-${SPARK_VERSION}_2.12-${ICEBERG_VERSION}.jar,jars/bundle-2.20.18.jar,jars/url-connection-client-2.20.18.jar,jars/hadoop-aws-${HADOOP_VERSION}.jar,jars/aws-java-sdk-bundle-1.12.262.jar') \\"
echo "  .getOrCreate()" 