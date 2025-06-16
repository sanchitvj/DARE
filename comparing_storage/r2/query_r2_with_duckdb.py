"""
Query R2 Iceberg Data with DuckDB

DuckDB has excellent Iceberg support and is much simpler than PyIceberg.
This script demonstrates how to query your R2 Iceberg data using DuckDB.
"""

import os
import duckdb
from dotenv import load_dotenv

load_dotenv()

def setup_duckdb():
    """Setup DuckDB with R2 configuration."""
    
    # Create DuckDB connection
    con = duckdb.connect(':memory:')
    
    # Install and load required extensions
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute("INSTALL iceberg")
    con.execute("LOAD iceberg")
    
    # Configure S3/R2 settings
    con.execute(f"""
        SET s3_endpoint='{os.getenv('CLOUDFLARE_ACCOUNT_ID')}.r2.cloudflarestorage.com';
        SET s3_access_key_id='{os.getenv('R2_ACCESS_KEY_ID')}';
        SET s3_secret_access_key='{os.getenv('R2_SECRET_ACCESS_KEY')}';
        SET s3_use_ssl=true;
        SET s3_url_style='path';
    """)
    
    print("✅ DuckDB configured with R2 settings!")
    return con

def query_iceberg_data(con):
    """Query the Iceberg data in R2."""
    
    print("\n" + "="*60)
    print("📊 QUERYING R2 ICEBERG DATA WITH DUCKDB")
    print("="*60)
    
    # Table path based on your directory structure
    table_path = "s3://hdd-iceberg-r2/iceberg/hdd_iceberg_r2/hard_drive_data"
    
    try:
        # 1. Basic table info
        print("\n1️⃣ TABLE INFORMATION:")
        print("-" * 30)
        
        # Get table schema
        schema_query = f"DESCRIBE SELECT * FROM iceberg_scan('{table_path}') LIMIT 0"
        schema_result = con.execute(schema_query).fetchall()
        
        print(f"📋 Table schema ({len(schema_result)} columns):")
        for i, row in enumerate(schema_result, 1):
            # DuckDB DESCRIBE returns (column_name, column_type, null, key, default, extra)
            col_name = row[0]
            col_type = row[1]
            print(f"  {i:2d}. {col_name}: {col_type}")
        
        # 2. Record count
        print("\n2️⃣ RECORD COUNT:")
        print("-" * 30)
        count_query = f"SELECT COUNT(*) as total_records FROM iceberg_scan('{table_path}')"
        count_result = con.execute(count_query).fetchone()
        total_records = count_result[0]
        print(f"Total records: {total_records:,}")
        
        # 3. Sample data
        print("\n3️⃣ SAMPLE DATA:")
        print("-" * 30)
        sample_query = f"""
            SELECT date, serial_number, model, failure, year, quarter 
            FROM iceberg_scan('{table_path}') 
            LIMIT 5
        """
        sample_result = con.execute(sample_query).fetchall()
        
        if sample_result:
            print("Sample records:")
            for row in sample_result:
                print(f"  {row}")
        
        # 4. Data by year
        print("\n4️⃣ DATA BY YEAR:")
        print("-" * 30)
        year_query = f"""
            SELECT year, COUNT(*) as record_count 
            FROM iceberg_scan('{table_path}') 
            GROUP BY year 
            ORDER BY year
        """
        year_result = con.execute(year_query).fetchall()
        
        for year, count in year_result:
            print(f"  {year}: {count:,} records")
        
        # 5. Failure analysis
        print("\n5️⃣ FAILURE ANALYSIS:")
        print("-" * 30)
        failure_query = f"""
            SELECT 
                year,
                COUNT(*) as total_records,
                SUM(CASE WHEN failure = true THEN 1 ELSE 0 END) as failures,
                ROUND(SUM(CASE WHEN failure = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as failure_rate_pct
            FROM iceberg_scan('{table_path}')
            GROUP BY year
            ORDER BY year
        """
        failure_result = con.execute(failure_query).fetchall()
        
        print("Failures by year:")
        for year, total, failures, rate in failure_result:
            print(f"  {year}: {failures:,} failures out of {total:,} ({rate}%)")
        
        # 6. Top models
        print("\n6️⃣ TOP 10 MODELS:")
        print("-" * 30)
        model_query = f"""
            SELECT model, COUNT(*) as count
            FROM iceberg_scan('{table_path}')
            GROUP BY model
            ORDER BY count DESC
            LIMIT 10
        """
        model_result = con.execute(model_query).fetchall()
        
        for model, count in model_result:
            print(f"  {model}: {count:,} records")
        
        # 7. Partition breakdown
        print("\n7️⃣ PARTITION BREAKDOWN:")
        print("-" * 30)
        partition_query = f"""
            SELECT year, quarter, COUNT(*) as records
            FROM iceberg_scan('{table_path}')
            GROUP BY year, quarter
            ORDER BY year, quarter
        """
        partition_result = con.execute(partition_query).fetchall()
        
        print("Records by year and quarter:")
        for year, quarter, count in partition_result:
            print(f"  {year} Q{quarter}: {count:,} records")
        
        # 8. Advanced analytics
        print("\n8️⃣ ADVANCED ANALYTICS:")
        print("-" * 30)
        
        # Failure rate by model (top models only)
        advanced_query = f"""
            WITH model_stats AS (
                SELECT model,
                       COUNT(*) as total_drives,
                       SUM(CASE WHEN failure = true THEN 1 ELSE 0 END) as failures,
                       ROUND(SUM(CASE WHEN failure = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as failure_rate_pct
                FROM iceberg_scan('{table_path}')
                GROUP BY model
                HAVING COUNT(*) >= 1000  -- Only models with significant data
            )
            SELECT model, total_drives, failures, failure_rate_pct
            FROM model_stats
            ORDER BY total_drives DESC
            LIMIT 5
        """
        advanced_result = con.execute(advanced_query).fetchall()
        
        print("Failure rates for top models:")
        for model, total, failures, rate in advanced_result:
            print(f"  {model}: {failures:,}/{total:,} ({rate}%)")
        
        return True
        
    except Exception as e:
        print(f"❌ Query failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def interactive_queries(con):
    """Interactive query mode."""
    
    table_path = "s3://hdd-iceberg-r2/iceberg/hdd_iceberg_r2/hard_drive_data"
    
    print("\n Interactive DuckDB Query Mode")
    print("="*50)
    print("Available commands:")
    print("  count - Get total record count")
    print("  years - Show records by year")
    print("  failures - Show failure analysis")
    print("  models - Show top models")
    print("  sample - Show sample data")
    print("  sql <query> - Run custom SQL")
    print("  quit - Exit")
    print("-" * 50)
    
    while True:
        try:
            command = input("\n🔍 DuckDB> ").strip()
            
            if command == "quit":
                break
            elif command == "count":
                result = con.execute(f"SELECT COUNT(*) FROM iceberg_scan('{table_path}')").fetchone()
                print(f"Total records: {result[0]:,}")
            elif command == "years":
                result = con.execute(f"SELECT year, COUNT(*) FROM iceberg_scan('{table_path}') GROUP BY year ORDER BY year").fetchall()
                for year, count in result:
                    print(f"  {year}: {count:,}")
            elif command == "failures":
                result = con.execute(f"SELECT SUM(CASE WHEN failure = true THEN 1 ELSE 0 END) as failures, COUNT(*) as total FROM iceberg_scan('{table_path}')").fetchone()
                failures, total = result
                rate = (failures / total) * 100
                print(f"Failures: {failures:,}/{total:,} ({rate:.2f}%)")
            elif command == "models":
                result = con.execute(f"SELECT model, COUNT(*) FROM iceberg_scan('{table_path}') GROUP BY model ORDER BY COUNT(*) DESC LIMIT 10").fetchall()
                for model, count in result:
                    print(f"  {model}: {count:,}")
            elif command == "sample":
                result = con.execute(f"SELECT date, serial_number, model, failure FROM iceberg_scan('{table_path}') LIMIT 5").fetchall()
                for row in result:
                    print(f"  {row}")
            elif command.startswith("sql "):
                sql = command[4:].strip()
                try:
                    result = con.execute(sql).fetchall()
                    for row in result:
                        print(f"  {row}")
                except Exception as e:
                    print(f"❌ SQL Error: {e}")
            else:
                print("❌ Unknown command. Type 'quit' to exit.")
                
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"❌ Error: {e}")
    
    print("\n👋 Goodbye!")

def main():
    print("🚀 DuckDB R2 Iceberg Query")
    print("="*40)
    
    # Check environment variables
    required_vars = ['R2_ACCESS_KEY_ID', 'R2_SECRET_ACCESS_KEY', 'CLOUDFLARE_ACCOUNT_ID']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        print(f"❌ Missing environment variables: {missing_vars}")
        return
    
    try:
        # Setup DuckDB
        con = setup_duckdb()
        
        # Run demo queries
        success = query_iceberg_data(con)
        
        if success:
            print("\n🎉 Demo completed successfully!")
            
            # Ask if user wants interactive mode
            response = input("\n🤔 Want to try interactive queries? (y/n): ").strip().lower()
            if response in ['y', 'yes']:
                interactive_queries(con)
        else:
            print("\n❌ Demo failed!")
            
    except Exception as e:
        print(f"❌ Script failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'con' in locals():
            con.close()

if __name__ == "__main__":
    main() 