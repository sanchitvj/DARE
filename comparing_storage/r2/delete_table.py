import os
import requests
from dotenv import load_dotenv
from pyiceberg.catalog.rest import RestCatalog

load_dotenv()

# Load configuration from environment variables
catalog_uri = os.getenv("R2_CATALOG_URI")
warehouse = os.getenv("R2_WAREHOUSE") 
token = os.getenv("R2_CATALOG_API_TOKEN")
catalog_name = os.getenv("R2_CATALOG_NAME", "r2_catalog")

# Validate required environment variables
required_vars = ["R2_CATALOG_URI", "R2_WAREHOUSE", "R2_CATALOG_API_TOKEN"]
missing_vars = [var for var in required_vars if not os.getenv(var)]

if missing_vars:
    print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
    exit(1)

print("Connecting to R2 Data Catalog...")

try:
    catalog = RestCatalog(
        name=catalog_name,
        warehouse=warehouse,
        uri=catalog_uri,
        token=token,
    )
    
    print("✅ Successfully connected to catalog")
    
    # List all namespaces
    print("\nNamespaces:")
    namespaces = catalog.list_namespaces()
    for ns in namespaces:
        print(f"  - {ns}")

    # List tables in each namespace
    print("\nTables:")
    all_tables = []
    for namespace in namespaces:
        tables = catalog.list_tables(namespace)
        print(f"  Namespace '{namespace}':")
        for table in tables:
            print(f"    - {table}")
            all_tables.append((namespace, table))
        
        # Add actual deletion commands (uncomment ONE to delete that table)
        catalog.drop_table('hdd_iceberg_std.hard_drive_data')    # Table 1
        # catalog.drop_table('hdd_iceberg_r2.hard_drive_data')     # Table 2  
        catalog.drop_table('default.hard_drive_data')           # Table 3
        # catalog.drop_table('default.hdd_iceberg_r2')            # Table 4
        catalog.drop_table('hdd_r2.hard_drive_data')            # Table 5
        
        print("✅ Table deleted successfully")
        
    else:
        print("No tables found in the catalog")

except Exception as e:
    print(f"❌ Error: {e}")
    
    # Fall back to direct REST API if PyIceberg fails
    print("\nFalling back to direct REST API...")
    try:
        response = requests.get(
            f"{catalog_uri}/v1/namespaces",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            print("Namespaces (via REST API):", data)
            
            # List tables using REST API
            if 'namespaces' in data:
                for ns_list in data['namespaces']:
                    ns = '.'.join(ns_list) if isinstance(ns_list, list) else str(ns_list)
                    tables_resp = requests.get(
                        f"{catalog_uri}/v1/namespaces/{ns}/tables",
                        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                        timeout=30
                    )
                    if tables_resp.status_code == 200:
                        tables_data = tables_resp.json()
                        print(f"Tables in {ns}:", tables_data)
        else:
            print(f"REST API failed: {response.status_code}")
            
    except Exception as rest_error:
        print(f"REST API error: {rest_error}")
