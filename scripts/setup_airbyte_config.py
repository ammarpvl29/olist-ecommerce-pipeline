"""
Manual steps in Airbyte UI (http://localhost:8000):

1. Create Source (File/CSV):
   - Name: Olist CSV Files
   - Storage: Local Filesystem
   - Path: /data/raw/ (mount this in Airbyte if needed)
   
2. Create Destination (PostgreSQL):
   - Host: host.docker.internal (or your machine's IP)
   - Port: 5432
   - Database: olist_analytics
   - Schema: raw_data
   - Username: olist_user
   - Password: olist_pass123
   
3. Create Connection:
   - Source: Olist CSV Files
   - Destination: PostgreSQL
   - Sync Mode: Full Refresh - Overwrite
   - Schedule: Manual (or set as needed)
"""

import json
from pathlib import Path

def generate_file_config():
    """Generate configuration for CSV files"""
    
    csv_files = [
        "olist_customers_dataset.csv",
        "olist_geolocation_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_order_payments_dataset.csv",
        "olist_order_reviews_dataset.csv",
        "olist_orders_dataset.csv",
        "olist_products_dataset.csv",
        "olist_sellers_dataset.csv",
        "product_category_name_translation.csv"
    ]
    
    config = {
        "files": [],
        "target_schema": "raw_data"
    }
    
    for file in csv_files:
        table_name = file.replace(".csv", "")
        config["files"].append({
            "file": f"data/raw/{file}",
            "table": table_name,
            "format": "csv",
            "header": True
        })
    
    with open("configs/airbyte_source_config.json", "w") as f:
        json.dump(config, f, indent=2)
    
    print("Configuration generated: configs/airbyte_source_config.json")

if __name__ == "__main__":
    generate_file_config()