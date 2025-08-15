import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, Integer, Float, DateTime, Boolean, ForeignKey, Index
from sqlalchemy.schema import CreateSchema
from pathlib import Path
import sys
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_loading.log'),
        logging.StreamHandler()
    ]
)

class OlistDataLoader:
    def __init__(self):
        self.engine = None
        self.data_path = Path(__file__).parent.parent / "data" / "raw"
        self.metadata = MetaData()
        
        # Define table configurations with proper data types
        self.table_configs = {
            "olist_customers_dataset.csv": {
                "table_name": "customers",
                "schema": "raw_data",
                "dtypes": {
                    "customer_id": "string",
                    "customer_unique_id": "string", 
                    "customer_zip_code_prefix": "string",  # Keep as string to preserve leading zeros
                    "customer_city": "string",
                    "customer_state": "string"
                },
                "date_columns": [],
                "primary_key": "customer_id",
                "indexes": ["customer_unique_id", "customer_state", "customer_zip_code_prefix"]
            },
            
            "olist_geolocation_dataset.csv": {
                "table_name": "geolocation",
                "schema": "raw_data",
                "dtypes": {
                    "geolocation_zip_code_prefix": "string",  # Keep as string
                    "geolocation_lat": "float",
                    "geolocation_lng": "float",
                    "geolocation_city": "string",
                    "geolocation_state": "string"
                },
                "date_columns": [],
                "primary_key": None,  # No unique identifier
                "indexes": ["geolocation_zip_code_prefix", "geolocation_state"]
            },
            
            "olist_orders_dataset.csv": {
                "table_name": "orders",
                "schema": "raw_data",
                "dtypes": {
                    "order_id": "string",
                    "customer_id": "string",
                    "order_status": "string",
                    "order_purchase_timestamp": "datetime",
                    "order_approved_at": "datetime",
                    "order_delivered_carrier_date": "datetime",
                    "order_delivered_customer_date": "datetime",
                    "order_estimated_delivery_date": "datetime"
                },
                "date_columns": [
                    "order_purchase_timestamp",
                    "order_approved_at", 
                    "order_delivered_carrier_date",
                    "order_delivered_customer_date",
                    "order_estimated_delivery_date"
                ],
                "primary_key": "order_id",
                "indexes": ["customer_id", "order_status", "order_purchase_timestamp"]
            },
            
            "olist_order_items_dataset.csv": {
                "table_name": "order_items",
                "schema": "raw_data",
                "dtypes": {
                    "order_id": "string",
                    "order_item_id": "int",
                    "product_id": "string",
                    "seller_id": "string",
                    "shipping_limit_date": "datetime",
                    "price": "float",
                    "freight_value": "float"
                },
                "date_columns": ["shipping_limit_date"],
                "primary_key": None,  # Composite key: order_id + order_item_id
                "indexes": ["order_id", "product_id", "seller_id"]
            },
            
            "olist_order_payments_dataset.csv": {
                "table_name": "order_payments",
                "schema": "raw_data",
                "dtypes": {
                    "order_id": "string",
                    "payment_sequential": "int",
                    "payment_type": "string",
                    "payment_installments": "int",
                    "payment_value": "float"
                },
                "date_columns": [],
                "primary_key": None,  # Composite key: order_id + payment_sequential
                "indexes": ["order_id", "payment_type"]
            },
            
            "olist_order_reviews_dataset.csv": {
                "table_name": "order_reviews",
                "schema": "raw_data",
                "dtypes": {
                    "review_id": "string",
                    "order_id": "string",
                    "review_score": "int",
                    "review_comment_title": "string",
                    "review_comment_message": "string",
                    "review_creation_date": "datetime",
                    "review_answer_timestamp": "datetime"
                },
                "date_columns": ["review_creation_date", "review_answer_timestamp"],
                "primary_key": "review_id",
                "indexes": ["order_id", "review_score", "review_creation_date"]
            },
            
            "olist_products_dataset.csv": {
                "table_name": "products",
                "schema": "raw_data",
                "dtypes": {
                    "product_id": "string",
                    "product_category_name": "string",
                    "product_name_lenght": "float",  # Note: typo in original column name
                    "product_description_lenght": "float",  # Note: typo in original column name
                    "product_photos_qty": "float",
                    "product_weight_g": "float",
                    "product_length_cm": "float",
                    "product_height_cm": "float",
                    "product_width_cm": "float"
                },
                "date_columns": [],
                "primary_key": "product_id",
                "indexes": ["product_category_name"]
            },
            
            "olist_sellers_dataset.csv": {
                "table_name": "sellers",
                "schema": "raw_data",
                "dtypes": {
                    "seller_id": "string",
                    "seller_zip_code_prefix": "string",  # Keep as string
                    "seller_city": "string",
                    "seller_state": "string"
                },
                "date_columns": [],
                "primary_key": "seller_id",
                "indexes": ["seller_state", "seller_zip_code_prefix"]
            },
            
            "product_category_name_translation.csv": {
                "table_name": "product_category_translation",
                "schema": "raw_data",
                "dtypes": {
                    "product_category_name": "string",
                    "product_category_name_english": "string"
                },
                "date_columns": [],
                "primary_key": "product_category_name",
                "indexes": []
            }
        }
    
    def connect_database(self):
        """Establish database connection"""
        import os
        
        try:
            # Database connection parameters - Updated to use port 5433
            db_host = os.getenv('DB_HOST', '127.0.0.1')
            db_port = os.getenv('DB_PORT', '5433')  # Changed from 5432 to 5433
            db_name = os.getenv('DB_NAME', 'olist_analytics')
            db_user = os.getenv('DB_USER', 'olist_user')
            db_password = os.getenv('DB_PASSWORD', 'olist_pass123')
            
            connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
            
            logging.info(f"Attempting to connect to: postgresql://{db_user}:***@{db_host}:{db_port}/{db_name}")
            
            self.engine = create_engine(
                connection_string,
                echo=False,
                pool_pre_ping=True,
                connect_args={
                    "connect_timeout": 10
                }
            )
            
            # Test the connection
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT current_user, current_database()"))
                user_db = result.fetchone()
                logging.info(f"✓ Connected as user: {user_db[0]}, database: {user_db[1]}")
                return True
                
        except Exception as e:
            logging.error(f"✗ Connection failed: {e}")
            
            # Try to give more specific error information
            if "password authentication failed" in str(e):
                logging.error("  This is a password authentication error.")
                logging.error(f"  Make sure the PostgreSQL user '{db_user}' exists with password '{db_password}'")
            elif "Connection refused" in str(e):
                logging.error("  PostgreSQL server is not accepting connections.")
                logging.error("  Make sure Docker container is running on port 5433: docker-compose up -d")
            
            return False
    
    def create_schemas(self):
        """Create necessary schemas if they don't exist"""
        try:
            with self.engine.connect() as conn:
                # Create schemas
                for schema in ['raw_data', 'staging', 'analytics']:
                    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
                    conn.execute(text(f"GRANT ALL ON SCHEMA {schema} TO olist_user"))
                conn.commit()
                
                logging.info("✓ Schemas created/verified")
                return True
        except Exception as e:
            logging.error(f"✗ Error creating schemas: {e}")
            return False
    
    def load_csv_files(self):
        """Load all CSV files to PostgreSQL with proper data types"""
        
        successful_loads = []
        failed_loads = []
        
        for csv_file, config in self.table_configs.items():
            file_path = self.data_path / csv_file
            
            if not file_path.exists():
                logging.warning(f"⚠️  File not found: {csv_file}")
                failed_loads.append(csv_file)
                continue
            
            logging.info(f"\n{'='*60}")
            logging.info(f"📊 Loading {csv_file}")
            logging.info(f"{'='*60}")
            
            try:
                # Read CSV with proper date parsing
                df = pd.read_csv(
                    file_path,
                    parse_dates=config["date_columns"] if config["date_columns"] else False,
                    low_memory=False
                )
                
                logging.info(f"  Rows: {len(df):,}")
                logging.info(f"  Columns: {', '.join(df.columns[:5])}" + 
                            (f"... ({len(df.columns)} total)" if len(df.columns) > 5 else ""))
                
                # Data cleaning and type conversion
                for col, dtype in config["dtypes"].items():
                    if col in df.columns:
                        if dtype == "string":
                            df[col] = df[col].astype(str).replace('nan', None)
                        elif dtype == "int":
                            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                        elif dtype == "float":
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                        elif dtype == "datetime":
                            if col not in config["date_columns"]:  # If not already parsed
                                df[col] = pd.to_datetime(df[col], errors='coerce')
                
                # Handle missing values
                null_counts = df.isnull().sum()
                if null_counts.any():
                    logging.info("  Null values found:")
                    for col, count in null_counts[null_counts > 0].items():
                        pct = (count / len(df)) * 100
                        logging.info(f"    - {col}: {count:,} ({pct:.1f}%)")
                
                # Load to PostgreSQL
                df.to_sql(
                    name=config["table_name"],
                    con=self.engine,
                    schema=config["schema"],
                    if_exists='replace',
                    index=False,
                    chunksize=5000,
                    method='multi'
                )
                
                # Create indexes
                if config["indexes"]:
                    with self.engine.connect() as conn:
                        for index_col in config["indexes"]:
                            if index_col in df.columns:
                                index_name = f"idx_{config['table_name']}_{index_col}"
                                conn.execute(text(f"""
                                    CREATE INDEX IF NOT EXISTS {index_name} 
                                    ON {config['schema']}.{config['table_name']} ({index_col})
                                """))
                        conn.commit()
                
                # Verify load
                with self.engine.connect() as conn:
                    result = conn.execute(
                        text(f"SELECT COUNT(*) FROM {config['schema']}.{config['table_name']}")
                    )
                    count = result.scalar()
                    logging.info(f"  ✓ Loaded {count:,} rows to {config['schema']}.{config['table_name']}")
                
                successful_loads.append(config["table_name"])
                
            except Exception as e:
                logging.error(f"  ✗ Error loading {csv_file}: {e}")
                failed_loads.append(csv_file)
        
        # Summary
        logging.info("\n" + "="*60)
        logging.info("📋 LOADING SUMMARY")
        logging.info("="*60)
        logging.info(f"✓ Successfully loaded: {len(successful_loads)} tables")
        if successful_loads:
            for table in successful_loads:
                logging.info(f"   - {table}")
        
        if failed_loads:
            logging.warning(f"\n✗ Failed to load: {len(failed_loads)} files")
            for file in failed_loads:
                logging.warning(f"   - {file}")
        
        return len(successful_loads) > 0
    
    def create_foreign_key_documentation(self):
        """Document relationships between tables"""
        
        relationship_sql = """
        -- Document table relationships (not enforced due to data quality)
        
        COMMENT ON TABLE raw_data.orders IS 
            'Core orders table - central fact table connecting customers, payments, reviews, and items';
        
        COMMENT ON TABLE raw_data.order_items IS 
            'Order line items - each row is a product in an order (order_id -> orders, product_id -> products, seller_id -> sellers)';
        
        COMMENT ON TABLE raw_data.customers IS 
            'Customer dimension - unique customer_id per order, customer_unique_id identifies repeat customers';
        
        COMMENT ON TABLE raw_data.sellers IS 
            'Seller dimension - marketplace sellers fulfilling orders';
        
        COMMENT ON TABLE raw_data.products IS 
            'Product dimension - catalog of products sold on platform';
        
        COMMENT ON TABLE raw_data.order_reviews IS 
            'Customer reviews - one review per order (order_id -> orders)';
        
        COMMENT ON TABLE raw_data.order_payments IS 
            'Payment transactions - can have multiple payments per order (order_id -> orders)';
        
        COMMENT ON TABLE raw_data.geolocation IS 
            'Geographic coordinates for Brazilian zip codes - join on zip_code_prefix';
        
        COMMENT ON TABLE raw_data.product_category_translation IS 
            'Translation table for product categories from Portuguese to English';
        
        -- Column-level documentation for key fields
        COMMENT ON COLUMN raw_data.customers.customer_unique_id IS 
            'Identifies same customer across multiple orders - use for repeat purchase analysis';
        
        COMMENT ON COLUMN raw_data.orders.order_status IS 
            'Order lifecycle: created -> approved -> invoiced -> shipped -> delivered (or cancelled/unavailable)';
        
        COMMENT ON COLUMN raw_data.order_items.order_item_id IS 
            'Sequential number of item within the order (1, 2, 3...)';
        
        COMMENT ON COLUMN raw_data.order_payments.payment_sequential IS 
            'Sequential number for multiple payments on same order';
        """
        
        try:
            with self.engine.connect() as conn:
                for statement in relationship_sql.split(';'):
                    if statement.strip():
                        conn.execute(text(statement))
                conn.commit()
            logging.info("✓ Documentation added to tables")
        except Exception as e:
            logging.warning(f"⚠️  Documentation notes: {e}")
    
    def verify_data_quality(self):
        """Comprehensive data quality checks"""
        
        logging.info("\n" + "="*60)
        logging.info("🔍 DATA QUALITY ANALYSIS")
        logging.info("="*60)
        
        quality_checks = {
            "📊 Key Metrics": [
                ("Total Orders", 
                 "SELECT COUNT(*) FROM raw_data.orders"),
                
                ("Orders with Reviews", 
                 """SELECT COUNT(DISTINCT o.order_id) 
                    FROM raw_data.orders o 
                    JOIN raw_data.order_reviews r ON o.order_id = r.order_id"""),
                
                ("Unique Customers", 
                 "SELECT COUNT(DISTINCT customer_unique_id) FROM raw_data.customers"),
                
                ("Repeat Customers",
                 """SELECT COUNT(*) FROM (
                    SELECT customer_unique_id, COUNT(*) as order_count
                    FROM raw_data.customers
                    GROUP BY customer_unique_id
                    HAVING COUNT(*) > 1
                 ) t"""),
                
                ("Active Sellers", 
                 "SELECT COUNT(DISTINCT seller_id) FROM raw_data.order_items"),
                
                ("Product Categories",
                 "SELECT COUNT(DISTINCT product_category_name) FROM raw_data.products WHERE product_category_name IS NOT NULL"),
            ],
            
            "📅 Time Range": [
                ("First Order Date",
                 "SELECT MIN(order_purchase_timestamp)::date FROM raw_data.orders"),
                
                ("Last Order Date",
                 "SELECT MAX(order_purchase_timestamp)::date FROM raw_data.orders"),
                
                ("Data Coverage (months)",
                 """SELECT EXTRACT(YEAR FROM age(
                    MAX(order_purchase_timestamp), 
                    MIN(order_purchase_timestamp)
                 )) * 12 + EXTRACT(MONTH FROM age(
                    MAX(order_purchase_timestamp), 
                    MIN(order_purchase_timestamp)
                 )) as months FROM raw_data.orders"""),
            ],
            
            "💰 Financial Metrics": [
                ("Total Revenue (BRL)",
                 """SELECT ROUND(SUM(payment_value)::numeric, 2) 
                    FROM raw_data.order_payments"""),
                
                ("Average Order Value (BRL)",
                 """SELECT ROUND(AVG(order_total)::numeric, 2)
                    FROM (
                        SELECT order_id, SUM(payment_value) as order_total
                        FROM raw_data.order_payments
                        GROUP BY order_id
                    ) t"""),
                
                ("Average Product Price (BRL)",
                 "SELECT ROUND(AVG(price)::numeric, 2) FROM raw_data.order_items"),
                
                ("Average Freight Cost (BRL)",
                 "SELECT ROUND(AVG(freight_value)::numeric, 2) FROM raw_data.order_items"),
            ],
            
            "⭐ Customer Satisfaction": [
                ("Average Review Score",
                 "SELECT ROUND(AVG(review_score)::numeric, 2) FROM raw_data.order_reviews"),
                
                ("5-Star Reviews",
                 """SELECT COUNT(*) || ' (' || 
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM raw_data.order_reviews), 1) || '%)' 
                    FROM raw_data.order_reviews WHERE review_score = 5"""),
                
                ("1-Star Reviews",
                 """SELECT COUNT(*) || ' (' || 
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM raw_data.order_reviews), 1) || '%)' 
                    FROM raw_data.order_reviews WHERE review_score = 1"""),
            ],
            
            "📍 Geographic Distribution": [
                ("States with Orders",
                 "SELECT COUNT(DISTINCT customer_state) FROM raw_data.customers"),
                
                ("Cities with Orders",
                 "SELECT COUNT(DISTINCT customer_city) FROM raw_data.customers"),
                
                ("Top State by Orders",
                 """SELECT customer_state || ' (' || COUNT(*) || ' orders)'
                    FROM raw_data.orders o
                    JOIN raw_data.customers c ON o.customer_id = c.customer_id
                    GROUP BY customer_state
                    ORDER BY COUNT(*) DESC
                    LIMIT 1"""),
            ],
            
            "📦 Order Status": [
                ("Delivered Orders",
                 """SELECT COUNT(*) || ' (' || 
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM raw_data.orders), 1) || '%)' 
                    FROM raw_data.orders WHERE order_status = 'delivered'"""),
                
                ("Cancelled Orders",
                 """SELECT COUNT(*) || ' (' || 
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM raw_data.orders), 1) || '%)' 
                    FROM raw_data.orders WHERE order_status = 'canceled'"""),
            ]
        }
        
        for category, checks in quality_checks.items():
            logging.info(f"\n{category}")
            logging.info("-" * 40)
            
            for check_name, query in checks:
                try:
                    with self.engine.connect() as conn:
                        result = conn.execute(text(query))
                        value = result.scalar()
                        
                        if value is not None:
                            if isinstance(value, (int, float)):
                                logging.info(f"  {check_name}: {value:,.0f}")
                            else:
                                logging.info(f"  {check_name}: {value}")
                        else:
                            logging.info(f"  {check_name}: No data")
                            
                except Exception as e:
                    logging.warning(f"  {check_name}: Error - {str(e)[:80]}")
    
    def create_summary_views(self):
        """Create useful views for quick analysis"""
        
        views_sql = """
        -- Order summary view
        CREATE OR REPLACE VIEW raw_data.v_order_summary AS
        SELECT 
            o.order_id,
            o.customer_id,
            c.customer_unique_id,
            c.customer_city,
            c.customer_state,
            o.order_status,
            o.order_purchase_timestamp,
            o.order_delivered_customer_date,
            EXTRACT(EPOCH FROM (o.order_delivered_customer_date - o.order_purchase_timestamp))/86400 as delivery_days,
            COUNT(DISTINCT oi.product_id) as product_count,
            SUM(oi.price) as total_product_value,
            SUM(oi.freight_value) as total_freight_value,
            SUM(oi.price + oi.freight_value) as total_order_value
        FROM raw_data.orders o
        LEFT JOIN raw_data.customers c ON o.customer_id = c.customer_id
        LEFT JOIN raw_data.order_items oi ON o.order_id = oi.order_id
        GROUP BY o.order_id, o.customer_id, c.customer_unique_id, 
                 c.customer_city, c.customer_state, o.order_status,
                 o.order_purchase_timestamp, o.order_delivered_customer_date;
        
        -- Product performance view
        CREATE OR REPLACE VIEW raw_data.v_product_performance AS
        SELECT 
            p.product_id,
            p.product_category_name,
            pt.product_category_name_english,
            COUNT(DISTINCT oi.order_id) as times_ordered,
            SUM(oi.price) as total_revenue,
            AVG(oi.price) as avg_price,
            AVG(r.review_score) as avg_review_score
        FROM raw_data.products p
        LEFT JOIN raw_data.order_items oi ON p.product_id = oi.product_id
        LEFT JOIN raw_data.order_reviews r ON oi.order_id = r.order_id
        LEFT JOIN raw_data.product_category_translation pt 
            ON p.product_category_name = pt.product_category_name
        GROUP BY p.product_id, p.product_category_name, pt.product_category_name_english;
        
        -- Seller performance view
        CREATE OR REPLACE VIEW raw_data.v_seller_performance AS
        SELECT 
            s.seller_id,
            s.seller_city,
            s.seller_state,
            COUNT(DISTINCT oi.order_id) as total_orders,
            COUNT(DISTINCT oi.product_id) as unique_products_sold,
            SUM(oi.price) as total_revenue,
            AVG(oi.price) as avg_product_price,
            AVG(oi.freight_value) as avg_freight_value,
            AVG(r.review_score) as avg_review_score
        FROM raw_data.sellers s
        LEFT JOIN raw_data.order_items oi ON s.seller_id = oi.seller_id
        LEFT JOIN raw_data.order_reviews r ON oi.order_id = r.order_id
        GROUP BY s.seller_id, s.seller_city, s.seller_state;
        """
        
        try:
            with self.engine.connect() as conn:
                for statement in views_sql.split(';'):
                    if statement.strip():
                        conn.execute(text(statement))
                conn.commit()
            logging.info("\n✓ Created summary views for analysis")
            logging.info("  - v_order_summary")
            logging.info("  - v_product_performance") 
            logging.info("  - v_seller_performance")
        except Exception as e:
            logging.warning(f"⚠️  Error creating views: {e}")

def main():
    """Main execution function"""
    print("\n" + "🚀 " + "="*58)
    print("   OLIST E-COMMERCE DATA PIPELINE - ENHANCED LOADER")
    print("="*60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    loader = OlistDataLoader()
    
    # Step 1: Connect to database
    if not loader.connect_database():
        logging.error("Cannot proceed without database connection")
        sys.exit(1)
    
    # Step 2: Create schemas
    if not loader.create_schemas():
        logging.error("Cannot proceed without schemas")
        sys.exit(1)
    
    # Step 3: Load CSV files with proper data types
    if loader.load_csv_files():
        # Step 4: Add documentation
        loader.create_foreign_key_documentation()
        
        # Step 5: Create summary views
        loader.create_summary_views()
        
        # Step 6: Run comprehensive data quality checks
        loader.verify_data_quality()
        
        print("\n" + "="*60)
        print("✅ DATA PIPELINE SETUP COMPLETE!")
        print("="*60)
        print("\n📌 Next Steps:")
        print("1. Review data quality metrics above")
        print("2. Create dbt models in staging schema")
        print("3. Build analytics models with business logic")
        print("4. Connect Superset and create dashboards")
        print("5. Document any data quality issues found")
        print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        print("\n💡 Quick Start Queries:")
        print("   - SELECT * FROM raw_data.v_order_summary LIMIT 10;")
        print("   - SELECT * FROM raw_data.v_product_performance LIMIT 10;")
        print("   - SELECT * FROM raw_data.v_seller_performance LIMIT 10;")
    else:
        logging.error("Data loading failed")
        sys.exit(1)

if __name__ == "__main__":
    main()