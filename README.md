# 🛒 Olist E-Commerce Data Pipeline

A comprehensive end-to-end data pipeline for analyzing Brazilian e-commerce data from Olist, built with modern data engineering tools and best practices.

![Pipeline Status](https://img.shields.io/badge/Status-Active-green)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)

## 📊 Project Overview

This project implements a scalable data pipeline for processing and analyzing Olist's e-commerce dataset, featuring:

- **Data Ingestion**: Automated CSV data loading with proper type inference
- **Data Transformation**: dbt models for data cleaning and business logic
- **Data Storage**: PostgreSQL with optimized schemas and indexing
- **Data Visualization**: Apache Superset dashboards for business insights
- **Data Quality**: Comprehensive validation and monitoring

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Raw CSV Data  │──▶ │  PostgreSQL DB  │──▶ │   dbt Models    │──▶ │ Apache Superset │
│   (9 datasets)  │    │  (raw_data)     │    │  (staging/mart) │    │  (Dashboards)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
                                ▲                        ▲
                                │                        │
                       ┌─────────────────┐    ┌─────────────────┐
                       │ Data Quality    │    │ Airbyte         │
                       │ Monitoring      │    │ (Future)        │
                       └─────────────────┘    └─────────────────┘
```

## 📁 Project Structure

```
olist-ecommerce-pipeline/
├── 📂 configs/                    # Configuration files
│   └── airbyte_source_config.json
├── 📂 data/                       # Data storage
│   ├── 📂 raw/                    # Original CSV datasets (9 files)
│   └── 📂 processed/              # Analysis results and artifacts
├── 📂 dbt_project/               # dbt transformation models
│   └── 📂 olist_analytics/
│       ├── dbt_project.yml
│       └── 📂 models/
├── 📂 docker/                    # Docker containerization
│   ├── docker-compose.yml
│   └── .env
├── 📂 scripts/                   # Python utilities and loaders
│   ├── data_field_and_types_raw.py     # Data analysis script
│   ├── enhanced_olist_loader.py         # Main data loader
│   ├── init.sql                        # Database initialization
│   └── setup_airbyte_config.py         # Airbyte configuration
└── 📂 docs/                      # Documentation
```

## 🎯 Key Features

### 📊 **Data Analysis & Quality**
- **Automated Data Profiling**: Analyzes 1.5M+ records across 9 CSV files
- **Type Inference**: Smart detection of strings, numerics, and datetime fields
- **Data Quality Checks**: Comprehensive validation rules and monitoring
- **Statistical Summaries**: Key metrics and data distribution analysis

### 🔄 **Data Processing Pipeline**
- **Enhanced Data Loader**: Robust CSV ingestion with error handling
- **Schema Management**: Organized data layers (raw → staging → analytics)
- **Indexing Strategy**: Optimized database performance
- **Data Lineage**: Clear tracking of data transformations

### 🗄️ **Database Design**
- **Multi-Schema Architecture**: 
  - `raw_data`: Unchanged source data
  - `staging`: Cleaned and standardized data
  - `analytics`: Business logic and aggregations
  - `data_quality`: Monitoring and validation
- **Relationship Documentation**: Comprehensive table and column comments
- **Performance Optimization**: Strategic indexing and materialized views

### 🐳 **Containerized Infrastructure**
- **Docker Compose**: Multi-service orchestration
- **PostgreSQL**: Primary analytical database
- **Superset**: Self-service analytics platform
- **Environment Isolation**: Clean, reproducible deployments

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- Docker & Docker Compose
- Git

### 1. Clone and Setup
```bash
git clone https://github.com/ammarpvl29/olist-ecommerce-pipeline.git
cd olist-ecommerce-pipeline

# Create virtual environment
python -m venv venv
venv/Scripts/activate  # Windows
# source venv/bin/activate  # Linux/Mac

# Install dependencies
pip install pandas numpy sqlalchemy psycopg2-binary
```

### 2. Start Infrastructure
```bash
cd docker
docker-compose up -d
```

This starts:
- **PostgreSQL** at `localhost:5432`
- **Apache Superset** at `http://localhost:8088`

### 3. Load Data
```bash
# Analyze raw data
python scripts/data_field_and_types_raw.py

# Load data into PostgreSQL
python scripts/enhanced_olist_loader.py
```

### 4. Access Analytics
- **Superset UI**: http://localhost:8088
- **Database**: `postgresql://olist_user:olist_pass123@localhost:5432/olist_analytics`

## 📈 Dataset Overview

The Olist dataset contains **1.5M+ records** across 9 interconnected tables:

| Table | Records | Description |
|-------|---------|-------------|
| `orders` | 99,441 | Core order transactions |
| `order_items` | 112,650 | Individual items per order |
| `customers` | 99,441 | Customer information |
| `sellers` | 3,095 | Marketplace sellers |
| `products` | 32,951 | Product catalog |
| `order_reviews` | 99,224 | Customer reviews and ratings |
| `order_payments` | 103,886 | Payment transactions |
| `geolocation` | 1,000,163 | Brazilian zip code coordinates |
| `category_translation` | 71 | Portuguese to English translations |

### Key Business Metrics
- **📅 Time Period**: 2016-2020 (4+ years of data)
- **🌍 Geographic Coverage**: All 27 Brazilian states
- **💰 Total Revenue**: Multi-million BRL in transactions
- **⭐ Customer Satisfaction**: Average 4+ star ratings
- **🔄 Repeat Customers**: Detailed customer journey analysis

## 🛠️ Scripts & Tools

### `data_field_and_types_raw.py`
**Data Analysis & Profiling Tool**
```bash
python scripts/data_field_and_types_raw.py
```
- Analyzes all CSV files automatically
- Infers optimal data types
- Generates comprehensive statistics
- Exports results to JSON format

### `enhanced_olist_loader.py`
**Production Data Loader**
```bash
python scripts/enhanced_olist_loader.py
```
- Loads 9 CSV files into PostgreSQL
- Creates optimized database schemas
- Implements data quality checks
- Generates summary views and documentation

### `init.sql`
**Database Initialization Script**
- Creates multi-schema architecture
- Sets up user permissions and security
- Implements utility functions
- Defines data validation rules

## 📊 Analytics & Insights

### Pre-built Views
- `v_order_summary`: Comprehensive order analytics
- `v_product_performance`: Product sales metrics
- `v_seller_performance`: Marketplace seller analysis

### Business Intelligence Queries
```sql
-- Customer Lifetime Value
SELECT customer_unique_id, 
       COUNT(*) as total_orders,
       SUM(total_order_value) as lifetime_value
FROM raw_data.v_order_summary 
GROUP BY customer_unique_id;

-- Top Product Categories
SELECT category, total_revenue, avg_review_score
FROM raw_data.v_product_performance 
ORDER BY total_revenue DESC;

-- Geographic Sales Distribution
SELECT customer_state, 
       COUNT(*) as orders,
       AVG(delivery_days) as avg_delivery
FROM raw_data.v_order_summary 
GROUP BY customer_state;
```

## 🔍 Data Quality Monitoring

### Automated Checks
- **Referential Integrity**: Cross-table validation
- **Business Rules**: Logical consistency checks
- **Statistical Outliers**: Anomaly detection
- **Completeness**: Missing data analysis

### Quality Metrics Dashboard
- Data freshness indicators
- Validation rule pass/fail rates
- Trend analysis and alerts
- Historical quality tracking

## 🚦 Environment Configuration

### Database Connection
```python
DATABASE_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'olist_analytics',
    'user': 'olist_user',
    'password': 'olist_pass123'
}
```

### Docker Environment Variables
```env
# PostgreSQL
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=olist_analytics
POSTGRES_USER=olist_user
POSTGRES_PASSWORD=olist_pass123

# Superset
SUPERSET_SECRET_KEY=your_secret_key_here
SQLALCHEMY_DATABASE_URI=postgresql://superset:superset123@postgres_superset:5432/superset
```

## 🔮 Future Enhancements

### Phase 1: Advanced Analytics
- [ ] Machine Learning models for customer segmentation
- [ ] Predictive analytics for sales forecasting
- [ ] Real-time streaming data integration

### Phase 2: Platform Expansion
- [ ] Airbyte connectors for additional data sources
- [ ] Advanced dbt transformations and tests
- [ ] Data catalog with automated documentation

### Phase 3: Production Readiness
- [ ] CI/CD pipeline with automated testing
- [ ] Monitoring and alerting system
- [ ] High availability and disaster recovery

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🏆 Acknowledgments

- **Olist** for providing the comprehensive e-commerce dataset
- **dbt Labs** for the excellent transformation framework
- **Apache Superset** for the powerful visualization platform
- **PostgreSQL** community for the robust database system

---

**💡 Ready to explore Brazilian e-commerce data? Start with `python scripts/data_field_and_types_raw.py` to see what insights await!**