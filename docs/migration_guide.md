# Pidilite DataWiz - Migration Guide

## Overview

This guide helps you migrate from the old structure to the new modular architecture.

## New Directory Structure

### Core Components

#### 1. **config/** - Centralized Configuration
- `settings.py`: All configuration variables (DB, Azure, MongoDB, Email)
- `database.py`: Database connection pooling for StarRocks
- `storage.py`: Azure Blob Storage client management
- `logging_config.py`: Logging setup
- `observability.py`: SignOz/OpenTelemetry tracing and metrics

#### 2. **core/** - ETL Business Logic

**extractors/** - Data Extraction Layer
- Extract data from Azure Blob Storage
- Read Parquet files
- Download blob files

**transformers/** - Data Transformation Layer
- Data cleaning
- Material mapping
- RLS mapping
- Type conversions

**loaders/** - Data Loading Layer
- Batch inserts to StarRocks
- Incremental loading
- Streaming inserts

**jobs/** - ETL Job Definitions
- Historical data ingestion
- Incremental data ingestion
- Dimension table refresh (evening)
- Fact table ingestion (morning - FIS, FID)
- DD logic processing
- Materialized view refresh

#### 3. **scheduler/** - Job Scheduling

**daily/evening/** - Evening Jobs (Dimensional Data)
- `dimension_sync.py`: Load dimension tables
- `tsr_hierarchy.py`: Update TSR hierarchy
- `refresh_matviews.py`: Refresh materialized views
- `business_constants.py`: Update business constants

**daily/morning/** - Morning Jobs (Fact Data)
- `blob_backup.py`: Backup blob files
- `fis_incremental.py`: Load FIS fact table
- `fid_incremental.py`: Load FID fact table
- `dd_logic.py`: Run DD business logic

**monthly/**
- `vacuum_datawiz.py`: Database maintenance

#### 4. **notifications/** - Email & Alerts
- `email_service.py`: Send email notifications
- `templates/`: HTML email templates
- `utils/`: MJML generators, table generators

#### 5. **db/** - Database Management
- `tables.py`: Table definitions
- `create_tables.py`: Create database tables
- `migrations/`: SQL migration scripts
- `indexes/`: Index management
- `rls/`: Row-level security

#### 6. **observability/** - Monitoring
- `tracer.py`: OpenTelemetry tracing
- `metrics.py`: Custom metrics
- `logging_setup.py`: Structured logging
- `decorators.py`: @trace, @measure decorators

#### 7. **data/** - Data Storage
```
data/
├── data_historical/
│   ├── raw/              # Raw CSV files from Azure
│   ├── raw_parquets/     # Converted parquet files
│   ├── cleaned_parquets/ # Cleaned data
│   └── archive/          # Archived data
├── data_incremental/
│   ├── raw_parquets/     # Incremental raw data
│   ├── cleaned_parquets/ # Cleaned incremental data
│   ├── incremental/      # Incremental processing
│   └── checkpoint/       # Checkpoint files
└── temp/                 # Temporary processing
```

## Migration Steps

### Step 1: Environment Setup

1. Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```

2. Update `.env` with your credentials:
```bash
# StarRocks Database
DB_HOST=your_starrocks_host
DB_PORT=9030
DB_NAME=datawiz
DB_USER=your_user
DB_PASSWORD=your_password

# Azure Blob Storage
AZURE_STORAGE_CONNECTION_STRING=your_connection_string
AZURE_CONTAINER_NAME=your_container

# MongoDB (for business constants)
MONGODB_URI=mongodb://your_mongo_host:27017

# Email
SMTP_HOST=smtp.gmail.com
SMTP_USER=your_email@example.com
SMTP_PASSWORD=your_app_password
EMAIL_RECIPIENTS=admin@pidilite.com
```

### Step 2: Install Dependencies

```bash
pip install -e .
# or for development
pip install -e ".[dev]"
```

### Step 3: Database Setup

1. Create tables:
```bash
python db/create_tables.py
```

2. Run migrations:
```bash
python db/migrations/001_initial_schema.sql
```

3. Create indexes:
```bash
python db/indexes/clean_indexes.py
```

### Step 4: Data Migration

#### One-Time Manual Data Load (CSV files)

Place your CSV files in `data/data_historical/raw/` and run:

```bash
# Convert CSV to Parquet
python core/transformers/type_converter.py

# Clean Parquet files
python core/transformers/data_cleaner.py

# Load to database
python core/jobs/historical_ingestion.py
```

### Step 5: Schedule Configuration

The scheduler runs:

**Evening (6 PM)**: Dimension Tables Load
- Dimensions must be loaded first
- Updates reference data
- Refreshes hierarchies

**Morning (8 AM)**: Fact Tables Load
- Depends on dimension data
- Loads FIS (Fact Inventory Sales)
- Loads FID (Fact Inventory Distribution)
- Runs DD business logic

Edit `scheduler/crontab.yaml` for custom schedules:
```yaml
jobs:
  - name: evening_dimensions
    schedule: "0 18 * * *"  # 6 PM daily
    module: scheduler.daily.evening.main

  - name: morning_facts
    schedule: "0 8 * * *"   # 8 AM daily
    module: scheduler.daily.morning.main
```

### Step 6: Notifications Setup

Configure email templates in `notifications/templates/`:
- `job_success.html`: Success notifications
- `job_failure.html`: Failure alerts
- `daily_report.html`: Daily summary report

### Step 7: Observability

Setup SignOz for monitoring:
```bash
# Update .env
SIGNOZ_ENDPOINT=http://your_signoz:4317
ENABLE_TRACING=true
ENABLE_METRICS=true
```

View dashboards in `observability/dashboards/`.

## Job Execution Flow

### Evening Pipeline (Dimensions)
```
1. dimension_sync.py       → Load dimension tables
2. tsr_hierarchy.py        → Update TSR/RLS master
3. refresh_matviews.py     → Refresh materialized views
4. business_constants.py   → Update MongoDB constants
```

### Morning Pipeline (Facts)
```
1. blob_backup.py          → Backup Azure blobs
2. fis_incremental.py      → Load FIS fact table
3. fid_incremental.py      → Load FID fact table
4. dd_logic.py             → Run DD business logic
```

## Key Features

### 1. Incremental Loading
- Checkpoint-based processing
- Idempotent operations
- Automatic retry on failure

### 2. Data Validation
- Schema validation
- Data quality checks
- Email notifications on errors

### 3. Monitoring
- OpenTelemetry tracing
- Custom metrics
- SignOz dashboards

### 4. Maintenance
- Automatic vacuum (monthly)
- Index optimization
- Log rotation

## Testing

Run tests:
```bash
# Unit tests
pytest tests/unit/

# Integration tests
pytest tests/integration/

# All tests with coverage
pytest --cov
```

## Troubleshooting

### Common Issues

1. **Connection errors**
   - Check DB_HOST and DB_PORT in .env
   - Verify StarRocks is running
   - Test connection: `python config/database.py`

2. **Azure blob access**
   - Verify AZURE_STORAGE_CONNECTION_STRING
   - Check container permissions
   - Test: `python config/storage.py`

3. **Email not sending**
   - Enable "Less secure app access" or use app passwords
   - Check SMTP settings
   - Test: `python notifications/email_service.py`

4. **Scheduler not running**
   - Check ENABLE_SCHEDULER=true in .env
   - Verify crontab.yaml syntax
   - Check logs: `tail -f logs/scheduler/scheduler.log`

## Support

For issues or questions:
1. Check logs in `logs/` directory
2. Review `docs/troubleshooting.md`
3. Contact: rahul@pidilite.com

## Next Steps

1. Migrate existing cron jobs from `cron_jobs/` to `scheduler/`
2. Move utilities from `cron_jobs/utils/incremental_utils.py` to modular files
3. Setup monitoring dashboards
4. Configure backup strategies
5. Document custom business logic
