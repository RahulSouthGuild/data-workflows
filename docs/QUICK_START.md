# Pidilite DataWiz - Quick Start Guide

## Prerequisites

- Python 3.9+
- StarRocks database running
- Azure Blob Storage account
- MongoDB (optional, for business constants)

## Installation

### 1. Clone and Setup

```bash
cd /home/rahul/RahulSouthGuild/pidilite_datawiz
```

### 2. Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -e .
```

For development:
```bash
pip install -e ".[dev]"
```

### 4. Configure Environment

```bash
# Copy example environment file
cp .env.example .env

# Edit .env with your credentials
nano .env  # or use your preferred editor
```

**Required configuration:**
```env
# StarRocks Database
DB_HOST=your_starrocks_host
DB_PORT=9030
DB_NAME=datawiz
DB_USER=root
DB_PASSWORD=your_password

# Azure Blob Storage
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=...
AZURE_CONTAINER_NAME=your-container
```

### 5. Initialize Database

```bash
# Create database tables
python db/create_tables.py

# Create indexes
python db/indexes/clean_indexes.py

# (Optional) Setup RLS
python db/rls/create_rls.py
```

## Directory Structure Overview

```
pidilite_datawiz/
├── config/              # Configuration (DB, Azure, logging)
├── core/                # ETL logic (extractors, transformers, loaders, jobs)
├── scheduler/           # Job scheduling (daily/evening, daily/morning, monthly)
├── notifications/       # Email notifications
├── db/                  # Database setup, migrations, indexes
├── data/                # Data storage
│   ├── data_historical/ # Historical data
│   └── data_incremental/# Incremental data
└── logs/                # Application logs
```

## Usage

### One-Time Data Load (Historical)

If you have CSV files to load manually:

```bash
# 1. Place CSV files in data/data_historical/raw/

# 2. Convert CSV to Parquet
python core/transformers/type_converter.py

# 3. Clean and validate
python core/transformers/data_cleaner.py

# 4. Load to database
python core/jobs/historical_ingestion.py
```

### Daily ETL Pipeline

The system runs two main pipelines:

**Evening (6 PM)**: Load Dimension Tables
```bash
# Manual trigger (for testing)
python scheduler/daily/evening/main.py
```

Jobs executed:
1. Dimension sync (6:00 PM)
2. TSR hierarchy update (6:30 PM)
3. Refresh materialized views (7:00 PM)
4. Business constants (7:30 PM)

**Morning (8 AM)**: Load Fact Tables
```bash
# Manual trigger (for testing)
python scheduler/daily/morning/main.py
```

Jobs executed:
1. Blob backup (8:00 AM)
2. FIS incremental load (9:00 AM)
3. FID incremental load (10:30 AM)
4. DD logic (12:00 PM)

### Start Scheduler

To run the automated scheduler:

```bash
python scheduler/orchestrator.py
```

This will:
- Load schedule from `scheduler/crontab.yaml`
- Run jobs at configured times
- Send email notifications
- Log all activities

### Check Logs

```bash
# Scheduler logs
tail -f logs/scheduler/scheduler.log

# ETL job logs
tail -f logs/etl/etl.log

# Notification logs
tail -f logs/notifications/notifications.log
```

## Configuration Files

### scheduler/crontab.yaml

Define job schedules:
```yaml
evening_dimension_sync:
  schedule: "0 18 * * *"  # 6:00 PM daily
  job_module: scheduler.daily.evening.dimension_sync
  enabled: true
```

### config/settings.py

Central configuration:
- Database settings
- Azure configuration
- Email settings
- Logging levels

## Common Tasks

### Manually Trigger a Job

```bash
# Evening dimension sync
python -m scheduler.daily.evening.dimension_sync

# Morning FIS load
python -m scheduler.daily.morning.fis_incremental
```

### Validate Configuration

```bash
python config/settings.py
```

### Test Database Connection

```bash
python config/database.py
```

### Test Azure Connection

```bash
python config/storage.py
```

### View Job History

```bash
# Check logs directory
ls -lh logs/etl/

# View recent job logs
cat logs/etl/fis_incremental_2025-11-25.log
```

## Monitoring

### Email Notifications

Configured in `.env`:
```env
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your_email@example.com
SMTP_PASSWORD=your_app_password
EMAIL_RECIPIENTS=admin@pidilite.com,team@pidilite.com
```

Notifications sent for:
- Job failures
- Daily summary (8 PM)
- Data validation errors

### Observability (Optional)

Setup SignOz for monitoring:
```env
SIGNOZ_ENDPOINT=http://your_signoz:4317
ENABLE_TRACING=true
ENABLE_METRICS=true
```

## Troubleshooting

### Database Connection Failed

```bash
# Check StarRocks is running
telnet your_starrocks_host 9030

# Verify credentials in .env
python config/database.py
```

### Azure Blob Access Error

```bash
# Verify connection string
python config/storage.py

# Check container exists
az storage container list --connection-string "your_connection_string"
```

### Jobs Not Running

```bash
# Check scheduler is enabled
grep ENABLE_SCHEDULER .env

# Check crontab.yaml syntax
python -c "import yaml; yaml.safe_load(open('scheduler/crontab.yaml'))"

# View scheduler logs
tail -f logs/scheduler/scheduler.log
```

### Email Not Sending

```bash
# Test SMTP connection
python notifications/email_service.py

# For Gmail, enable App Passwords:
# https://myaccount.google.com/apppasswords
```

## Data Flow

```
Azure Blob Storage
    ↓
[Extract] → data/data_*/raw/
    ↓
[Transform] → data/data_*/cleaned_parquets/
    ↓
[Load] → StarRocks Database
    ↓
[Validate] → Email Notification
```

## Best Practices

1. **Always load dimensions before facts**
   - Evening jobs (dimensions) must complete
   - Morning jobs (facts) depend on dimension data

2. **Monitor disk space**
   - Clean old files in `data/*/archive/`
   - Check log rotation

3. **Backup regularly**
   - Database backup runs at 2 AM daily
   - Keep backups offsite

4. **Test before production**
   - Use `.env.dev` for testing
   - Validate data before loading

## Next Steps

1. Review [migration_guide.md](migration_guide.md) for detailed migration
2. Check [ETL_Pipeline_Documentation.md](ETL_Pipeline_Documentation.md) for ETL details
3. Setup monitoring dashboards
4. Configure custom business logic
5. Document your specific workflows

## Support

- Documentation: `docs/` directory
- Issues: Contact rahul@pidilite.com
- Logs: `logs/` directory
