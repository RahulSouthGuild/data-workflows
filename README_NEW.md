# 🏭 Pidilite DataWiz - ETL Pipeline

Modern, modular ETL pipeline for loading data from Azure Blob Storage into StarRocks database with comprehensive monitoring and notifications.

## ✨ Features

- **Modular Architecture**: Clean separation of concerns (Extract, Transform, Load)
- **StarRocks Database**: High-performance OLAP database for analytics
- **Azure Integration**: Direct blob storage integration
- **Automated Scheduling**: Evening (dimensions) and morning (facts) jobs
- **Email Notifications**: Alerts for job success/failure
- **Observability**: OpenTelemetry tracing, SignOz metrics, comprehensive logging
- **Incremental Loading**: Checkpoint-based processing for efficiency
- **Data Quality**: Validation and cleaning at every stage

## 📁 Project Structure

```
pidilite_datawiz/
│
├── config/                   # 🔧 Configuration Management
│   ├── settings.py           # Centralized settings (DB, Azure, Email)
│   ├── database.py           # StarRocks connection pooling
│   ├── storage.py            # Azure Blob Storage client
│   ├── logging_config.py     # Logging configuration
│   └── observability.py      # OpenTelemetry tracing & metrics
│
├── core/                     # 🚀 ETL Pipeline
│   ├── extractors/           # Extract from Azure Blob Storage
│   ├── transformers/         # Clean, validate, transform data
│   ├── loaders/              # Load to StarRocks (batch/incremental)
│   └── jobs/                 # ETL job definitions
│
├── scheduler/                # ⏰ Job Scheduling
│   ├── daily/evening/        # 6 PM - Load dimension tables
│   ├── daily/morning/        # 8 AM - Load fact tables
│   ├── monthly/              # Monthly maintenance
│   └── crontab.yaml          # Job schedule configuration
│
├── notifications/            # 📧 Email & Alerts
│   ├── email_service.py      # SMTP email service
│   ├── templates/            # HTML email templates
│   └── utils/                # Email generators (MJML)
│
├── db/                       # 🗄️ Database Management
│   ├── tables.py             # Table definitions
│   ├── migrations/           # Schema migrations
│   ├── indexes/              # Index optimization
│   └── rls/                  # Row-level security
│
├── observability/            # 📊 Monitoring
│   ├── tracer.py             # OpenTelemetry tracing
│   ├── metrics.py            # Custom metrics
│   └── dashboards/           # SignOz dashboards
│
├── data/                     # 💾 Data Storage
│   ├── data_historical/      # Historical data (one-time load)
│   └── data_incremental/     # Incremental daily data
│
├── logs/                     # 📝 Application Logs
│   ├── scheduler/            # Scheduler logs
│   ├── etl/                  # ETL job logs
│   └── notifications/        # Email notification logs
│
└── docs/                     # 📚 Documentation
    ├── QUICK_START.md        # Getting started guide
    ├── migration_guide.md    # Migration instructions
    └── ARCHITECTURE.md       # System architecture
```

## 🚀 Quick Start

### 1. Setup Environment

```bash
# Copy environment template
cp .env.example .env

# Edit with your credentials
nano .env
```

### 2. Install Dependencies

```bash
pip install -e .
```

### 3. Configure Database

Update `.env` with your StarRocks credentials:

```env
DB_HOST=your_starrocks_host
DB_PORT=9030
DB_NAME=datawiz
DB_USER=root
DB_PASSWORD=your_password
```

### 4. Configure Azure Storage

```env
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;...
AZURE_CONTAINER_NAME=your-container
```

### 5. Initialize Database

```bash
python db/create_tables.py
python db/indexes/clean_indexes.py
```

### 6. Start Pipeline

```bash
# Manual test run
python scheduler/daily/evening/main.py   # Load dimensions
python scheduler/daily/morning/main.py   # Load facts

# Start automated scheduler
python scheduler/orchestrator.py
```

## 📊 ETL Flow

### Evening Pipeline (6:00 PM - 8:00 PM)
**Load Dimension Tables First**

```
18:00  Dimension Sync        → Load reference/master data
18:30  TSR Hierarchy Update  → Update sales hierarchies
19:00  Refresh Mat Views     → Update reporting views
19:30  Business Constants    → Update MongoDB configs
```

### Morning Pipeline (8:00 AM - 12:00 PM)
**Load Fact Tables (depends on dimensions)**

```
08:00  Blob Backup           → Download latest Azure files
09:00  FIS Incremental Load  → Load sales fact table
10:30  FID Incremental Load  → Load distribution fact
12:00  DD Logic              → Business calculations
```

## 🎯 Key Components

### Configuration ([config/](config/))
- **Centralized settings**: All config in one place
- **Connection pooling**: Reusable database connections
- **Environment-based**: Separate dev/prod configs

### ETL Pipeline ([core/](core/))
- **Extractors**: Download from Azure Blob Storage
- **Transformers**: Clean, validate, map data
- **Loaders**: Batch/incremental inserts to StarRocks
- **Jobs**: Orchestrated ETL workflows

### Scheduler ([scheduler/](scheduler/))
- **YAML-based**: Easy configuration (crontab.yaml)
- **Automatic retry**: Failed jobs retry automatically
- **Dependencies**: Morning jobs wait for evening completion

### Notifications ([notifications/](notifications/))
- **Email alerts**: Job success/failure notifications
- **Daily reports**: Summary of all jobs
- **MJML templates**: Beautiful HTML emails

### Observability ([observability/](observability/))
- **OpenTelemetry**: Distributed tracing
- **SignOz**: Metrics and dashboards
- **Structured logging**: Searchable logs

## 🛠️ Technology Stack

| Component | Technology |
|-----------|------------|
| Database | StarRocks (OLAP) |
| Storage | Azure Blob Storage |
| Language | Python 3.9+ |
| Scheduler | APScheduler |
| Monitoring | OpenTelemetry + SignOz |
| Data Format | Parquet (columnar) |
| Testing | pytest |

## 📖 Documentation

- **[Quick Start Guide](docs/QUICK_START.md)** - Get up and running
- **[Migration Guide](docs/migration_guide.md)** - Migrate existing pipelines
- **[Architecture](docs/ARCHITECTURE.md)** - System design details
- **[Project Summary](PROJECT_SUMMARY.md)** - Overview and next steps

## 🔍 Common Tasks

### Manually Trigger a Job

```bash
# Test dimension sync
python -m scheduler.daily.evening.dimension_sync

# Test fact load
python -m scheduler.daily.morning.fis_incremental
```

### Check Logs

```bash
# Scheduler logs
tail -f logs/scheduler/scheduler.log

# ETL job logs
tail -f logs/etl/etl.log

# All logs
tail -f logs/**/*.log
```

### Validate Configuration

```bash
# Test database connection
python config/database.py

# Test Azure connection
python config/storage.py

# Validate all config
python config/settings.py
```

## 📧 Email Notifications

Configure in `.env`:

```env
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your_email@example.com
SMTP_PASSWORD=your_app_password
EMAIL_RECIPIENTS=admin@pidilite.com,team@pidilite.com
```

**Notifications sent for:**
- Job failures (immediate)
- Daily summary (8 PM)
- Data validation errors
- System health alerts

## 📈 Monitoring & Observability

### Logs
- Location: `logs/` directory
- Rotation: Daily, 30-day retention
- Format: Structured JSON logs

### Tracing (OpenTelemetry)
- Trace every job execution
- Measure execution time
- Identify bottlenecks

### Metrics (SignOz)
- Job success/failure rates
- Data volume processed
- Database performance
- System resources

## 🔒 Security

- ✅ Environment-based credentials (.env)
- ✅ No hardcoded passwords
- ✅ Connection pooling (secure connections)
- ✅ Row-level security (RLS) support
- ✅ TLS encryption for emails

## 🧪 Testing

```bash
# Run all tests
pytest

# Unit tests only
pytest tests/unit/

# Integration tests
pytest tests/integration/

# With coverage
pytest --cov
```

## 📦 Data Flow

```
Azure Blob Storage
    ↓ [Extract]
data/*/raw/
    ↓ [Transform]
data/*/cleaned_parquets/
    ↓ [Validate]
data/*/incremental/
    ↓ [Load]
StarRocks Database
    ↓ [Verify]
Email Notification
```

## 🎓 Best Practices

1. **Always load dimensions before facts**
   - Evening jobs (dimensions) run first
   - Morning jobs (facts) depend on dimension data

2. **Monitor disk space**
   - Archive old files regularly
   - Clean temp directories

3. **Test before production**
   - Use `.env.dev` for testing
   - Validate data quality

4. **Review logs regularly**
   - Check for warnings
   - Monitor job execution times

## 🐛 Troubleshooting

### Database Connection Failed
```bash
# Check StarRocks is running
telnet your_starrocks_host 9030

# Verify config
python config/database.py
```

### Azure Access Error
```bash
# Test connection
python config/storage.py

# List containers
az storage container list --connection-string "..."
```

### Jobs Not Running
```bash
# Check scheduler enabled
grep ENABLE_SCHEDULER .env

# View logs
tail -f logs/scheduler/scheduler.log
```

## 🤝 Support

- **Documentation**: See `docs/` directory
- **Logs**: Check `logs/` directory
- **Issues**: Contact team

## 📝 License

MIT License - See LICENSE file

## 🎯 Next Steps

1. ✅ Setup complete - Review [QUICK_START.md](docs/QUICK_START.md)
2. ⚙️ Configure environment - Edit `.env` file
3. 🗄️ Initialize database - Run `python db/create_tables.py`
4. 🚀 Start pipeline - Run `python scheduler/orchestrator.py`
5. 📊 Monitor - Check logs and metrics

---

**Built with ❤️ for Pidilite DataWiz**
