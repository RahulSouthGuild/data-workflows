# Pidilite DataWiz - Project Summary

## Overview
ETL pipeline for loading data from Azure Blob Storage into StarRocks database with comprehensive monitoring and notifications.

## Structure Created ✓

```
pidilite_datawiz/
├── config/              ✓ Configuration management
├── core/                ✓ ETL pipeline (extractors, transformers, loaders, jobs)
├── scheduler/           ✓ Job scheduling (evening/morning/monthly)
├── notifications/       ✓ Email alerts
├── db/                  ✓ Database setup and migrations
├── observability/       ✓ Monitoring (SignOz/OpenTelemetry)
├── utils/               ✓ Common utilities
├── data/                ✓ Data storage
├── logs/                ✓ Application logs
├── tests/               ✓ Test suite
├── docs/                ✓ Documentation
└── scripts/             ✓ Utility scripts
```

## Key Files Created

### Configuration
- [.env.example](.env.example) - Environment template
- [pyproject.toml](pyproject.toml) - Python packaging
- [config/settings.py](config/settings.py) - Centralized settings
- [config/database.py](config/database.py) - StarRocks connection
- [config/storage.py](config/storage.py) - Azure Blob Storage
- [config/logging_config.py](config/logging_config.py) - Logging setup
- [config/observability.py](config/observability.py) - Tracing & metrics

### Documentation
- [docs/QUICK_START.md](docs/QUICK_START.md) - Getting started guide
- [docs/migration_guide.md](docs/migration_guide.md) - Migration instructions
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) - System architecture

### Scheduling
- [scheduler/crontab.yaml](scheduler/crontab.yaml) - Job schedules

## Next Steps

1. **Environment Setup**
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

2. **Install Dependencies**
   ```bash
   pip install -e .
   ```

3. **Database Setup**
   ```bash
   python db/create_tables.py
   ```

4. **Start Using**
   - Review: [docs/QUICK_START.md](docs/QUICK_START.md)
   - Configure: Edit `.env` file
   - Test: `python config/settings.py`

## ETL Flow

**Evening (6 PM)**: Load Dimension Tables
- Dimension sync → TSR hierarchy → Refresh views → Business constants

**Morning (8 AM)**: Load Fact Tables  
- Blob backup → FIS load → FID load → DD logic

## Technology Stack

- **Database**: StarRocks (OLAP)
- **Storage**: Azure Blob Storage
- **Language**: Python 3.9+
- **Scheduler**: APScheduler
- **Monitoring**: OpenTelemetry + SignOz
- **Notifications**: Email (SMTP)

## Documentation

- [Quick Start](docs/QUICK_START.md) - Installation & usage
- [Migration Guide](docs/migration_guide.md) - Detailed migration steps
- [Architecture](docs/ARCHITECTURE.md) - System design
- [ETL Pipeline](docs/ETL_Pipeline_Documentation.md) - Pipeline details

## Support

For questions or issues, review the documentation or check logs in `logs/` directory.
