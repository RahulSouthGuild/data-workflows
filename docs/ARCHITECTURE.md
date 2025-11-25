# Pidilite DataWiz - Architecture Overview

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Azure Blob Storage                         │
│                   (Source Data Repository)                      │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ETL PIPELINE (core/)                         │
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐     │
│  │  Extractors  │ ─▶ │ Transformers │ ─▶ │   Loaders    │     │
│  └──────────────┘    └──────────────┘    └──────────────┘     │
│         │                   │                    │             │
│    [Download]          [Clean &            [Batch Insert]      │
│    [Read Blob]          Validate]          [Incremental]       │
│                                                                 │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                   StarRocks Database                            │
│                                                                 │
│  ┌──────────────────┐         ┌──────────────────┐            │
│  │ Dimension Tables │         │   Fact Tables    │            │
│  │  (Evening Load)  │         │  (Morning Load)  │            │
│  └──────────────────┘         └──────────────────┘            │
│         │                              │                       │
│    [Reference Data]              [Transactional]               │
│    [Hierarchies]                 [FIS, FID, DD]                │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Observability Layer                          │
│                                                                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐      │
│  │  Logging │  │  Tracing │  │ Metrics  │  │  Email   │      │
│  │          │  │ (SignOz) │  │ (OTEL)   │  │  Alerts  │      │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘      │
└─────────────────────────────────────────────────────────────────┘
```

## Data Flow

### Evening Pipeline (6 PM - 8 PM)

```
Time    Job                          Purpose
────────────────────────────────────────────────────────────
18:00   Dimension Sync               Load reference tables
        ↓
18:30   TSR Hierarchy Update         Update sales hierarchies
        ↓
19:00   Refresh Materialized Views   Update reporting views
        ↓
19:30   Business Constants           Update MongoDB configs
```

### Morning Pipeline (8 AM - 12 PM)

```
Time    Job                          Purpose
────────────────────────────────────────────────────────────
08:00   Blob Backup                  Download latest files
        ↓
09:00   FIS Incremental Load         Load sales fact table
        ↓
10:30   FID Incremental Load         Load distribution fact
        ↓
12:00   DD Logic                     Business calculations
```

## Component Details

### 1. Configuration Layer (config/)

**Purpose**: Centralized configuration management

- `settings.py`: All environment variables and configs
- `database.py`: Database connection pooling (StarRocks)
- `storage.py`: Azure Blob Storage client
- `logging_config.py`: Structured logging setup
- `observability.py`: OpenTelemetry tracing & metrics

**Key Features**:
- Environment-based configuration (.env files)
- Connection pooling for performance
- Automatic retry logic
- Health checks

### 2. Core ETL Layer (core/)

#### Extractors (core/extractors/)
- `azure_blob_extractor.py`: Download files from Azure
- `parquet_reader.py`: Read Parquet files efficiently
- `base_extractor.py`: Common extraction interface

#### Transformers (core/transformers/)
- `data_cleaner.py`: Data quality & validation
- `material_mapper.py`: Material code mapping
- `rls_mapper.py`: Row-level security mapping
- `type_converter.py`: CSV to Parquet conversion

#### Loaders (core/loaders/)
- `batch_loader.py`: Bulk insert operations
- `incremental_loader.py`: Checkpoint-based loading
- `stream_loader.py`: Real-time streaming
- `base_loader.py`: Common loading interface

#### Jobs (core/jobs/)
- `historical_ingestion.py`: One-time data load
- `incremental_ingestion.py`: Daily incremental load
- `dimension_refresh.py`: Evening dimension jobs
- `fact_ingestion_fis.py`: FIS fact table load
- `fact_ingestion_fid.py`: FID fact table load
- `dd_logic.py`: Business logic processing

### 3. Scheduler Layer (scheduler/)

**Purpose**: Job orchestration and scheduling

```python
# APScheduler-based orchestration
orchestrator.py          # Main scheduler
job_registry.py          # Job definitions

daily/
  evening/               # 6 PM - Dimensions
    dimension_sync.py
    tsr_hierarchy.py
    refresh_matviews.py
  morning/               # 8 AM - Facts
    fis_incremental.py
    fid_incremental.py
    dd_logic.py
monthly/
  vacuum_datawiz.py      # Database maintenance
```

**Features**:
- YAML-based configuration (crontab.yaml)
- Automatic retry on failure
- Timeout management
- Job dependencies
- Email notifications

### 4. Notification Layer (notifications/)

**Purpose**: Email alerts and reporting

- `email_service.py`: SMTP email service
- `templates/`: HTML email templates
- `utils/mjml_generator.py`: Beautiful emails using MJML
- `handlers/`: Email template handlers

**Notification Types**:
- Job success/failure
- Daily summary report
- Data validation errors
- System health alerts

### 5. Database Layer (db/)

**Purpose**: Database schema and maintenance

```
db/
├── tables.py              # Table definitions
├── create_tables.py       # Initial setup
├── migrations/            # Schema changes
├── indexes/               # Index optimization
├── rls/                   # Row-level security
├── benchmarking/          # Performance testing
└── scripts/               # Utility scripts
```

**Database**: StarRocks
- Columnar storage for analytics
- High-performance queries
- Horizontal scaling
- Real-time data ingestion

### 6. Observability Layer (observability/)

**Purpose**: Monitoring and debugging

```
observability/
├── tracer.py              # OpenTelemetry tracing
├── metrics.py             # Custom metrics
├── logging_setup.py       # Structured logging
├── decorators.py          # @trace, @measure
└── dashboards/            # SignOz dashboards
```

**Metrics Tracked**:
- Job execution time
- Data volume processed
- Error rates
- Database performance
- System resources

## Data Storage Structure

```
data/
├── data_historical/           # Historical data load
│   ├── raw/                  # CSV files from Azure
│   ├── raw_parquets/         # Converted Parquet
│   ├── cleaned_parquets/     # Validated data
│   └── archive/              # Old files
│
├── data_incremental/          # Daily incremental
│   ├── raw_parquets/         # New data
│   ├── cleaned_parquets/     # Validated
│   ├── incremental/          # Processing
│   └── checkpoint/           # State tracking
│
└── temp/                      # Temporary files
```

## Technology Stack

### Languages & Frameworks
- **Python 3.9+**: Core language
- **SQLAlchemy**: Database ORM
- **APScheduler**: Job scheduling
- **Pandas/PyArrow**: Data processing

### Databases
- **StarRocks**: OLAP database (main)
- **MongoDB**: Business constants (optional)

### Cloud Services
- **Azure Blob Storage**: Data lake
- **Azure Storage SDK**: Blob operations

### Observability
- **OpenTelemetry**: Distributed tracing
- **SignOz**: Metrics & dashboards
- **Python logging**: Application logs

### DevOps
- **pytest**: Testing framework
- **black**: Code formatting
- **mypy**: Type checking

## Design Principles

### 1. Separation of Concerns
- Extract, Transform, Load are separate modules
- Configuration isolated from business logic
- Clear boundaries between layers

### 2. Idempotency
- Jobs can be re-run safely
- Checkpoint-based processing
- Upsert operations (not just insert)

### 3. Observability
- Every job is traced
- Metrics for all operations
- Comprehensive logging

### 4. Resilience
- Automatic retry logic
- Connection pooling
- Graceful error handling
- Email alerts on failures

### 5. Scalability
- Batch processing for large data
- Incremental loading
- Parallel job execution
- Connection pooling

## Security

### Database Security
- Connection pooling (no credential exposure)
- Environment-based credentials
- Row-level security (RLS)

### Azure Security
- Connection string in .env (not code)
- Container-level access control

### Email Security
- App passwords (not real passwords)
- TLS encryption

## Performance Optimization

### Database
- Indexes on frequently queried columns
- Materialized views for complex queries
- Batch inserts (not row-by-row)
- Connection pooling

### Data Processing
- Parquet format (columnar, compressed)
- Pandas vectorized operations
- Chunked reading for large files
- Parallel processing where possible

### Scheduling
- Dimension tables loaded first (evening)
- Fact tables depend on dimensions (morning)
- Off-peak database maintenance (2 AM)
- Monthly vacuum for optimization

## Disaster Recovery

### Backups
- Daily database backup (2 AM)
- Azure blob file backup
- 30-day retention policy
- Offsite storage recommended

### Recovery
- Checkpoint files for incremental loads
- Archive historical data
- Database restore scripts
- Documented recovery procedures

## Future Enhancements

1. **Real-time Streaming**
   - Kafka/Event Hub integration
   - CDC (Change Data Capture)
   - Near real-time dashboards

2. **Advanced Monitoring**
   - Slack notifications
   - MS Teams integration
   - Custom dashboards
   - Anomaly detection

3. **Data Quality**
   - Great Expectations integration
   - Automated data profiling
   - Quality scorecards

4. **Orchestration**
   - Apache Airflow migration
   - DAG-based workflows
   - Advanced dependencies

## Summary

Pidilite DataWiz is a modular, scalable ETL pipeline designed for:
- **Reliability**: Automatic retry, checkpointing, monitoring
- **Performance**: Batch processing, connection pooling, optimized queries
- **Maintainability**: Clear structure, separation of concerns, documentation
- **Observability**: Comprehensive logging, tracing, metrics, alerts

The system handles both historical and incremental data loads, with a clear separation between dimensional (evening) and fact (morning) table processing.
