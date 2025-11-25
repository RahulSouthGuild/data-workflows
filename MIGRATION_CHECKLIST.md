# Migration Checklist - Pidilite DataWiz

Use this checklist to track your migration from the old structure to the new modular architecture.

## Phase 1: Environment Setup

- [ ] Copy `.env.example` to `.env`
- [ ] Update database credentials (StarRocks)
  - [ ] DB_HOST
  - [ ] DB_PORT
  - [ ] DB_NAME
  - [ ] DB_USER
  - [ ] DB_PASSWORD
- [ ] Update Azure Blob Storage credentials
  - [ ] AZURE_STORAGE_CONNECTION_STRING
  - [ ] AZURE_CONTAINER_NAME
- [ ] Update MongoDB credentials (if used)
  - [ ] MONGODB_URI
- [ ] Configure email settings
  - [ ] SMTP_HOST
  - [ ] SMTP_USER
  - [ ] SMTP_PASSWORD
  - [ ] EMAIL_RECIPIENTS
- [ ] Set environment to development
  - [ ] ENVIRONMENT=development
- [ ] Install Python dependencies
  - [ ] `pip install -e .`
- [ ] Verify configuration
  - [ ] `python config/settings.py`

## Phase 2: Database Setup

- [ ] Backup existing database (if applicable)
  - [ ] `pg_dump` or equivalent for your current DB
- [ ] Test StarRocks connection
  - [ ] `python config/database.py`
- [ ] Create database tables
  - [ ] `python db/create_tables.py`
- [ ] Create indexes
  - [ ] `python db/indexes/clean_indexes.py`
- [ ] Setup row-level security (optional)
  - [ ] `python db/rls/create_rls.py`
- [ ] Run initial migrations
  - [ ] Review `db/migrations/` SQL files
  - [ ] Execute migration scripts

## Phase 3: Data Migration

### One-Time Historical Data Load

- [ ] Identify CSV files to migrate
- [ ] Place CSV files in `data/data_historical/raw/`
- [ ] Convert CSV to Parquet
  - [ ] Implement `core/transformers/type_converter.py`
  - [ ] Run conversion
- [ ] Clean and validate Parquet files
  - [ ] Implement `core/transformers/data_cleaner.py`
  - [ ] Run cleaning
- [ ] Load to database
  - [ ] Implement `core/jobs/historical_ingestion.py`
  - [ ] Run historical load
- [ ] Verify data loaded correctly
  - [ ] Query StarRocks
  - [ ] Check row counts

### Incremental Data Setup

- [ ] Configure Azure blob paths
- [ ] Test blob download
  - [ ] `python config/storage.py`
- [ ] Setup checkpoint directory
  - [ ] `data/data_incremental/checkpoint/`
- [ ] Initialize checkpoint files

## Phase 4: Code Migration

### Extract Code from Old Structure

- [ ] **Extractors** (from `utils/blob_downloader.py`, `scripts/blob_files_backup/`)
  - [ ] Implement `core/extractors/azure_blob_extractor.py`
  - [ ] Implement `core/extractors/parquet_reader.py`
  - [ ] Test extraction

- [ ] **Transformers** (from `utils/parquet_cleaner.py`, `pipeline_historical/`)
  - [ ] Implement `core/transformers/data_cleaner.py`
  - [ ] Implement `core/transformers/material_mapper.py` (from `pipeline_historical/6.material_mapper.py`)
  - [ ] Implement `core/transformers/rls_mapper.py` (from `pipeline_historical/5.rls_mapper.py`)
  - [ ] Test transformations

- [ ] **Loaders** (from `scripts/ingest_do_backup_incremental/`)
  - [ ] Implement `core/loaders/batch_loader.py`
  - [ ] Implement `core/loaders/incremental_loader.py`
  - [ ] Test loading

- [ ] **Jobs** (from `pipeline_historical/`, `pipeline_incremental/`)
  - [ ] Implement `core/jobs/historical_ingestion.py` (from `pipeline_historical/7.dd_insert.py`, `9.salesgroup_insert.py`)
  - [ ] Implement `core/jobs/dimension_refresh.py` (from `cron_jobs/daily/evening/daily_1_dimension_incremental.py`)
  - [ ] Implement `core/jobs/fact_ingestion_fis.py` (from `cron_jobs/daily/morning/daily_5_incremental_fis.py`)
  - [ ] Implement `core/jobs/fact_ingestion_fid.py` (from `cron_jobs/daily/morning/daily_6_incremental_fid.py`)
  - [ ] Implement `core/jobs/dd_logic.py` (from `cron_jobs/daily/morning/daily_7_dd_logic.py`)

### Migrate Utilities (from `cron_jobs/utils/incremental_utils.py`)

- [ ] Connection pooling → `utils/connection_pool.py`
- [ ] Retry handler → `utils/retry_handler.py`
- [ ] MongoDB client → `utils/mongo_client.py`
- [ ] Maintenance mode → `utils/maintenance_mode.py`

### Migrate Scheduler Jobs

- [ ] **Evening Jobs** (from `cron_jobs/daily/evening/`)
  - [ ] `scheduler/daily/evening/dimension_sync.py` (from `daily_1_dimension_incremental.py`)
  - [ ] `scheduler/daily/evening/tsr_hierarchy.py` (from `daily_2_update_tsr_rlsmaster.py`)
  - [ ] `scheduler/daily/evening/refresh_matviews.py` (from `daily_3_refresh_matview.py`)
  - [ ] `scheduler/daily/evening/business_constants.py` (from `daily_4_business_constants.py`)

- [ ] **Morning Jobs** (from `cron_jobs/daily/morning/`)
  - [ ] `scheduler/daily/morning/blob_backup.py` (from `daily_4_blob_files_backup.py`)
  - [ ] `scheduler/daily/morning/fis_incremental.py` (from `daily_5_incremental_fis.py`)
  - [ ] `scheduler/daily/morning/fid_incremental.py` (from `daily_6_incremental_fid.py`)
  - [ ] `scheduler/daily/morning/dd_logic.py` (from `daily_7_dd_logic.py`)

- [ ] **Daily Jobs** (from `cron_jobs/daily/`)
  - [ ] `scheduler/daily/summary.py` (from `daily_summary_incremental.py`)
  - [ ] `scheduler/daily/db_backup.py` (from `daily_db_backup.py`)
  - [ ] `scheduler/daily/maintenance_mode.py` (from `daily_disable_maintanencemode.py`)

- [ ] **Monthly Jobs** (from `cron_jobs/monthly/`)
  - [ ] `scheduler/monthly/vacuum_datawiz.py` (from `monthly_1_vacuum_datawiz.py`)

### Migrate Notifications (from `cron_jobs/service/`, `cron_jobs/utils/email/`)

- [ ] `notifications/email_service.py` (from `cron_jobs/service/email_service.py`)
- [ ] `notifications/utils/mjml_generator.py` (from `cron_jobs/utils/email/mjml_generator.py`)
- [ ] `notifications/utils/table_generators.py` (from `cron_jobs/utils/email/table_generators.py`)
- [ ] `notifications/utils/helpers.py` (from `cron_jobs/utils/email/helpers.py`)
- [ ] `notifications/utils/data_loader.py` (from `cron_jobs/utils/email/data_loader.py`)
- [ ] `notifications/handlers/mjml_handler.py` (from `cron_jobs/handler/mjml_handler.py`)

## Phase 5: Testing

- [ ] **Unit Tests**
  - [ ] Test extractors (`tests/unit/test_extractors.py`)
  - [ ] Test transformers (`tests/unit/test_transformers.py`)
  - [ ] Test loaders (`tests/unit/test_loaders.py`)
  - [ ] Test utilities (`tests/unit/test_utils.py`)

- [ ] **Integration Tests**
  - [ ] Test ETL pipeline (`tests/integration/test_etl_pipeline.py`)
  - [ ] Test database operations (`tests/integration/test_database.py`)
  - [ ] Test scheduler (`tests/integration/test_scheduler.py`)

- [ ] **Manual Tests**
  - [ ] Run evening jobs manually
  - [ ] Run morning jobs manually
  - [ ] Verify email notifications
  - [ ] Check logs

- [ ] **Data Validation**
  - [ ] Compare row counts (old vs new)
  - [ ] Verify data integrity
  - [ ] Check for duplicates
  - [ ] Validate business logic

## Phase 6: Scheduler Setup

- [ ] Review `scheduler/crontab.yaml`
- [ ] Update job schedules (timezone: Asia/Kolkata)
- [ ] Configure evening jobs (6 PM - 8 PM)
- [ ] Configure morning jobs (8 AM - 12 PM)
- [ ] Configure monthly jobs
- [ ] Implement `scheduler/orchestrator.py`
- [ ] Implement `scheduler/job_registry.py`
- [ ] Test scheduler with dry-run
- [ ] Enable scheduler in production

## Phase 7: Monitoring & Observability

- [ ] Setup logging
  - [ ] Test logging configuration
  - [ ] Verify log files created in `logs/`
  - [ ] Configure log rotation

- [ ] Setup OpenTelemetry (optional)
  - [ ] Configure SignOz endpoint
  - [ ] Implement tracing decorators
  - [ ] Test tracing

- [ ] Setup metrics (optional)
  - [ ] Implement custom metrics
  - [ ] Create dashboards
  - [ ] Test metrics collection

- [ ] Configure email notifications
  - [ ] Test email sending
  - [ ] Create email templates
  - [ ] Test failure notifications

## Phase 8: Documentation

- [ ] Update project README
- [ ] Document custom business logic
- [ ] Document data schemas
- [ ] Create runbook for operations
- [ ] Document troubleshooting steps
- [ ] Create deployment guide

## Phase 9: Deployment

- [ ] Setup production environment
  - [ ] Create `.env.production`
  - [ ] Configure production database
  - [ ] Setup production Azure storage

- [ ] Deploy code
  - [ ] Clone repository
  - [ ] Install dependencies
  - [ ] Copy production `.env`

- [ ] Initialize production database
  - [ ] Run migrations
  - [ ] Create tables
  - [ ] Create indexes

- [ ] Test in production
  - [ ] Run test job
  - [ ] Verify data loaded
  - [ ] Check logs

- [ ] Enable scheduler
  - [ ] Set `ENABLE_SCHEDULER=true`
  - [ ] Start orchestrator
  - [ ] Monitor first run

## Phase 10: Cleanup

- [ ] Archive old code
  - [ ] Move `pipeline_historical/` to `archive/`
  - [ ] Move `pipeline_incremental/` to `archive/`
  - [ ] Move old scripts to `scripts/legacy/`

- [ ] Remove old cron jobs
  - [ ] Disable old crontab entries
  - [ ] Archive old job files

- [ ] Clean up data directories
  - [ ] Archive old data files
  - [ ] Clean temp directories

- [ ] Update version control
  - [ ] Commit new structure
  - [ ] Tag release (v2.0.0)
  - [ ] Update changelog

## Phase 11: Monitoring & Maintenance

- [ ] Monitor first week
  - [ ] Check logs daily
  - [ ] Verify all jobs running
  - [ ] Review email notifications

- [ ] Performance tuning
  - [ ] Monitor query performance
  - [ ] Optimize slow queries
  - [ ] Adjust batch sizes

- [ ] Setup alerts
  - [ ] Disk space alerts
  - [ ] Job failure alerts
  - [ ] Data quality alerts

- [ ] Schedule regular maintenance
  - [ ] Weekly log review
  - [ ] Monthly database vacuum
  - [ ] Quarterly code review

## Verification Checklist

After migration, verify:

- [ ] All dimension tables loading correctly (evening)
- [ ] All fact tables loading correctly (morning)
- [ ] Incremental loading working (checkpoints)
- [ ] Email notifications sending
- [ ] Logs being written
- [ ] Database backups running
- [ ] No duplicate data
- [ ] Business logic correct
- [ ] Performance acceptable
- [ ] Monitoring working

## Rollback Plan

If migration fails:

- [ ] Document rollback procedure
- [ ] Keep old code accessible
- [ ] Maintain database backups
- [ ] Have fallback cron jobs ready

## Success Criteria

Migration is successful when:

- [ ] All jobs running on schedule
- [ ] Data loading correctly
- [ ] Notifications working
- [ ] Logs accessible
- [ ] Performance meets SLA
- [ ] Team trained on new structure
- [ ] Documentation complete

---

**Estimated Timeline**: 2-4 weeks depending on code complexity

**Priority**: High priority items first (database, core ETL), then nice-to-haves (monitoring, dashboards)
