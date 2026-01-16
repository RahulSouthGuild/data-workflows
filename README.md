# StarRocks Multi-Tenant ETL Pipeline

Production-grade ETL pipeline for loading multi-tenant data from Azure Blob Storage into StarRocks database with comprehensive data validation, transformation, and Row-Level Security (RLS).

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Documentation](#documentation)
- [Recent Improvements](#recent-improvements)
- [Project Structure](#project-structure)

---

## Overview

This ETL pipeline provides:

- **Multi-Tenant Support**: Separate databases per tenant with isolated data and configurations
- **Azure Integration**: Async blob download with retry logic and automatic decompression
- **Data Transformation**: Column mapping, type conversion, computed columns, and data cleaning
- **Schema Validation**: Strict type checking with automatic VARCHAR overflow handling
- **Row-Level Security**: Automated RLS policy injection based on user roles
- **Observability**: Structured logging with optional Signoz integration
- **Production Quality**: Comprehensive error handling, progress tracking, and clean logs

---

## Architecture

### ETL Pipeline Flow

```
Azure Blob Storage
    ↓ (async download + decompress)
CSV Files
    ↓ (convert to parquet)
Parquet Files
    ↓ (transform + validate)
Clean Data
    ↓ (stream load)
StarRocks Database (per tenant)
```

### Core Components

- **Extractors** (`core/extractors/`): Azure blob download and decompression
- **Transformers** (`core/transformers/`): Column mapping, type conversion, computed columns
- **Loaders** (`core/loaders/`): StarRocks Stream Load with RLS injection
- **Orchestration** (`orchestration/`): Tenant-aware job management
- **Schedulers** (`scheduler/tenants/`): Per-tenant daily jobs (morning/evening)

---

## Quick Start

### Prerequisites

- Python 3.9+
- StarRocks database
- Azure Storage Account with SAS token or connection string

### Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Set up tenant configuration
cp configs/tenants/_template configs/tenants/your_tenant
# Edit configs/tenants/your_tenant/config.yaml and .env
```

### Run a Job

```bash
# Morning dimension incremental load
python scheduler/tenants/pidilite/daily/morning/01_dimensions_incremental.py

# Fact invoice secondary incremental load
python scheduler/tenants/pidilite/daily/morning/02_fact_invoice_secondary.py

# Evening dimension full refresh
python scheduler/tenants/pidilite/daily/evening/01_dimensions_refresh.py
```

---

## Documentation

### Week 1: Initial Setup
- [**WEEK1_CONFIG_PLAN.md**](WEEK1_CONFIG_PLAN.md) - Multi-tenant architecture design and configuration strategy
- [**WEEK1_COMPLETION_REPORT.md**](WEEK1_COMPLETION_REPORT.md) - Initial implementation summary

### Week 3: Production Fixes
- [**WEEK3_ATTRIBUTE_FIXES.md**](WEEK3_ATTRIBUTE_FIXES.md) - TenantConfig attribute fixes for database connection
- [**WEEK3_AZURE_PATH_FIX.md**](WEEK3_AZURE_PATH_FIX.md) - Azure blob path resolution fixes
- [**WEEK3_COLUMN_ORDER_FIX.md**](WEEK3_COLUMN_ORDER_FIX.md) - Critical column order alignment for Stream Load ⚠️
- [**WEEK3_LOGGING_PHASE3_COMPLETE.md**](WEEK3_LOGGING_PHASE3_COMPLETE.md) - Comprehensive logging improvements (96% reduction)

### Reference
- [**DIRECTORY_STRUCTURE.md**](DIRECTORY_STRUCTURE.md) - Complete directory structure and file organization
- [**PROJECT_SUMMARY.md**](PROJECT_SUMMARY.md) - High-level project overview

---

## Recent Improvements

### ✅ Critical Fixes (Week 3)

1. **Column Order Alignment** ([WEEK3_COLUMN_ORDER_FIX.md](WEEK3_COLUMN_ORDER_FIX.md))
   - Fixed data corruption issue where columns were loaded in wrong order
   - DataFrame now reordered to match database schema before Stream Load
   - **Impact**: Prevented data integrity issues across all tables

2. **Logging Optimization** ([WEEK3_LOGGING_PHASE3_COMPLETE.md](WEEK3_LOGGING_PHASE3_COMPLETE.md))
   - Reduced log volume by 96% (280 lines → 12 lines per file)
   - Added tqdm progress bars for visual feedback
   - Implemented "silence is success" logging philosophy
   - **Impact**: Production-quality logs, easier debugging, better UX

3. **Tenant Configuration Fixes** ([WEEK3_ATTRIBUTE_FIXES.md](WEEK3_ATTRIBUTE_FIXES.md))
   - Fixed database connection attributes in TenantConfig
   - Added proper attribute accessors for all config values
   - **Impact**: Multi-tenant jobs now work correctly

4. **Azure Path Resolution** ([WEEK3_AZURE_PATH_FIX.md](WEEK3_AZURE_PATH_FIX.md))
   - Fixed blob path construction for both tenants
   - Corrected folder structure and file discovery
   - **Impact**: All Azure blob downloads work correctly

---

## Project Structure

```
data-workflows/
├── configs/                      # Tenant configurations
│   └── tenants/
│       ├── pidilite/            # Pidilite tenant config
│       └── uthra-global/        # Uthra Global tenant config
│
├── core/                        # Core ETL components (shared)
│   ├── extractors/              # Azure blob download
│   ├── transformers/            # Data transformation engine
│   └── loaders/                 # StarRocks Stream Load
│
├── orchestration/               # Multi-tenant orchestration
│   └── tenant_manager.py        # Tenant config loader
│
├── scheduler/                   # Job schedulers
│   └── tenants/
│       ├── pidilite/daily/      # Pidilite daily jobs
│       │   ├── morning/         # Incremental loads
│       │   └── evening/         # Full refreshes
│       └── uthra-global/daily/  # Uthra Global daily jobs
│
├── utils/                       # Utility functions
│   ├── etl_orchestrator.py      # ETL pipeline orchestrator
│   ├── schema_validator.py      # Schema validation
│   ├── blob_processor_utils.py  # Async blob processing
│   └── dim_transform_utils.py   # Dimension transformations
│
├── db/                          # Shared database schemas
│   ├── schemas/                 # Table/view/matview definitions
│   ├── column_mappings/         # CSV → DB column mappings
│   ├── computed_columns.json    # Computed column definitions
│   └── seeds/                   # Reference data
│
├── data/                        # Per-tenant data storage
│   ├── pidilite/
│   │   ├── historical/          # Historical data loads
│   │   └── incremental/         # Daily incremental loads
│   └── uthra-global/
│
└── logs/                        # Per-tenant logs
    ├── pidilite/
    └── uthra-global/
```

See [DIRECTORY_STRUCTURE.md](DIRECTORY_STRUCTURE.md) for complete details.

---

## Key Features

### 🎯 Multi-Tenant Architecture
- Separate database per tenant (`datawiz_pidilite`, `datawiz_uthra_global`)
- Isolated data paths, logs, and configurations
- Tenant-aware job orchestration

### 🔄 Data Pipeline
- **Extract**: Async Azure blob download with retry logic
- **Transform**: Column mapping, type conversion, computed columns, filtering
- **Validate**: Schema validation, type checking, overflow detection
- **Load**: StarRocks Stream Load with chunking and progress tracking

### 🔒 Row-Level Security (RLS)
- Automated RLS policy injection based on territory/region
- User role-based data filtering
- Configurable RLS columns per view

### 📊 Data Quality
- Type validation with strict mode
- Numeric overflow detection
- VARCHAR auto-expansion with ALTER TABLE
- Computed columns for composite keys

### 📝 Clean Logging
- tqdm progress bars for transformations
- Smart chunk logging (every 10th chunk)
- "Silence is success" - only log warnings/errors
- 96% log reduction vs verbose mode

---

## Configuration

### Tenant Configuration Structure

```
configs/tenants/pidilite/
├── config.yaml              # Tenant metadata and settings
├── .env                     # Secrets (DB password, Azure tokens)
├── schemas/                 # Table/view definitions
├── column_mappings/         # CSV → DB column mappings
├── computed_columns.json    # Computed column rules
└── seeds/                   # Reference data
```

### Environment Variables (.env)

```bash
# Database
DB_PASSWORD=your_secure_password

# Azure Storage
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;...
# OR
AZURE_ACCOUNT_URL=https://account.blob.core.windows.net
AZURE_SAS_TOKEN=sv=2023-01-01&st=2025-01-15...
```

---

## Development Guidelines

### Adding a New Tenant

1. Copy template: `cp -r configs/tenants/_template configs/tenants/new_tenant`
2. Edit `config.yaml` with tenant details
3. Update `.env` with secrets
4. Customize schemas and column mappings
5. Create StarRocks database: `CREATE DATABASE datawiz_new_tenant;`
6. Run initialization: `python db/create_tables.py --tenant new_tenant`

### Adding a New Table

1. Create schema in `db/schemas/tables/XX_TableName.py`
2. Create column mapping in `db/column_mappings/XX_TableName.json`
3. Add computed columns to `db/computed_columns.json` (if needed)
4. Test with sample data

### Logging Best Practices

- ✅ Log warnings and errors
- ✅ Log progress milestones (every 10th chunk/blob)
- ✅ Log summaries (counts, totals)
- ❌ Don't log individual successful operations
- ❌ Don't log expected behavior
- ❌ Don't log redundant announcements

---

## Testing

### Run Test Suite

```bash
# Test tenant manager
python test_tenant_manager.py

# Test Week 2 components
python test_week2_components.py
python test_week2_simple.py
```

### Manual Testing

```bash
# Test dimension load
python scheduler/tenants/pidilite/daily/morning/01_dimensions_incremental.py

# Check logs
tail -f logs/pidilite/etl/pipeline_*.log
```

---

## Troubleshooting

### Column Order Mismatch
**Symptom**: Data appears in wrong columns (e.g., `active_flag` has dealer names)

**Solution**: Fixed in [WEEK3_COLUMN_ORDER_FIX.md](WEEK3_COLUMN_ORDER_FIX.md) - DataFrame reordered to match DB schema

### Azure Blob Not Found
**Symptom**: "Blob not found" errors during download

**Solution**: Check blob path construction in [WEEK3_AZURE_PATH_FIX.md](WEEK3_AZURE_PATH_FIX.md)

### Tenant Config Errors
**Symptom**: AttributeError when accessing tenant config

**Solution**: See [WEEK3_ATTRIBUTE_FIXES.md](WEEK3_ATTRIBUTE_FIXES.md) for TenantConfig attribute fixes

---

## License

Proprietary - Internal use only

---

## Contact

For questions or issues, contact the data engineering team.

---

**Last Updated**: 2026-01-17
**Version**: Week 3 Complete
**Status**: ✅ Production Ready
