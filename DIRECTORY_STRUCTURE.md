# Multi-Tenant Data Workflows - Directory Structure Reference

> **Last Updated:** 2025-01-15
> **Purpose:** Production-ready multi-tenant ETL pipeline for StarRocks
> **Scale:** Optimized for 2-5 tenants, scalable to 20+

---

## Complete Directory Structure

```
data-workflows/
│
├── core/                                    # SHARED CODE (business-agnostic)
│   ├── __init__.py
│   │
│   ├── extractors/                         # File extraction
│   │   ├── __init__.py
│   │   ├── csv_extractor.py                # Generic CSV reader
│   │   ├── excel_extractor.py              # Generic Excel reader (xlsx, xls)
│   │   └── azure_blob_extractor.py         # Generic Azure blob downloader
│   │
│   ├── transformers/                       # Data transformation
│   │   ├── __init__.py
│   │   ├── file_to_parquet.py              # Universal converter (CSV/Excel/Parquet)
│   │   │                                   # - Converts CSV → Parquet
│   │   │                                   # - Converts Excel → Parquet
│   │   │                                   # - Copies Parquet → raw_parquet
│   │   ├── dtype_transformer.py            # Generic dtype conversion
│   │   ├── column_mapper.py                # Generic column renaming
│   │   ├── roundoff_transformer.py         # Generic numeric rounding
│   │   └── transformation_engine.py        # Orchestrates all transformations
│   │
│   └── loaders/                            # Data loading
│       ├── __init__.py
│       └── starrocks_loader.py             # Generic StarRocks Stream Load API
│
├── orchestration/                           # MULTI-TENANT ORCHESTRATION
│   ├── __init__.py
│   ├── tenant_manager.py                   # TenantConfig & TenantManager classes
│   │                                       # - Loads tenant_registry.yaml
│   │                                       # - Manages tenant configurations
│   │                                       # - Provides tenant iteration
│   ├── tenant_job_runner.py                # Sequential job execution
│   │                                       # - Runs jobs tenant1 → tenant2 → tenant3
│   │                                       # - Handles failures gracefully
│   └── scheduler_integration.py            # Cron/APScheduler integration
│
├── configs/                                 # CONFIGURATION LAYER
│   ├── tenant_registry.yaml                # ⭐ Master list of all tenants
│   │                                       # - enabled/disabled status
│   │                                       # - tenant metadata
│   │                                       # - schedule priorities
│   │
│   ├── shared/                             # Shared defaults (all tenants)
│   │   ├── default_config.yaml             # Default settings
│   │   └── common_business_rules.yaml      # Common validation rules
│   │
│   ├── starrocks/                          # StarRocks-specific configs
│   │   ├── connection_pool.yaml            # Connection pool settings
│   │   └── stream_load_defaults.yaml       # Stream Load defaults
│   │
│   └── tenants/                            # ⭐ Per-tenant configurations
│       │
│       ├── tenant1/                        # Client 1 (e.g., "pidilite_mumbai")
│       │   ├── config.yaml                 # Tenant metadata (DB name, paths, etc.)
│       │   ├── .env                        # ⚠️ SECRETS (gitignored!)
│       │   │
│       │   ├── schemas/                    # Database schema definitions
│       │   │   ├── tables/
│       │   │   │   ├── 01_dim_material_mapping.py
│       │   │   │   ├── 02_dim_customer_master.py
│       │   │   │   ├── 03_dim_dealer_master.py
│       │   │   │   ├── 04_dim_hierarchy.py
│       │   │   │   ├── 05_dim_sales_group.py
│       │   │   │   ├── 06_dim_material.py
│       │   │   │   ├── 07_fact_invoice_details.py
│       │   │   │   └── 08_fact_invoice_secondary.py
│       │   │   ├── views/
│       │   │   │   ├── 01_secondary_sales_view.py
│       │   │   │   └── 02_primary_sales_view.py
│       │   │   └── matviews/
│       │   │       └── 01_secondary_sales_matview.py
│       │   │
│       │   ├── column_mappings/            # CSV → DB column mappings
│       │   │   ├── 01_dim_material_mapping.yaml
│       │   │   ├── 02_dim_customer_master.yaml
│       │   │   ├── 03_dim_dealer_master.yaml
│       │   │   ├── 04_dim_hierarchy.yaml
│       │   │   ├── 05_dim_sales_group.yaml
│       │   │   ├── 06_dim_material.yaml
│       │   │   ├── 07_fact_invoice_details.yaml
│       │   │   └── 08_fact_invoice_secondary.yaml
│       │   │
│       │   ├── computed_columns.yaml       # Computed column definitions
│       │   │
│       │   ├── business_logic/             # Business-specific logic
│       │   │   ├── business_constants.py   # Filter dimensions configuration
│       │   │   ├── validation_rules.py     # Custom validation rules
│       │   │   └── rls_config.py           # Row-level security policies
│       │   │
│       │   └── seeds/                      # Reference data
│       │       ├── SEED_MAPPING.py         # Seed table configuration
│       │       ├── DimMaterialMapping.csv
│       │       └── DimSalesGroup.csv
│       │
│       ├── tenant2/                        # Client 2
│       │   └── ... (same structure as tenant1)
│       │
│       ├── tenant3/                        # Client 3
│       │   └── ... (same structure)
│       │
│       └── _template/                      # ⭐ Template for new tenant onboarding
│           ├── config.yaml.template
│           ├── .env.template
│           ├── schemas/
│           │   ├── tables/
│           │   ├── views/
│           │   └── matviews/
│           ├── column_mappings/
│           ├── computed_columns.yaml
│           ├── business_logic/
│           └── seeds/
│
├── data/                                    # DATA LAYER (per-tenant isolation)
│   ├── tenant1/
│   │   ├── historical/
│   │   │   ├── source_files/               # 📥 Raw files (CSV, Excel, Parquet)
│   │   │   │                               # - Downloaded from Azure blob
│   │   │   │                               # - Decompressed (.gz → .csv/.xlsx)
│   │   │   ├── raw_parquet/                # 🟤 Bronze: Converted to parquet
│   │   │   │                               # - CSV → Parquet
│   │   │   │                               # - Excel → Parquet
│   │   │   │                               # - Parquet → Parquet (copy)
│   │   │   │                               # - NO transformations applied
│   │   │   └── cleaned_parquet/            # 🥈 Silver: Transformed & validated
│   │   │       ├── DimMaterialMapping.parquet
│   │   │       ├── FactInvoiceDetails.parquet
│   │   │       └── ...
│   │   │
│   │   ├── incremental/                    # Same structure as historical
│   │   │   ├── source_files/
│   │   │   ├── raw_parquet/
│   │   │   └── cleaned_parquet/
│   │   │
│   │   └── temp/                           # Temporary processing files
│   │       └── ... (auto-cleaned)
│   │
│   ├── tenant2/
│   │   └── ... (same structure)
│   │
│   ├── tenant3/
│   │   └── ... (same structure)
│   │
│   └── .gitkeep                            # Track empty directory in git
│
├── logs/                                    # LOGS (per-tenant isolation)
│   ├── tenant1/
│   │   ├── scheduler/                      # Cron job logs
│   │   │   ├── evening_dimension_sync.log
│   │   │   ├── morning_fis_incremental.log
│   │   │   └── ...
│   │   ├── etl/                            # ETL pipeline logs
│   │   │   ├── extraction.log
│   │   │   ├── transformation.log
│   │   │   └── loading.log
│   │   └── notifications/                  # Email notification logs
│   │
│   ├── tenant2/
│   │   └── ... (same structure)
│   │
│   └── tenant3/
│       └── ... (same structure)
│
├── utils/                                   # UTILITIES
│   ├── __init__.py
│   ├── etl_orchestrator.py                 # Pipeline executor (tenant-aware)
│   │                                       # - Extract → Transform → Load
│   │                                       # - Accepts TenantConfig parameter
│   ├── schema_validator.py                 # Schema validation
│   ├── pipeline_config.py                  # Config loader helpers
│   └── logging_config.py                   # Logging setup (per-tenant)
│
├── db/                                      # DATABASE MANAGEMENT SCRIPTS
│   ├── __init__.py
│   ├── create_tables.py                    # ⭐ Table creation (tenant-aware)
│   │                                       # - Loads from configs/tenants/{id}/schemas/
│   │                                       # - Shows which tenant is being processed
│   ├── load_seed_data.py                   # Seed data loader (tenant-aware)
│   ├── populate_business_constants.py      # Business constants (tenant-aware)
│   └── migrations/                         # Schema migration scripts
│       └── README.md
│
├── scheduler/                               # JOB SCHEDULER
│   ├── __init__.py
│   ├── orchestrator.py                     # ⭐ Main scheduler (updated)
│   │                                       # - Iterates over enabled tenants
│   │                                       # - Calls tenant_job_runner
│   ├── crontab.yaml                        # Job schedules
│   │
│   └── daily/
│       ├── __init__.py
│       │
│       ├── evening/                        # Evening jobs (6-8 PM)
│       │   ├── __init__.py
│       │   ├── dimension_sync.py           # ⭐ Accepts tenant_config param
│       │   ├── tsr_hierarchy.py
│       │   ├── refresh_matviews.py
│       │   └── business_constants.py
│       │
│       └── morning/                        # Morning jobs (8 AM - 12 PM)
│           ├── __init__.py
│           ├── blob_backup.py
│           ├── fis_incremental.py          # ⭐ Accepts tenant_config param
│           ├── fid_incremental.py
│           └── dd_logic.py
│
├── config/                                  # LEGACY CONFIG (backward compatibility)
│   ├── __init__.py
│   ├── settings.py                         # ⭐ Updated to support tenant context
│   ├── database.py                         # ⭐ Updated with tenant-aware pooling
│   ├── logging_config.py                   # Updated for per-tenant logs
│   └── storage.py                          # Azure storage helpers
│
├── scripts/                                 # UTILITY SCRIPTS
│   ├── onboard_tenant.sh                   # ⭐ Tenant onboarding automation
│   │                                       # - Copies _template/
│   │                                       # - Prompts for config
│   │                                       # - Creates database
│   ├── validate_tenant_config.py           # Config validation
│   └── migrate_single_to_multi.py          # Migration helper
│
├── tests/                                   # TESTS
│   ├── __init__.py
│   ├── test_core/
│   │   ├── test_extractors.py
│   │   ├── test_transformers.py
│   │   └── test_loaders.py
│   ├── test_orchestration/
│   │   ├── test_tenant_manager.py
│   │   └── test_tenant_job_runner.py
│   └── test_integration/
│       └── test_end_to_end.py
│
├── docker-compose.yml                       # StarRocks infrastructure
├── Dockerfile                               # Application container
├── .env                                     # Global env vars (optional)
├── .gitignore                               # ⚠️ MUST ignore configs/tenants/*/.env
├── requirements.txt                         # Python dependencies
├── README.md                                # Project documentation
├── MULTI_TENANT_SETUP.md                    # Multi-tenant onboarding guide
└── DIRECTORY_STRUCTURE.md                   # ⭐ This file
```

---

## Data Flow: Source Files → StarRocks

### Stage 1: Extraction (source_files/)
```
Azure Blob Storage
    ↓ (download)
DimMaterialMapping.csv.gz
    ↓ (decompress)
data/tenant1/historical/source_files/DimMaterialMapping.csv
```

### Stage 2: Raw Conversion (raw_parquet/) - Bronze Layer
```
core/transformers/file_to_parquet.py

Input: source_files/DimMaterialMapping.csv
Output: raw_parquet/DimMaterialMapping.parquet
Action: Convert CSV → Parquet (NO transformations)

Input: source_files/FactInvoice.xlsx
Output: raw_parquet/FactInvoice.parquet
Action: Convert Excel → Parquet (NO transformations)

Input: source_files/AlreadyParquet.parquet
Output: raw_parquet/AlreadyParquet.parquet
Action: Copy file (NO conversion needed)
```

### Stage 3: Transformation (cleaned_parquet/) - Silver Layer
```
core/transformers/transformation_engine.py

Input: raw_parquet/DimMaterialMapping.parquet
Actions:
  1. Load column_mappings/01_dim_material_mapping.yaml
  2. Rename columns (materialcode → material_code)
  3. Convert dtypes (STRING → INT, FLOAT → DECIMAL)
  4. Apply roundoff transformations
  5. Generate computed columns
  6. Validate schema
Output: cleaned_parquet/DimMaterialMapping.parquet
```

### Stage 4: Loading (StarRocks)
```
core/loaders/starrocks_loader.py

Input: cleaned_parquet/DimMaterialMapping.parquet
Action: Stream Load to datawiz_tenant1.dim_material_mapping
Protocol: HTTP Stream Load API
```

---

## Key File Descriptions

### `core/transformers/file_to_parquet.py`
**Purpose:** Universal file → parquet converter

**Supported Input Formats:**
- CSV files (`.csv`)
- Excel files (`.xlsx`, `.xls`)
- Parquet files (`.parquet`) - copy only

**Behavior:**
```python
def convert_to_parquet(source_file, output_dir, tenant_config):
    """
    Converts any file type to parquet format.

    Args:
        source_file: Path to source file (CSV/Excel/Parquet)
        output_dir: Path to raw_parquet/ directory
        tenant_config: TenantConfig object

    Returns:
        Path to output parquet file

    Logic:
        IF file is CSV:
            - Use polars.scan_csv()
            - Convert to parquet with row_groups=100,000
        ELIF file is Excel:
            - Use polars.read_excel()
            - Convert to parquet with row_groups=100,000
        ELIF file is Parquet:
            - Copy file to output_dir (no conversion)
        ELSE:
            - Raise UnsupportedFileTypeError
    """
```

### `configs/tenant_registry.yaml`
**Purpose:** Master registry of all tenants

```yaml
tenants:
  - tenant_id: tenant1
    tenant_name: "Pidilite Mumbai Operations"
    enabled: true                           # Job runner will process
    database_name: "datawiz_tenant1"
    database_user: "tenant1_admin"
    azure_container: "pidilite-mumbai-prod"
    azure_folder_prefix: "synapse_data/"
    schedule_priority: 1                    # Lower = higher priority
    tier: premium

  - tenant_id: tenant2
    tenant_name: "Pidilite Delhi Operations"
    enabled: true
    database_name: "datawiz_tenant2"
    database_user: "tenant2_admin"
    azure_container: "pidilite-delhi-prod"
    azure_folder_prefix: "data_exports/"
    schedule_priority: 2
    tier: standard

  - tenant_id: tenant3
    tenant_name: "Pidilite South Region"
    enabled: false                          # NOT processed by job runner
    database_name: "datawiz_tenant3"
    database_user: "tenant3_admin"
    azure_container: "pidilite-south-prod"
    schedule_priority: 3
    tier: standard

global_config:
  max_concurrent_tenants: 1                 # Sequential execution
  tenant_timeout: 7200                      # 2 hours per tenant
  fail_fast: false                          # Continue on failure
  shared_starrocks_cluster: true
```

### `configs/tenants/tenant1/config.yaml`
**Purpose:** Tenant-specific configuration (non-sensitive)

```yaml
tenant_id: tenant1
tenant_name: "Pidilite Mumbai Operations"
enabled: true

# Database Configuration (passwords in .env)
database:
  database_name: "datawiz_tenant1"
  user: "tenant1_admin"
  host: "localhost"
  port: 9030
  http_port: 8040

# Azure Blob Storage (connection string in .env)
azure:
  container_name: "pidilite-mumbai-prod"
  folder_prefix: "synapse_data/"

# Data Paths (relative to project root)
data_paths:
  historical: "data/tenant1/data_historical"
  incremental: "data/tenant1/data_incremental"
  temp: "data/tenant1/temp"

# Logs (relative to project root)
logs:
  base_path: "logs/tenant1"

# Business Rules
business_rules:
  date_filter_start: "20230401"             # FactInvoiceSecondary filter
  sales_threshold: 10000                    # Business constants filter
  material_type_filter: ["ZFGD"]            # Secondary sales material type

# Scheduler Configuration
scheduler:
  timezone: "Asia/Kolkata"
  enable_evening_jobs: true
  enable_morning_jobs: true
  evening_start_time: "18:00"               # 6:00 PM
  morning_start_time: "09:00"               # 9:00 AM

# Observability
observability:
  service_name: "datawiz-tenant1"
  enable_tracing: true
  enable_metrics: true
```

### `configs/tenants/tenant1/.env`
**Purpose:** Tenant secrets (⚠️ NEVER COMMIT!)

```bash
# Database Credentials
DB_PASSWORD=tenant1_secure_password_here

# Azure Storage
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=...
AZURE_SAS_TOKEN=sv=2023-01-01&st=2025-01-15...

# MongoDB (if used for business constants)
MONGODB_URI=mongodb://tenant1_user:tenant1_pass@localhost:27017

# Email Notifications
SMTP_PASSWORD=email_password_here
EMAIL_RECIPIENTS=tenant1-admin@pidilite.com,ops@pidilite.com
```

---

## Configuration Hierarchy

```
Global Defaults (configs/shared/default_config.yaml)
    ↓ (overridden by)
Tenant Config (configs/tenants/tenant1/config.yaml)
    ↓ (secrets from)
Tenant Secrets (configs/tenants/tenant1/.env)
```

---

## Tenant Onboarding Process

### Step 1: Copy Template
```bash
cp -r configs/tenants/_template configs/tenants/tenant_new
```

### Step 2: Configure Tenant
```bash
# Edit config.yaml
vim configs/tenants/tenant_new/config.yaml

# Add secrets to .env
vim configs/tenants/tenant_new/.env
```

### Step 3: Register Tenant
```bash
# Add entry to tenant_registry.yaml
vim configs/tenant_registry.yaml
```

### Step 4: Create Database
```sql
CREATE DATABASE datawiz_tenant_new;
CREATE USER 'tenant_new_admin'@'%' IDENTIFIED BY 'password';
GRANT ALL PRIVILEGES ON datawiz_tenant_new.* TO 'tenant_new_admin'@'%';
```

### Step 5: Initialize Schema
```bash
python db/create_tables.py --tenant tenant_new
python db/load_seed_data.py --tenant tenant_new
```

### Step 6: Create Data Directories
```bash
mkdir -p data/tenant_new/{historical,incremental}/{source_files,raw_parquet,cleaned_parquet}
mkdir -p data/tenant_new/temp
mkdir -p logs/tenant_new/{scheduler,etl,notifications}
```

### Step 7: Enable Tenant
```yaml
# In tenant_registry.yaml
tenants:
  - tenant_id: tenant_new
    enabled: true  # Change to true
```

---

## Orchestration Flow

### Job Execution (Evening Dimension Sync Example)

```
1. scheduler/orchestrator.py starts
    ↓
2. Load tenant_registry.yaml
    ↓
3. Initialize TenantManager
    ↓
4. Get enabled tenants: [tenant1, tenant2]
    ↓
5. For each tenant (sequential):

    TENANT 1:
    ├─ Load configs/tenants/tenant1/config.yaml
    ├─ Load configs/tenants/tenant1/.env
    ├─ Create TenantConfig object
    ├─ Call dimension_sync(tenant_config)
    │   ├─ Download from Azure (pidilite-mumbai-prod)
    │   ├─ Save to data/tenant1/historical/source_files/
    │   ├─ Convert to parquet → raw_parquet/
    │   ├─ Transform → cleaned_parquet/
    │   ├─ Load to datawiz_tenant1 database
    │   └─ Log to logs/tenant1/scheduler/
    └─ Mark complete

    TENANT 2:
    ├─ Load configs/tenants/tenant2/config.yaml
    ├─ Load configs/tenants/tenant2/.env
    ├─ Create TenantConfig object
    ├─ Call dimension_sync(tenant_config)
    │   ├─ Download from Azure (pidilite-delhi-prod)
    │   ├─ Save to data/tenant2/historical/source_files/
    │   ├─ Convert to parquet → raw_parquet/
    │   ├─ Transform → cleaned_parquet/
    │   ├─ Load to datawiz_tenant2 database
    │   └─ Log to logs/tenant2/scheduler/
    └─ Mark complete

6. All tenants processed
```

---

## Security Best Practices

### 1. Secrets Management
- ✅ Store secrets in `.env` files per tenant
- ✅ Add `configs/tenants/*/.env` to `.gitignore`
- ✅ Never commit passwords, tokens, or connection strings
- ⚠️ Consider HashiCorp Vault for production

### 2. Database Isolation
- ✅ Separate database per tenant (`datawiz_tenant1`, `datawiz_tenant2`)
- ✅ Separate database users per tenant
- ✅ Tenant-aware connection pooling (no cross-contamination)

### 3. Data Isolation
- ✅ Separate data directories per tenant (`data/tenant1/`, `data/tenant2/`)
- ✅ Separate log directories per tenant (`logs/tenant1/`, `logs/tenant2/`)
- ✅ No shared file paths between tenants

### 4. Access Control
- ✅ RLS (Row-Level Security) policies in `business_logic/rls_config.py`
- ✅ Territory-based filtering (`wss_territory_code`)
- ✅ Role-based access control

---

## Scalability Considerations

### 2-5 Tenants (Current)
- **Config Structure:** Flat (`configs/tenants/tenant1/`)
- **Execution:** Sequential (one tenant at a time)
- **Complexity:** Low
- **Maintenance:** Simple

### 6-10 Tenants (Growth)
- **Config Structure:** Still flat, consider tiering
- **Execution:** Parallel with limits (max 3 concurrent)
- **Complexity:** Medium
- **Maintenance:** Moderate

### 10+ Tenants (Enterprise)
- **Config Structure:** Nested by tier (`tier_1/tenant1/`, `tier_2/tenant5/`)
- **Execution:** Parallel with resource pools
- **Complexity:** High
- **Maintenance:** Requires automation

---

## Key Design Principles

1. **DRY (Don't Repeat Yourself)**
   - Core transformers are SHARED (no tenant prefixes)
   - Business logic is TENANT-SPECIFIC (in configs)

2. **Separation of Concerns**
   - Code in `core/` (business-agnostic)
   - Config in `configs/tenants/` (business-specific)
   - Data in `data/` (per-tenant isolation)

3. **Single Source of Truth**
   - `tenant_registry.yaml` = master tenant list
   - `config.yaml` = tenant configuration
   - `.env` = tenant secrets

4. **Fail-Safe Defaults**
   - Continue to next tenant on failure (`fail_fast: false`)
   - Timeout per tenant (2 hours)
   - Comprehensive logging

5. **Security by Design**
   - Separate databases per tenant
   - Separate credentials per tenant
   - Secrets in `.env` files (gitignored)

---

## Common Operations

### Check Active Tenants
```bash
python -c "from orchestration.tenant_manager import TenantManager; \
           tm = TenantManager('configs'); \
           print([t.tenant_id for t in tm.get_all_enabled_tenants()])"
```

### Run Job for Single Tenant
```bash
python scheduler/daily/evening/dimension_sync.py --tenant tenant1
```

### Run Job for All Tenants
```bash
python scheduler/orchestrator.py --job evening_dimension_sync
```

### Validate Tenant Config
```bash
python scripts/validate_tenant_config.py --tenant tenant1
```

### Disable Tenant
```yaml
# Edit tenant_registry.yaml
tenants:
  - tenant_id: tenant1
    enabled: false  # Change to false
```

---

## Migration Path (Single → Multi-Tenant)

### Current State (Single Tenant)
```
db/schemas/
db/column_mappings/
db/seeds/
```

### Target State (Multi-Tenant)
```
configs/tenants/tenant1/schemas/
configs/tenants/tenant1/column_mappings/
configs/tenants/tenant1/seeds/
```

### Migration Script
```bash
python scripts/migrate_single_to_multi.py --tenant-id tenant1
```

---

## Related Documentation

- **README.md** - Project overview
- **MULTI_TENANT_SETUP.md** - Detailed setup guide
- **configs/tenants/_template/** - Onboarding template
- **Plan File:** `/home/rahul/.claude/plans/jolly-noodling-zephyr.md`

---

**End of Directory Structure Reference**
