# Scheduler Structure & Data Folder Path Fixes

## Current State Analysis

### Scheduler Structure (Option 2: Tenant-Specific Jobs) ✅
```
scheduler/
├── crontab.yaml
├── __init__.py
└── tenants/                              # Tenant-specific jobs
    ├── pidilite/
    │   └── daily/
    │       ├── evening/
    │       │   └── 01_dimensions_incremental.py
    │       └── morning/
    │           ├── 02_fact_invoice_secondary.py
    │           ├── 03_fact_invoice_details.py
    │           └── 04_dd_logic.py
    └── uthra-global/
        └── daily/
            ├── evening/
            │   └── 01_dimensions_refresh.py
            └── morning/
                ├── 01_dimensions_incremental.py
                ├── 02_fact_invoice_secondary.py
                ├── 03_fact_invoice_details.py
                └── 04_business_logic_dd.py
```

**Status**: ✅ This structure is CORRECT for tenant-specific business logic.

---

## Data Folder Path Issues

### Issue 1: Inconsistent Folder Names ⚠️

**DIRECTORY_STRUCTURE.md specifies:**
```
data/{tenant}/incremental/
├── source_files/       # Downloaded raw files
├── raw_parquet/        # Singular - Bronze layer
└── cleaned_parquet/    # Singular - Silver layer
```

**Current code uses:**
```python
# scheduler/tenants/pidilite/daily/morning/02_fact_invoice_secondary.py:119
output_dir = tenant_config.data_incremental_path / "raw_parquets"  # WRONG: plural
```

**Fix needed:**
```python
output_dir = tenant_config.data_incremental_path / "raw_parquet"   # Correct: singular
```

### Issue 2: Missing Subdirectory Properties in TenantConfig

**Current TenantConfig properties:**
```python
@property
def data_incremental_path(self) -> Path:
    """Returns: data/{tenant_slug}/incremental/"""
    return self.data_base_path / "incremental"
```

**Missing properties for subdirectories:**
- `data_incremental_source_path` → `data/{tenant}/incremental/source_files/`
- `data_incremental_raw_path` → `data/{tenant}/incremental/raw_parquet/`
- `data_incremental_cleaned_path` → `data/{tenant}/incremental/cleaned_parquet/`

**Same for historical:**
- `data_historical_source_path` → `data/{tenant}/historical/source_files/`
- `data_historical_raw_path` → `data/{tenant}/historical/raw_parquet/`
- `data_historical_cleaned_path` → `data/{tenant}/historical/cleaned_parquet/`

---

## Proposed Fixes

### Fix 1: Add Subdirectory Properties to TenantConfig

**File**: `orchestration/tenant_manager.py`

Add these properties after line 270:

```python
# Incremental data subdirectories
@property
def data_incremental_source_path(self) -> Path:
    """Source files directory for incremental loads."""
    return self.data_incremental_path / "source_files"

@property
def data_incremental_raw_path(self) -> Path:
    """Raw parquet directory for incremental loads (Bronze layer)."""
    return self.data_incremental_path / "raw_parquet"

@property
def data_incremental_cleaned_path(self) -> Path:
    """Cleaned parquet directory for incremental loads (Silver layer)."""
    return self.data_incremental_path / "cleaned_parquet"

# Historical data subdirectories
@property
def data_historical_source_path(self) -> Path:
    """Source files directory for historical loads."""
    return self.data_historical_path / "source_files"

@property
def data_historical_raw_path(self) -> Path:
    """Raw parquet directory for historical loads (Bronze layer)."""
    return self.data_historical_path / "raw_parquet"

@property
def data_historical_cleaned_path(self) -> Path:
    """Cleaned parquet directory for historical loads (Silver layer)."""
    return self.data_historical_path / "cleaned_parquet"
```

### Fix 2: Update Scheduler Jobs to Use New Properties

**Update all scheduler jobs** to use the new properties:

```python
# BEFORE (incorrect):
output_dir = tenant_config.data_incremental_path / "raw_parquets"

# AFTER (correct):
output_dir = tenant_config.data_incremental_raw_path

# OR for source files:
source_dir = tenant_config.data_incremental_source_path

# OR for cleaned parquet:
cleaned_dir = tenant_config.data_incremental_cleaned_path
```

**Files to update:**
1. `scheduler/tenants/pidilite/daily/morning/02_fact_invoice_secondary.py`
2. `scheduler/tenants/pidilite/daily/morning/03_fact_invoice_details.py`
3. `scheduler/tenants/pidilite/daily/evening/01_dimensions_incremental.py`
4. All corresponding `uthra-global` scheduler jobs

### Fix 3: Update ETLOrchestrator to Use New Properties

**File**: `utils/etl_orchestrator.py`

Ensure ETL Orchestrator uses:
- `tenant_config.data_incremental_raw_path` for reading raw parquet
- `tenant_config.data_incremental_cleaned_path` for writing cleaned parquet

### Fix 4: Update blob_processor_utils.py

**File**: `utils/blob_processor_utils.py`

Ensure blob processor downloads to:
- `tenant_config.data_incremental_source_path` for source files (CSV/Excel from Azure)
- `tenant_config.data_incremental_raw_path` for converted parquet files

---

## Complete Data Flow with Correct Paths

### Stage 1: Download from Azure → source_files/
```
Azure Blob: Incremental/FactInvoiceSecondary/LatestData/file.csv.gz
    ↓ (download & decompress)
data/pidilite/incremental/source_files/file.csv
```

### Stage 2: Convert to Parquet → raw_parquet/ (Bronze)
```
core/transformers/file_to_parquet.py
Input:  data/pidilite/incremental/source_files/file.csv
Output: data/pidilite/incremental/raw_parquet/file.parquet
Action: CSV → Parquet (NO transformations)
```

### Stage 3: Transform → cleaned_parquet/ (Silver)
```
core/transformers/transformation_engine.py
Input:  data/pidilite/incremental/raw_parquet/file.parquet
Actions: Column mapping, dtype conversion, computed columns
Output: data/pidilite/incremental/cleaned_parquet/FactInvoiceSecondary.parquet
```

### Stage 4: Load to StarRocks
```
core/loaders/starrocks_stream_loader.py
Input: data/pidilite/incremental/cleaned_parquet/FactInvoiceSecondary.parquet
Load to: pidilite_db.fact_invoice_secondary
```

---

## Implementation Checklist

- [ ] Add subdirectory properties to `TenantConfig` (orchestration/tenant_manager.py)
- [ ] Update `02_fact_invoice_secondary.py` to use `data_incremental_raw_path`
- [ ] Update `03_fact_invoice_details.py` to use `data_incremental_raw_path`
- [ ] Update `01_dimensions_incremental.py` to use `data_incremental_raw_path`
- [ ] Update all uthra-global scheduler jobs
- [ ] Update `etl_orchestrator.py` to use new properties
- [ ] Update `blob_processor_utils.py` to use new properties
- [ ] Test data flow end-to-end
- [ ] Create data directories:
  ```bash
  mkdir -p data/pidilite/incremental/{source_files,raw_parquet,cleaned_parquet}
  mkdir -p data/pidilite/historical/{source_files,raw_parquet,cleaned_parquet}
  mkdir -p data/uthra-global/incremental/{source_files,raw_parquet,cleaned_parquet}
  mkdir -p data/uthra-global/historical/{source_files,raw_parquet,cleaned_parquet}
  ```

---

## Benefits of These Fixes

1. ✅ **Consistency**: All jobs use same folder structure
2. ✅ **Clarity**: Properties make intent clear (`data_incremental_raw_path`)
3. ✅ **Maintainability**: Change folder name in one place (TenantConfig)
4. ✅ **Follows DIRECTORY_STRUCTURE.md**: Matches documented structure exactly
5. ✅ **Tenant Isolation**: Each tenant has complete data separation

---

## Updated DIRECTORY_STRUCTURE.md Section

The scheduler section should be updated from shared jobs to tenant-specific:

```markdown
├── scheduler/                               # JOB SCHEDULER
│   ├── crontab.yaml                        # Job schedules
│   └── tenants/                            # Tenant-specific job implementations
│       ├── pidilite/
│       │   └── daily/
│       │       ├── evening/               # Evening jobs (dimension sync)
│       │       │   └── 01_dimensions_incremental.py
│       │       └── morning/               # Morning jobs (fact loads)
│       │           ├── 02_fact_invoice_secondary.py
│       │           ├── 03_fact_invoice_details.py
│       │           └── 04_dd_logic.py
│       └── uthra-global/
│           └── daily/
│               ├── evening/
│               │   └── 01_dimensions_refresh.py
│               └── morning/
│                   ├── 01_dimensions_incremental.py
│                   ├── 02_fact_invoice_secondary.py
│                   ├── 03_fact_invoice_details.py
│                   └── 04_business_logic_dd.py
```

**Note**: Each tenant can have different jobs with different business logic.
Common patterns can be extracted to shared utilities in `utils/`.
