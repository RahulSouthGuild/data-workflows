# Scheduler Path Updates - Complete ✅

## Summary

Updated all Pidilite scheduler jobs to use the correct data folder paths from `TenantConfig` properties, ensuring consistency with DIRECTORY_STRUCTURE.md.

---

## Changes Made

### 1. TenantConfig Enhanced ✅

**File**: `orchestration/tenant_manager.py`

**Added new properties** for subdirectory access:

```python
# Incremental data subdirectories
@property
def data_incremental_source_path(self) -> Path:
    """Returns: data/{tenant_slug}/incremental/source_files/"""

@property
def data_incremental_raw_path(self) -> Path:
    """Returns: data/{tenant_slug}/incremental/raw_parquet/"""

@property
def data_incremental_cleaned_path(self) -> Path:
    """Returns: data/{tenant_slug}/incremental/cleaned_parquet/"""

# Historical data subdirectories
@property
def data_historical_source_path(self) -> Path:
    """Returns: data/{tenant_slug}/historical/source_files/"""

@property
def data_historical_raw_path(self) -> Path:
    """Returns: data/{tenant_slug}/historical/raw_parquet/"""

@property
def data_historical_cleaned_path(self) -> Path:
    """Returns: data/{tenant_slug}/historical/cleaned_parquet/"""
```

---

### 2. Scheduler Jobs Updated ✅

Updated all 3 data processing jobs in `scheduler/tenants/pidilite/`:

#### File 1: `daily/evening/01_dimensions_incremental.py`
**Before**:
```python
output_dir = tenant_config.data_incremental_path / "raw_parquets"  # ❌ Wrong: hardcoded plural
```

**After**:
```python
output_dir = tenant_config.data_incremental_raw_path  # ✅ Correct: uses property
```

**Result**: Downloads dimension files from Azure → `data/pidilite/incremental/raw_parquet/`

---

#### File 2: `daily/morning/02_fact_invoice_secondary.py`
**Before**:
```python
output_dir = tenant_config.data_incremental_path / "raw_parquets"  # ❌ Wrong: hardcoded plural
```

**After**:
```python
output_dir = tenant_config.data_incremental_raw_path  # ✅ Correct: uses property
```

**Result**: Downloads fact_invoice_secondary files from Azure → `data/pidilite/incremental/raw_parquet/`

---

#### File 3: `daily/morning/03_fact_invoice_details.py`
**Before**:
```python
output_dir = tenant_config.data_incremental_path / "raw_parquets"  # ❌ Wrong: hardcoded plural
```

**After**:
```python
output_dir = tenant_config.data_incremental_raw_path  # ✅ Correct: uses property
```

**Result**: Downloads fact_invoice_details files from Azure → `data/pidilite/incremental/raw_parquet/`

---

#### File 4: `daily/morning/04_dd_logic.py`
**Status**: ✅ No changes needed

**Reason**: DD logic script doesn't download data files. It queries StarRocks database directly and uses `/tmp` for temporary CSV generation.

---

### 3. Empty Directories Checked ✅

**Monthly & Weekly directories exist but are empty:**
```
scheduler/tenants/pidilite/monthly/   # Empty - no jobs yet
scheduler/tenants/pidilite/weekly/    # Empty - no jobs yet
```

**Action**: ✅ No updates needed (no Python files exist yet)

---

## Data Flow After Updates

### Complete ETL Pipeline with Correct Paths

```
┌─────────────────────────────────────────────────────────────┐
│ STAGE 1: Azure Blob → source_files/                        │
└─────────────────────────────────────────────────────────────┘
Azure: Incremental/FactInvoiceSecondary/LatestData/file.csv.gz
    ↓ (download & decompress via blob_processor_utils.py)
data/pidilite/incremental/source_files/file.csv

┌─────────────────────────────────────────────────────────────┐
│ STAGE 2: source_files/ → raw_parquet/ (BRONZE LAYER)       │
└─────────────────────────────────────────────────────────────┘
Input:  data/pidilite/incremental/source_files/file.csv
    ↓ (convert via file_to_parquet.py)
Output: data/pidilite/incremental/raw_parquet/file.parquet
Action: CSV/Excel → Parquet (NO transformations)

┌─────────────────────────────────────────────────────────────┐
│ STAGE 3: raw_parquet/ → cleaned_parquet/ (SILVER LAYER)    │
└─────────────────────────────────────────────────────────────┘
Input:  data/pidilite/incremental/raw_parquet/file.parquet
    ↓ (transform via transformation_engine.py)
    - Column mapping (CSV cols → DB cols)
    - Type conversion (STRING → INT/DOUBLE)
    - Computed columns generation
    - Schema validation
Output: data/pidilite/incremental/cleaned_parquet/FactInvoiceSecondary.parquet

┌─────────────────────────────────────────────────────────────┐
│ STAGE 4: cleaned_parquet/ → StarRocks (GOLD LAYER)         │
└─────────────────────────────────────────────────────────────┘
Input:  data/pidilite/incremental/cleaned_parquet/FactInvoiceSecondary.parquet
    ↓ (load via starrocks_stream_loader.py)
Output: pidilite_db.fact_invoice_secondary (StarRocks table)
```

---

## Folder Structure After Updates

```
data/
├── pidilite/
│   ├── incremental/
│   │   ├── source_files/          # Stage 1: Downloaded raw files (CSV/Excel/Parquet)
│   │   ├── raw_parquet/            # Stage 2: Bronze - Converted to parquet (no transforms)
│   │   └── cleaned_parquet/        # Stage 3: Silver - Transformed & validated
│   ├── historical/
│   │   ├── source_files/
│   │   ├── raw_parquet/
│   │   └── cleaned_parquet/
│   └── temp/                       # Temporary processing files
│
└── uthra-global/
    └── (same structure as pidilite)
```

---

## Benefits Achieved

1. ✅ **Consistency**: All jobs use standard `tenant_config` properties
2. ✅ **Correct folder names**: `raw_parquet` (singular) not `raw_parquets` (plural)
3. ✅ **DRY principle**: Path logic in one place (TenantConfig)
4. ✅ **Easy maintenance**: Change folder structure in TenantConfig, not in every job
5. ✅ **Clear data layers**: source_files (raw) → raw_parquet (bronze) → cleaned_parquet (silver) → StarRocks (gold)
6. ✅ **Tenant isolation**: Each tenant has completely separate data directories

---

## Testing Checklist

- [ ] Create data directories for Pidilite:
  ```bash
  mkdir -p data/pidilite/incremental/{source_files,raw_parquet,cleaned_parquet}
  mkdir -p data/pidilite/historical/{source_files,raw_parquet,cleaned_parquet}
  mkdir -p data/pidilite/temp
  ```

- [ ] Create data directories for Uthra Global:
  ```bash
  mkdir -p data/uthra-global/incremental/{source_files,raw_parquet,cleaned_parquet}
  mkdir -p data/uthra-global/historical/{source_files,raw_parquet,cleaned_parquet}
  mkdir -p data/uthra-global/temp
  ```

- [ ] Test dimension sync job:
  ```bash
  python scheduler/tenants/pidilite/daily/evening/01_dimensions_incremental.py
  ```

- [ ] Test fact load jobs:
  ```bash
  python scheduler/tenants/pidilite/daily/morning/02_fact_invoice_secondary.py
  python scheduler/tenants/pidilite/daily/morning/03_fact_invoice_details.py
  ```

- [ ] Verify data lands in correct directories:
  - `data/pidilite/incremental/raw_parquet/` contains `.parquet` files
  - `data/pidilite/incremental/cleaned_parquet/` contains transformed `.parquet` files

---

## Next Steps

1. **Update Uthra Global jobs** (optional - if they have the same issue)
2. **Update ETLOrchestrator** to use new properties if needed
3. **Update blob_processor_utils.py** to use new properties if needed
4. **Run end-to-end test** to verify complete data flow
5. **Update DIRECTORY_STRUCTURE.md** to reflect Option 2 (tenant-specific jobs)

---

## Files Modified

1. ✅ `orchestration/tenant_manager.py` - Added 6 new properties
2. ✅ `scheduler/tenants/pidilite/daily/evening/01_dimensions_incremental.py`
3. ✅ `scheduler/tenants/pidilite/daily/morning/02_fact_invoice_secondary.py`
4. ✅ `scheduler/tenants/pidilite/daily/morning/03_fact_invoice_details.py`
5. ⏭️ `scheduler/tenants/pidilite/monthly/` - Empty (no files yet)
6. ⏭️ `scheduler/tenants/pidilite/weekly/` - Empty (no files yet)

---

**Status**: ✅ **All Pidilite scheduler jobs updated successfully!**
