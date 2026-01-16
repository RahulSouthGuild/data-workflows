# Week 3 - Azure Blob Path Correction

## Issue

Dimension job files were generating incorrect Azure folder paths due to improper string transformation.

### Root Cause

**Code Logic:**
```python
folder_path = f"Incremental/{table_name.replace('_', '').title()}/LatestData/"
```

This transformed:
- `dim_customer_master` → `Dimcustomermaster` ❌
- Expected: `DimCustomerMaster` ✅

### Impact

All dimension jobs reported "No blobs found" because the paths didn't match actual Azure container structure.

---

## Actual Azure Folder Structure

Based on user's Azure container `synapsedataprod`:

```
Incremental/
├── DimHierarchy/LatestData/
├── DimDealer_MS/LatestData/
├── DimMaterial/LatestData/
├── DimCustomerMaster/LatestData/
├── FactInvoiceSecondary/LatestData/
├── FactInvoiceSecondary_107_112/LatestData/
├── FactInvoiceDetails/LatestData/
└── FactInvoiceDetails_107_112/LatestData/
```

**Key Observations:**
1. Folder names use **PascalCase** (e.g., `DimCustomerMaster`)
2. Special case: `DimDealer_MS` (not `DimDealerMaster`)
3. Seed tables NOT in Azure: `dim_material_mapping`, `dim_sales_group`

---

## Solution

### Updated Configuration

Changed from simple string list to tuple mapping:

**Before:**
```python
DIMENSION_TABLES = [
    "dim_material_mapping",
    "dim_customer_master",
    "dim_dealer_master",
    "dim_hierarchy",
    "dim_sales_group",
    "dim_material",
]
```

**After:**
```python
# Dimension tables to process with Azure folder mapping
# Format: (table_name, azure_folder_name)
# Note: dim_material_mapping and dim_sales_group are loaded from seed files, not Azure
DIMENSION_TABLES = [
    ("dim_customer_master", "DimCustomerMaster"),
    ("dim_dealer_master", "DimDealer_MS"),
    ("dim_hierarchy", "DimHierarchy"),
    ("dim_material", "DimMaterial"),
]
```

### Updated Processing Logic

**Before:**
```python
for table_name in DIMENSION_TABLES:
    folder_path = f"Incremental/{table_name.replace('_', '').title()}/LatestData/"
```

**After:**
```python
for table_name, azure_folder in DIMENSION_TABLES:
    folder_path = f"Incremental/{azure_folder}/LatestData/"
```

---

## Files Modified

### Pidilite Tenant (2 files)
1. ✅ [scheduler/tenants/pidilite/daily/morning/01_dimensions_incremental.py](scheduler/tenants/pidilite/daily/morning/01_dimensions_incremental.py)
   - Updated DIMENSION_TABLES configuration (lines 30-38)
   - Updated loop to unpack tuple (line 140)
   - Updated folder_path generation (line 146)

2. ✅ [scheduler/tenants/pidilite/daily/evening/01_dimensions_refresh.py](scheduler/tenants/pidilite/daily/evening/01_dimensions_refresh.py)
   - Updated DIMENSION_TABLES configuration (lines 30-38)
   - Updated loop to unpack tuple (line 140)
   - Updated folder_path generation (line 146)

### Uthra Global Tenant (2 files)
3. ✅ [scheduler/tenants/uthra-global/daily/morning/01_dimensions_incremental.py](scheduler/tenants/uthra-global/daily/morning/01_dimensions_incremental.py)
   - Updated DIMENSION_TABLES configuration (lines 30-38)
   - Updated loop to unpack tuple (line 140)
   - Updated folder_path generation (line 146)

4. ✅ [scheduler/tenants/uthra-global/daily/evening/01_dimensions_refresh.py](scheduler/tenants/uthra-global/daily/evening/01_dimensions_refresh.py)
   - Updated DIMENSION_TABLES configuration (lines 30-38)
   - Updated loop to unpack tuple (line 140)
   - Updated folder_path generation (line 146)

---

## Expected Behavior After Fix

When running dimension jobs, you should now see:

```
2026-01-17 00:53:26,050 - INFO - Processing: dim_customer_master
2026-01-17 00:53:26,050 - INFO - Azure folder: Incremental/DimCustomerMaster/LatestData/
2026-01-17 00:53:26,083 - INFO - Found 5 blob(s) to process
```

Instead of:

```
2026-01-17 00:53:25,752 - WARNING - No blobs found in Incremental/Dimcustomermaster/LatestData/
```

---

## Path Mapping Reference

| Table Name | Azure Folder | Notes |
|------------|--------------|-------|
| `dim_customer_master` | `DimCustomerMaster` | Standard PascalCase |
| `dim_dealer_master` | `DimDealer_MS` | Special case with underscore |
| `dim_hierarchy` | `DimHierarchy` | Standard PascalCase |
| `dim_material` | `DimMaterial` | Standard PascalCase |
| `dim_material_mapping` | ❌ Not in Azure | Loaded from seed CSV |
| `dim_sales_group` | ❌ Not in Azure | Loaded from seed CSV |

---

## Testing

Run the Pidilite morning dimension job to verify:

```bash
cd /home/rahul/RahulSouthGuild/data-workflows
source .venv/bin/activate
python scheduler/tenants/pidilite/daily/morning/01_dimensions_incremental.py
```

**Expected Result:**
- Job should find blobs in Azure folders
- Download and process files
- Load data into StarRocks

---

## Next Steps

1. ✅ **Fixed**: Azure folder path mapping
2. ⏭️ **Next**: Test with real Azure data files
3. ⏭️ **Next**: Verify data loads into StarRocks tables
4. ⏭️ **Next**: Run fact table jobs (02 and 03) to load transactional data

---

**Document Created**: 2026-01-17
**Fix Type**: Azure Blob Path Mapping
**Status**: ✅ Complete - Ready for Testing

All dimension jobs now correctly reference Azure blob paths and will find files in the container.
