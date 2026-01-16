# Database Management Refactoring Plan

## Overview
Refactor `/db` directory to be tenant-aware, removing duplicated schemas that now exist in tenant configs, and creating a terminal UI for tenant-specific database operations.

## Current Structure Issues
1. **Duplicate schemas**: Schemas exist in both `/db/schemas/` (old Python files) and `/configs/tenants/{tenant}/schemas/` (new YAML files)
2. **No tenant awareness**: `create_tables.py` doesn't know about multi-tenant architecture
3. **Old config**: Uses `utils.DB_CONFIG` instead of tenant configs
4. **Scattered column mappings**: Exist in both `/db/column_mappings/` and tenant configs

## Refactoring Goals

### 1. Clean Up `/db` Directory

**FILES TO REMOVE** (schemas now in tenant YAML):
- `/db/schemas/` - entire directory (schemas moved to tenant configs)
- `/db/column_mappings/` - now in `configs/tenants/{tenant}/column_mappings/`
- `/db/computed_columns.json` - now in `configs/tenants/{tenant}/computed_columns.json`
- `/db/schema.py` - no longer needed (imported old Python schemas)

**FILES TO KEEP**:
- `/db/create_tables.py` - REFACTOR to be tenant-aware
- `/db/seeds/` - seed data (may need tenant awareness later)
- `/db/migrations/` - migration scripts
- `/db/rls/` - RLS policies
- `/db/scripts/` - utility scripts
- `/db/populate_business_constants.py` - may need tenant awareness
- `/db/load_seed_data.py` - may need tenant awareness

### 2. Create Tenant-Aware Table Manager

**NEW FILE**: `/db/tenant_table_manager.py`

**Purpose**:
- Load schemas from tenant YAML files
- Execute CREATE/DROP statements using tenant database connections
- Parse YAML and handle comments

**Class Structure**:
```python
class TenantTableManager:
    """Manages database objects for a specific tenant"""

    def __init__(self, tenant_config: TenantConfig):
        self.tenant_config = tenant_config
        self.connection = None

    def connect(self):
        """Connect to tenant-specific database"""

    def load_table_schemas(self) -> List[Dict]:
        """Load all table schemas from tenant YAML files"""
        # Read from tenant_config.tables_path
        # Parse YAML files, sort by order

    def load_view_schemas(self) -> List[Dict]:
        """Load all view schemas from tenant YAML files"""
        # Read from tenant_config.views_path

    def load_matview_schemas(self) -> List[Dict]:
        """Load all materialized view schemas from tenant YAML files"""
        # Read from tenant_config.matviews_path

    def create_object(self, schema_dict: Dict) -> bool:
        """Create table/view/matview from YAML schema"""
        # Execute schema_dict['sql']
        # Handle comments if StarRocks supports them

    def drop_object(self, name: str, object_type: str = 'TABLE') -> bool:
        """Drop table/view/matview"""

    def get_all_schemas(self) -> Dict:
        """Get all schemas organized by type"""
        return {
            'tables': self.load_table_schemas(),
            'views': self.load_view_schemas(),
            'matviews': self.load_matview_schemas()
        }
```

### 3. Refactor `/db/create_tables.py`

**NEW TERMINAL UI FLOW**:

```
================================================================================
🏢 Multi-Tenant Database Management System
================================================================================

SELECT TENANT:
1. Pidilite (pidilite_db) - ✅ Enabled
2. Uthra Global (datawiz_uthra_global) - ✅ Enabled

Enter tenant number:
```

After tenant selection:

```
================================================================================
🏢 Database Management: Pidilite
📊 Database: pidilite_db
🔗 Host: 127.0.0.1:9030
================================================================================

CREATE Operations:
1. Create ALL Objects (Tables + Views + MatViews)

Tables:
2. Create All Tables
3. Create Specific Table(s)

Views:
4. Create All Views
5. Create Specific View(s)

Materialized Views:
6. Create All Materialized Views
7. Create Specific Materialized View(s)

DELETE Operations:
8. Drop All Objects
9. Drop Specific Object

Other:
10. Switch Tenant
0. Exit

Enter choice:
```

**Key Changes**:
- Replace `DB_CONFIG` with `TenantConfig`
- Replace `StarRocksTableManager` with `TenantTableManager`
- Add tenant selection menu
- Load schemas from tenant YAML files instead of Python imports
- Show current tenant context in all operations

### 4. Implementation Steps

#### Step 1: Create TenantTableManager
```bash
# Create new file
/db/tenant_table_manager.py

# Features:
- YAML schema loader (parse .yaml files from tenant schemas/)
- Connect using TenantConfig
- Execute CREATE/DROP statements
- Handle tables, views, materialized views
- Proper error handling and logging
```

#### Step 2: Refactor create_tables.py
```python
# Main changes:
1. Import TenantManager instead of DB_CONFIG
2. Add tenant_selection_menu() function
3. Replace StarRocksTableManager with TenantTableManager
4. Update all menu options to be tenant-aware
5. Add "Switch Tenant" option
6. Show current tenant in menu header
```

#### Step 3: Test with Pidilite tenant
```bash
# Test scenarios:
1. Select Pidilite tenant
2. Create all tables
3. Create specific view
4. Drop specific object
5. Switch to Uthra Global
6. Repeat operations
```

#### Step 4: Clean up old files
```bash
# Remove after testing:
rm -rf /db/schemas/
rm -rf /db/column_mappings/
rm /db/computed_columns.json
rm /db/schema.py

# Update any scripts that import from these
```

## File Structure After Refactoring

```
/db/
├── create_tables.py               # Tenant-aware terminal UI (REFACTORED)
├── tenant_table_manager.py        # NEW: Manages tenant schemas
├── load_seed_data.py               # Keep (may need tenant awareness)
├── populate_business_constants.py  # Keep (may need tenant awareness)
├── migrations/                     # Keep
├── rls/                            # Keep
├── scripts/                        # Keep
└── seeds/                          # Keep

/configs/tenants/{tenant}/schemas/  # Single source of truth
├── tables/
│   ├── 01_dim_material_mapping.yaml
│   ├── 02_dim_customer_master.yaml
│   └── ...
├── views/
│   ├── 01_secondary_sales_view.yaml
│   └── ...
└── matviews/
    └── (future)
```

## YAML Schema Format

Each schema file follows this structure:

```yaml
name: dim_material_mapping
type: TABLE  # or VIEW, MATVIEW
order: 1
sql: |
  CREATE TABLE dim_material_mapping (
    brand VARCHAR(118),
    material_code VARCHAR(118),
    ...
  )
  DISTRIBUTED BY HASH(material_code) BUCKETS 10
  PROPERTIES (
    "replication_num" = "1"
  );
comments:
  table: "Stores the mapping of material codes"
  columns:
    material_code: "Unique code for the material"
    brand: "Brand name"
```

## Benefits

1. ✅ **Single source of truth**: Schemas only in tenant YAML files
2. ✅ **Tenant isolation**: Each tenant has own database and schemas
3. ✅ **Easy tenant onboarding**: Just create new tenant config directory
4. ✅ **Clear separation**: Global vs tenant-specific configs
5. ✅ **User-friendly**: Terminal UI guides through tenant selection
6. ✅ **No duplication**: No more duplicate schema files
7. ✅ **Maintainable**: One place to update schemas
8. ✅ **Scalable**: Easy to add new tenants

## Testing Checklist

- [ ] TenantTableManager can load Pidilite table schemas
- [ ] TenantTableManager can load Pidilite view schemas
- [ ] Can create all Pidilite tables successfully
- [ ] Can create specific Pidilite view
- [ ] Can drop objects for Pidilite
- [ ] Tenant selection menu works correctly
- [ ] Can switch between tenants
- [ ] Uthra Global tenant operations work
- [ ] Error handling works (invalid tenant, connection failure)
- [ ] Logging captures all operations

## Migration Notes

**Before starting**:
1. Ensure all schemas are in tenant YAML files
2. Verify tenant configs have correct paths
3. Backup current `/db` directory

**During refactoring**:
1. Keep old files until testing complete
2. Test each tenant independently
3. Verify all operations work

**After completion**:
1. Remove old files
2. Update documentation
3. Train team on new workflow

## Future Enhancements

1. **Seed data per tenant**: Move `/db/seeds/` to tenant-specific location
2. **Tenant-specific RLS**: Manage RLS policies per tenant
3. **Schema versioning**: Track schema changes per tenant
4. **Migration support**: Tenant-aware migration runner
5. **Backup/Restore**: Per-tenant backup scripts
