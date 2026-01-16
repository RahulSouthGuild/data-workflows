# Multi-Tenant Scheduler

This directory contains tenant-specific job files for the multi-tenant ETL pipeline.

## Directory Structure

```
scheduler/tenants/
├── pidilite/
│   ├── daily/
│   │   ├── morning/
│   │   │   ├── 01_dimensions_incremental.py
│   │   │   ├── 02_fact_invoice_secondary.py
│   │   │   ├── 03_fact_invoice_details.py
│   │   │   └── 04_business_logic.py
│   │   └── evening/
│   │       └── 01_refresh_views.py
│   ├── weekly/
│   └── monthly/
│
├── uthra-global/
│   ├── daily/
│   │   ├── morning/
│   │   └── evening/
│   ├── weekly/
│   └── monthly/
│
└── README.md (this file)
```

## How It Works

Each tenant has its own folder with job files. The jobs are tenant-specific and use the tenant's configuration from `configs/tenants/{tenant_slug}/`.

### Job Configuration

Each job automatically loads its tenant configuration:

```python
from orchestration.tenant_manager import TenantManager

tenant_manager = TenantManager(Path("configs"))
tenant_config = tenant_manager.get_tenant_by_slug("pidilite")
```

The `tenant_config` object provides:
- `azure_connection_string` - Azure Blob Storage connection
- `azure_container` - Container name
- `database_name` - StarRocks database
- `database_host`, `database_port`, `database_user`, `database_password`
- `schema_path` - Path to tenant schemas
- `column_mappings_path` - Path to column mappings
- `computed_columns_path` - Path to computed columns config

## Running Jobs

### Run a Specific Job for a Tenant

```bash
cd /home/rahul/RahulSouthGuild/data-workflows

# Run Pidilite dimension load
python scheduler/tenants/pidilite/daily/morning/01_dimensions_incremental.py

# Run Pidilite FIS load
python scheduler/tenants/pidilite/daily/morning/02_fact_invoice_secondary.py

# Run Uthra Global jobs
python scheduler/tenants/uthra-global/daily/morning/01_dimensions_incremental.py
```

### Cron Schedule

Add to crontab:

```bash
# Pidilite - Daily morning jobs
0 6 * * * cd /path/to/project && python scheduler/tenants/pidilite/daily/morning/01_dimensions_incremental.py
5 6 * * * cd /path/to/project && python scheduler/tenants/pidilite/daily/morning/02_fact_invoice_secondary.py

# Uthra Global - Daily morning jobs
0 7 * * * cd /path/to/project && python scheduler/tenants/uthra-global/daily/morning/01_dimensions_incremental.py
5 7 * * * cd /path/to/project && python scheduler/tenants/uthra-global/daily/morning/02_fact_invoice_secondary.py
```

## Creating Jobs for New Tenants

1. Create tenant directory structure:
   ```bash
   mkdir -p scheduler/tenants/{new-tenant}/daily/morning
   mkdir -p scheduler/tenants/{new-tenant}/daily/evening
   ```

2. Copy job files from existing tenant:
   ```bash
   cp scheduler/tenants/pidilite/daily/morning/*.py scheduler/tenants/{new-tenant}/daily/morning/
   ```

3. Update `TENANT_SLUG` in each job file:
   ```python
   TENANT_SLUG = "new-tenant"
   ```

4. Adjust Azure folder paths if different:
   ```python
   FOLDERS = [
       "Incremental/FactInvoiceSecondary/LatestData/",  # Update if needed
   ]
   ```

5. Test the job:
   ```bash
   python scheduler/tenants/{new-tenant}/daily/morning/01_dimensions_incremental.py
   ```

## Job Dependencies

All jobs depend on:
- `orchestration/tenant_manager.py` - Tenant configuration loading
- `config/database.py` - Database connection management
- `core/transformers/transformation_engine.py` - Data transformation
- `core/loaders/starrocks_stream_loader.py` - StarRocks loading
- Tenant configuration in `configs/tenants/{tenant_slug}/`

## Monitoring

Check logs for each job execution:
- Logs are created with timestamp: `{tenant}_dimension_incremental_{timestamp}.log`
- Check for success/failure exit codes
- Monitor row counts and processing times

## Advantages of This Structure

1. **Clear Separation** - Each tenant's jobs are isolated
2. **Easy Customization** - Modify one tenant without affecting others
3. **Independent Schedules** - Different cron times per tenant
4. **Simple to Understand** - Folder name = tenant name
5. **No Conflicts** - Tenants never interfere with each other

## Next Steps

1. Create job files for remaining tables (fact_invoice_details, business logic, etc.)
2. Set up cron schedules for each tenant
3. Configure monitoring and alerting
4. Document tenant-specific customizations
