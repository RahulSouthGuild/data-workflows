# Tenant Configuration Template

This directory contains the template for creating new tenant configurations in the Pidilite DataWiz multi-tenant ETL pipeline.

## Quick Start: Onboarding a New Tenant

### Step 1: Generate Tenant UUID

```bash
python -c "import uuid; print(uuid.uuid4())"
```

Example output: `a1b2c3d4-e5f6-7890-abcd-ef1234567890`

### Step 2: Copy Template Directory

```bash
# Replace {tenant-slug} with your tenant identifier (e.g., "pidilite-bangalore")
cp -r configs/tenants/_template configs/tenants/{tenant-slug}
cd configs/tenants/{tenant-slug}
```

### Step 3: Configure config.yaml

Edit `config.yaml` and replace placeholders:

```yaml
tenant_id: "a1b2c3d4-e5f6-7890-abcd-ef1234567890"  # From Step 1
tenant_slug: "pidilite-bangalore"
tenant_name: "Pidilite Bangalore Operations"
database:
  database_name: "datawiz_pidilite_bangalore"
```

**Choose Cloud Storage Provider:**

Uncomment ONE of these sections in `config.yaml`:

- **Azure Blob Storage**: For Azure-based tenants
- **AWS S3**: For AWS-based tenants
- **Google Cloud Storage**: For GCP-based tenants
- **MinIO**: For self-hosted S3-compatible storage
- **Local Filesystem**: For local development/testing

**Choose Business Constants Backend:**

Uncomment ONE of these sections:

- **PostgreSQL**: Recommended for production (most robust)
- **MySQL**: Alternative relational database
- **MongoDB**: For document-based storage
- **StarRocks**: Use main database (simplest setup)

### Step 4: Configure .env Secrets

```bash
# Copy environment template
cp .env.template .env

# Edit .env and add credentials
nano .env  # or vim, code, etc.
```

**Required variables (based on your choices):**

```bash
# Database password (always required)
DB_PASSWORD=your_secure_password

# Cloud storage (choose one)
AZURE_STORAGE_CONNECTION_STRING=...        # If using Azure
AWS_ACCESS_KEY_ID=...                      # If using AWS with access key
GCP_SERVICE_ACCOUNT_JSON=...              # If using GCP

# Business constants (choose one, except StarRocks which uses DB_PASSWORD)
BC_a1b2c3d4_PG_URI=postgresql://...       # If using PostgreSQL
BC_a1b2c3d4_MYSQL_URI=mysql://...         # If using MySQL
BC_a1b2c3d4_MONGO_URI=mongodb://...       # If using MongoDB
```

**Security:**
```bash
# Set restrictive permissions
chmod 600 .env

# Verify .env is in .gitignore
git check-ignore .env  # Should output: .env
```

### Step 5: Copy Schema Definitions

```bash
# Copy from existing tenant or legacy db/schemas/
cp -r ../pidilite-mumbai/schemas/* schemas/

# OR from legacy location
cp /path/to/db/schemas/tables/* schemas/tables/
cp /path/to/db/schemas/views/* schemas/views/
cp /path/to/db/schemas/matviews/* schemas/matviews/
```

### Step 6: Copy Column Mappings

```bash
# Copy and customize column mappings
cp -r ../pidilite-mumbai/column_mappings/* column_mappings/

# OR create new mappings based on your CSV structure
# See column_mappings/README.md for format
```

### Step 7: Copy Seed Data

```bash
# Copy seed data (reference tables)
cp -r ../pidilite-mumbai/seeds/* seeds/

# Update seed data for tenant-specific values
# Edit seeds/SEED_MAPPING.py if needed
```

### Step 8: Configure Business Logic

```bash
# Copy RLS configuration if using row-level security
cp -r ../pidilite-mumbai/business_logic/* business_logic/

# Customize for tenant's organizational hierarchy
```

### Step 9: Update Tenant Registry

Edit `configs/tenant_registry.yaml` and add entry:

```yaml
tenants:
  - tenant_id: "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    tenant_slug: "pidilite-bangalore"
    tenant_name: "Pidilite Bangalore Operations"
    enabled: false  # Keep disabled until testing complete
    database_name: "datawiz_pidilite_bangalore"
    database_user: "pidilite_bangalore_admin"
    storage_provider: "azure"  # or aws, gcp, minio, local
    storage_config:
      container_name: "pidilite-bangalore-prod"
      folder_prefix: "synapse_data/"
      auth_method: "connection_string"
    business_constants:
      enabled: true
      backend: "postgres"  # or mysql, mongodb, starrocks
    schedule_priority: 3
```

### Step 10: Create Database

```bash
# Connect to StarRocks
mysql -h localhost -P 9030 -u root -p

# Create database and user
CREATE DATABASE datawiz_pidilite_bangalore;
CREATE USER 'pidilite_bangalore_admin'@'%' IDENTIFIED BY 'secure_password';
GRANT ALL PRIVILEGES ON datawiz_pidilite_bangalore.* TO 'pidilite_bangalore_admin'@'%';
FLUSH PRIVILEGES;
```

### Step 11: Initialize Schema

```bash
# Run table creation script
python db/create_tables.py --tenant a1b2c3d4-e5f6-7890-abcd-ef1234567890

# Or using tenant slug
python db/create_tables.py --tenant-slug pidilite-bangalore
```

### Step 12: Load Seed Data

```bash
# Load reference data
python db/load_seed_data.py --tenant a1b2c3d4-e5f6-7890-abcd-ef1234567890
```

### Step 13: Validate Configuration

```bash
# Run validation script
python scripts/validate_tenant_config.py pidilite-bangalore

# Expected output:
# ✓ Config file exists
# ✓ All required fields present
# ✓ Database connection successful
# ✓ Cloud storage connection successful
# ✓ Business constants backend connection successful
# ✓ Schema files valid
# ✓ Column mappings valid
```

### Step 14: Test Data Load (Dry Run)

```bash
# Run ETL in dry-run mode (don't commit to database)
python main.py --tenant pidilite-bangalore --dry-run --table fact_invoice_secondary

# Check logs
tail -f logs/pidilite-bangalore/etl/pipeline.log
```

### Step 15: Enable Tenant

Once validation passes:

```yaml
# In configs/tenant_registry.yaml
tenants:
  - tenant_id: "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    enabled: true  # ← Change to true
```

### Step 16: Run First Full Load

```bash
# Run evening jobs (dimension sync)
python scheduler/run_job.py --tenant pidilite-bangalore --job evening_dimension_sync

# Run morning jobs (fact load)
python scheduler/run_job.py --tenant pidilite-bangalore --job morning_fis_incremental
```

---

## Directory Structure

```
configs/tenants/{tenant-slug}/
├── config.yaml                 # Tenant configuration (non-sensitive)
├── .env                        # Secrets and credentials (NEVER commit!)
├── computed_columns.yaml       # Derived column definitions
├── schemas/                    # Table/view/matview schemas
│   ├── tables/
│   │   ├── 01_DimMaterialMapping.py
│   │   ├── 02_DimSalesGroup.py
│   │   ├── 07_FactInvoiceDetails.py
│   │   └── ...
│   ├── views/
│   │   ├── 01_SecondarySalesView.py
│   │   └── ...
│   └── matviews/
│       └── ...
├── column_mappings/            # CSV → Database column mappings
│   ├── 01_DimMaterialMapping.yaml
│   ├── 07_FactInvoiceDetails.yaml
│   └── ...
├── business_logic/             # Business rules and RLS config
│   ├── rls_config.py
│   ├── data_quality_rules.yaml
│   └── ...
├── seeds/                      # Reference/seed data
│   ├── SEED_MAPPING.py
│   ├── DimMaterialMapping.csv
│   └── DimSalesGroup.csv
└── README.md                   # This file
```

---

## Configuration Files Explained

### config.yaml
- **Purpose**: Non-sensitive tenant configuration
- **Contents**: Database name, cloud provider settings, business rules, feature flags
- **Git**: ✓ COMMIT to git (no secrets here)

### .env
- **Purpose**: Secrets and credentials
- **Contents**: Passwords, API keys, connection strings, tokens
- **Git**: ✗ NEVER commit (in .gitignore)

### computed_columns.yaml
- **Purpose**: Define derived/calculated columns
- **Contents**: Concatenation rules, formulas, transformations
- **Git**: ✓ COMMIT to git

### schemas/
- **Purpose**: Table, view, and materialized view definitions
- **Format**: Python dictionaries with schema structure
- **Git**: ✓ COMMIT to git

### column_mappings/
- **Purpose**: Map CSV columns to database columns
- **Format**: YAML files (one per table)
- **Git**: ✓ COMMIT to git

### business_logic/
- **Purpose**: Tenant-specific business rules and RLS policies
- **Git**: ✓ COMMIT to git

### seeds/
- **Purpose**: Reference data (materials, sales groups, etc.)
- **Git**: ✓ COMMIT to git (if not too large; otherwise use .gitignore for CSVs)

---

## Cloud Provider Configuration Examples

### Azure Blob Storage

```yaml
# config.yaml
storage_provider: "azure"
storage_config:
  container_name: "pidilite-prod"
  folder_prefix: "synapse_data/"
  auth_method: "connection_string"
```

```bash
# .env
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=pidiliteprod;AccountKey=abc123...;EndpointSuffix=core.windows.net
```

### AWS S3

```yaml
# config.yaml
storage_provider: "aws"
storage_config:
  bucket_name: "pidilite-bangalore-data"
  region: "ap-south-1"
  folder_prefix: "raw_data/"
  auth_method: "iam_role"  # Or "access_key"
```

```bash
# .env (if using access_key)
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

### Google Cloud Storage

```yaml
# config.yaml
storage_provider: "gcp"
storage_config:
  bucket_name: "pidilite-south-data"
  project_id: "pidilite-analytics"
  folder_prefix: "exports/"
  auth_method: "service_account"
```

```bash
# .env
GCP_SERVICE_ACCOUNT_JSON={"type":"service_account","project_id":"pidilite-analytics",...}
```

---

## Business Constants Backend Examples

### PostgreSQL (Recommended)

```yaml
# config.yaml
business_constants:
  enabled: true
  backend: "postgres"
  database: "metadata_db"
  table: "business_constants"
```

```bash
# .env (replace a1b2c3d4 with first 8 chars of your tenant UUID)
BC_a1b2c3d4_PG_URI=postgresql://bc_user:bc_password@localhost:5432/metadata_db
```

### MongoDB

```yaml
# config.yaml
business_constants:
  enabled: true
  backend: "mongodb"
  database: "metadata_db"
  collection: "business_constants"
```

```bash
# .env
BC_a1b2c3d4_MONGO_URI=mongodb://bc_user:bc_password@localhost:27017/metadata_db
```

---

## Troubleshooting

### Issue: "Tenant not found in registry"
**Solution**: Add tenant entry to `configs/tenant_registry.yaml`

### Issue: "Database connection failed"
**Solution**:
1. Check `DB_PASSWORD` in `.env`
2. Verify database exists: `SHOW DATABASES LIKE 'datawiz_%';`
3. Test connection: `mysql -h localhost -P 9030 -u {user} -p`

### Issue: "Cloud storage authentication failed"
**Solution**:
1. Verify credentials in `.env`
2. Check auth_method in `config.yaml` matches credential type
3. Test Azure: `az storage container list --connection-string "..."`
4. Test AWS: `aws s3 ls s3://{bucket-name} --region {region}`

### Issue: "Schema validation failed"
**Solution**:
1. Check schema files in `schemas/tables/`
2. Run validation: `python scripts/validate_schema.py {tenant-slug}`
3. Compare with reference tenant schema

### Issue: "Column mapping mismatch"
**Solution**:
1. Download sample CSV from cloud storage
2. Inspect column headers
3. Update `column_mappings/{table}.yaml` to match CSV structure

---

## Best Practices

### Security
- ✓ Always use strong passwords (16+ chars, mixed case, symbols)
- ✓ Set `.env` permissions: `chmod 600 .env`
- ✓ Rotate credentials every 90 days
- ✓ Use IAM roles/service principals instead of static keys when possible
- ✗ Never commit `.env` to git
- ✗ Never hardcode secrets in `config.yaml`

### Configuration
- ✓ Inherit from shared defaults (`configs/shared/default_config.yaml`)
- ✓ Override only what's different for this tenant
- ✓ Document tenant-specific business rules in `notes` field
- ✓ Use meaningful tenant slugs (company-location format)

### Data Management
- ✓ Test with small dataset first (dry-run)
- ✓ Validate row counts after load
- ✓ Monitor logs during first production load
- ✓ Set up alerts for load failures

### Performance
- ✓ Use Parquet format for best performance
- ✓ Tune batch sizes based on data volume
- ✓ Enable parallel loads for independent tables
- ✓ Use incremental loads when possible

---

## Support

For questions or issues:

1. Check existing tenant configurations for examples
2. Review main documentation: `/docs/multi-tenant-architecture.md`
3. Run validation script: `python scripts/validate_tenant_config.py {slug}`
4. Contact: datawiz-support@pidilite.com

---

## Checklist

Use this checklist when onboarding a new tenant:

- [ ] Generated UUID for `tenant_id`
- [ ] Copied template directory to `configs/tenants/{slug}`
- [ ] Configured `config.yaml` with tenant details
- [ ] Chose cloud storage provider (Azure/AWS/GCP/MinIO/Local)
- [ ] Chose business constants backend (PostgreSQL/MySQL/MongoDB/StarRocks)
- [ ] Created `.env` with all required credentials
- [ ] Set `.env` permissions: `chmod 600 .env`
- [ ] Copied schema files to `schemas/`
- [ ] Copied column mappings to `column_mappings/`
- [ ] Copied seed data to `seeds/`
- [ ] Configured RLS policies in `business_logic/`
- [ ] Updated `configs/tenant_registry.yaml`
- [ ] Created StarRocks database
- [ ] Created database user with permissions
- [ ] Ran `python db/create_tables.py --tenant {uuid}`
- [ ] Ran `python db/load_seed_data.py --tenant {uuid}`
- [ ] Ran `python scripts/validate_tenant_config.py {slug}`
- [ ] Tested cloud storage connection
- [ ] Tested database connection
- [ ] Tested business constants backend connection
- [ ] Ran dry-run ETL job
- [ ] Reviewed logs for errors
- [ ] Set `enabled: true` in tenant registry
- [ ] Ran first full production load
- [ ] Verified data loaded successfully
- [ ] Set up monitoring/alerts
- [ ] Documented any tenant-specific quirks

---

**Template Version**: 1.0
**Last Updated**: 2025-01-16
**Compatible with**: DataWiz Multi-Tenant v2.0+
