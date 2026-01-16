# Configs Directory - Quick Reference

**Last Updated:** 2025-01-16

This directory contains all configuration files for the multi-tenant ETL pipeline.

---

## 📁 Directory Structure

```
configs/
├── README.md                         ← You are here
├── CONFIG_HIERARCHY.md               ← Detailed hierarchy and merge order
├── tenant_registry.yaml              ← Master tenant list
├── shared/
│   ├── default_config.yaml           ← Global defaults
│   └── common_business_rules.yaml    ← Data quality rules
├── starrocks/
│   ├── connection_pool.yaml          ← Database pooling (SOURCE OF TRUTH)
│   └── stream_load_defaults.yaml     ← Bulk load params (SOURCE OF TRUTH)
└── tenants/
    ├── _template/                    ← Template for new tenants
    └── tenant1/                      ← Example: pidilite-mumbai
```

---

## 🎯 Quick Start

### I want to...

| Task | Edit this file |
|------|----------------|
| Add a new tenant | `tenant_registry.yaml` + copy `_template/` |
| Change database pool size | `starrocks/connection_pool.yaml` |
| Change Stream Load timeout | `starrocks/stream_load_defaults.yaml` |
| Change global defaults | `shared/default_config.yaml` |
| Override for one tenant | `tenants/{slug}/config.yaml` |
| Add tenant secrets | `tenants/{slug}/.env` |
| Change data quality rules | `shared/common_business_rules.yaml` |

---

## 📋 File Descriptions

### `tenant_registry.yaml`
**Purpose:** Master list of all tenants (enabled and disabled)

**Contains:**
- Tenant UUIDs and slugs
- Enabled/disabled status
- Cloud provider choices (Azure, AWS, GCP, MinIO, Local)
- Business constants backend choices (PostgreSQL, MySQL, MongoDB, StarRocks)
- Schedule priorities

**Edit when:** Adding/removing tenants, enabling/disabling tenants

---

### `shared/default_config.yaml`
**Purpose:** Global configuration defaults for ALL tenants

**Contains:**
- Basic database connection details (host, port)
- Cloud storage provider defaults
- Business constants backend defaults
- Data paths patterns
- Logging configuration
- ETL pipeline settings
- Notification settings
- Feature flags

**Edit when:** Changing settings that apply to all tenants

**NOTE:** Does NOT contain database pooling or Stream Load settings (see `starrocks/`)

---

### `shared/common_business_rules.yaml`
**Purpose:** Data quality and validation rules for all tenants

**Contains:**
- Null handling rules
- Data type validation
- Range validation
- Pattern validation (regex)
- Duplicate detection
- Schema validation rules
- Transformation rules
- Error handling rules

**Edit when:** Adding/changing data quality rules

---

### `starrocks/connection_pool.yaml`
**Purpose:** StarRocks database connection pooling (SOURCE OF TRUTH)

**Contains:**
- Pool sizing (min, max, overflow)
- Connection timeouts (connect, query, stream_load)
- Connection lifecycle (recycle, pre_ping)
- Retry logic
- Health checks
- Load balancing
- Event handlers
- Performance monitoring
- Disaster recovery

**Edit when:** Tuning database connection pooling

**⚠️ Important:** This is the ONLY source of truth for pooling settings

---

### `starrocks/stream_load_defaults.yaml`
**Purpose:** StarRocks Stream Load parameters (SOURCE OF TRUTH)

**Contains:**
- Format-specific settings (Parquet, CSV, JSON)
- Error handling (max_filter_ratio, strict_mode)
- Batch sizing
- Retry configuration
- Table-specific strategies (dimension vs fact)
- Load modes (incremental, full, upsert)
- Validation rules

**Edit when:** Tuning bulk data loading

**⚠️ Important:** This is the ONLY source of truth for Stream Load settings

---

### `tenants/_template/`
**Purpose:** Template directory for creating new tenants

**Contains:**
- `config.yaml.template` - Template with all provider options
- `.env.template` - Template with all credential placeholders
- `computed_columns.yaml` - Example computed columns
- `README.md` - 16-step onboarding guide
- Empty directories: schemas/, column_mappings/, business_logic/, seeds/

**Use when:** Onboarding a new tenant

**How to use:**
```bash
# Copy template
cp -r tenants/_template tenants/new-tenant-slug

# Edit config files
cd tenants/new-tenant-slug
# Edit config.yaml (replace placeholders)
# Edit .env (add secrets)
# Copy schemas, mappings, seeds
```

---

### `tenants/{slug}/config.yaml`
**Purpose:** Tenant-specific configuration overrides

**Contains:**
- Tenant ID (UUID) and name
- Database name and user
- Cloud storage provider and settings
- Business constants backend and settings
- Business rules overrides
- Feature flag overrides
- Scheduler overrides

**Edit when:** Configuring tenant-specific settings

**NOTE:** Overrides settings from `shared/default_config.yaml`

---

### `tenants/{slug}/.env`
**Purpose:** Tenant-specific secrets (NEVER commit to git!)

**Contains:**
- Database password: `DB_PASSWORD`
- Cloud storage credentials:
  - Azure: `AZURE_STORAGE_CONNECTION_STRING`
  - AWS: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
  - GCP: `GCP_SERVICE_ACCOUNT_JSON`
  - MinIO: `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`
- Business constants credentials:
  - PostgreSQL: `BC_{UUID_PREFIX}_PG_URI`
  - MySQL: `BC_{UUID_PREFIX}_MYSQL_URI`
  - MongoDB: `BC_{UUID_PREFIX}_MONGO_URI`
- Email credentials: `SMTP_USERNAME`, `SMTP_PASSWORD`

**Edit when:** Adding/updating tenant secrets

**⚠️ Security:**
- File permissions: `chmod 600 .env`
- In `.gitignore`: `configs/tenants/*/.env`
- NEVER commit to git

---

## 🔀 Configuration Merge Order

When loading a tenant's configuration, files are merged in this order:

```
1. shared/default_config.yaml           ← Base defaults
2. starrocks/connection_pool.yaml       ← Database pooling
3. starrocks/stream_load_defaults.yaml  ← Stream Load params
4. scheduler/crontab.yaml               ← Job schedules
5. tenants/{slug}/config.yaml           ← Tenant overrides
6. tenants/{slug}/.env                  ← Secrets

Priority: Later files override earlier files
```

**See [CONFIG_HIERARCHY.md](CONFIG_HIERARCHY.md) for detailed explanation**

---

## ✅ Validation

Before deploying changes, validate configuration:

```bash
# Validate single tenant
python scripts/validate_tenant_config.py tenant1

# Validate all enabled tenants
python scripts/validate_tenant_config.py --all
```

**Validation checks:**
- ✓ All required fields present
- ✓ Secrets in .env (not in YAML)
- ✓ Valid provider/backend choices
- ✓ YAML syntax correct
- ✓ File structure complete
- ✓ Database connection (if credentials provided)
- ✓ Cloud storage connection (if credentials provided)

---

## 🚀 Onboarding a New Tenant

**Step-by-step guide:** See `tenants/_template/README.md`

**Quick checklist:**
1. Generate UUID: `python -c "import uuid; print(uuid.uuid4())"`
2. Copy template: `cp -r tenants/_template tenants/new-slug`
3. Edit `config.yaml` (replace placeholders)
4. Edit `.env` (add secrets)
5. Copy schemas, mappings, seeds
6. Add entry to `tenant_registry.yaml`
7. Validate: `python scripts/validate_tenant_config.py new-slug`
8. Set `enabled: true` in registry

---

## 📖 Documentation

| Document | Purpose |
|----------|---------|
| [README.md](README.md) | This quick reference |
| [CONFIG_HIERARCHY.md](CONFIG_HIERARCHY.md) | Detailed merge order and sources of truth |
| [tenants/_template/README.md](tenants/_template/README.md) | Tenant onboarding guide |
| [WEEK1_COMPLETION_REPORT.md](../WEEK1_COMPLETION_REPORT.md) | Implementation details |

---

## ⚠️ Common Mistakes to Avoid

### ❌ Don't edit both default_config.yaml and starrocks/*.yaml
**Problem:** Creates confusion about source of truth

**Solution:** Edit database settings in `starrocks/connection_pool.yaml` only

---

### ❌ Don't put secrets in YAML files
**Problem:** Secrets committed to git

**Solution:** All secrets go in `.env` files (gitignored)

---

### ❌ Don't forget to update tenant_registry.yaml
**Problem:** Tenant won't be recognized by orchestrator

**Solution:** Always add tenant entry to registry

---

### ❌ Don't skip validation
**Problem:** Broken configuration deployed to production

**Solution:** Run validation script before deployment

---

## 🆘 Troubleshooting

### "Tenant not found"
→ Check `tenant_registry.yaml` has entry for tenant

### "Missing required field"
→ Run validation script to see which field is missing

### "Database connection failed"
→ Check `.env` has correct `DB_PASSWORD`

### "Which file should I edit?"
→ See "I want to..." table above or [CONFIG_HIERARCHY.md](CONFIG_HIERARCHY.md)

---

## 📞 Support

For questions:
1. Read [CONFIG_HIERARCHY.md](CONFIG_HIERARCHY.md) for detailed explanation
2. Check existing tenant configs for examples
3. Run validation script for specific errors
4. Contact: datawiz-support@pidilite.com

---

**Version:** 1.0
**Last Updated:** 2025-01-16
**Maintained by:** DataWiz Multi-Tenant Team
