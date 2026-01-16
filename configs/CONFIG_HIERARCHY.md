# Configuration Hierarchy - Source of Truth

**Last Updated:** 2025-01-16

This document explains the configuration hierarchy and sources of truth for the multi-tenant ETL pipeline.

---

## Configuration Files Overview

```
configs/
├── tenant_registry.yaml              # Master tenant list (UUID, enabled status)
├── shared/
│   ├── default_config.yaml           # Global defaults (all components)
│   └── common_business_rules.yaml    # Data quality rules
├── starrocks/
│   ├── connection_pool.yaml          # DATABASE: Pooling, timeouts, health checks
│   └── stream_load_defaults.yaml     # DATABASE: Bulk load parameters
└── tenants/{slug}/
    ├── config.yaml                   # Tenant-specific overrides
    └── .env                          # Tenant secrets

scheduler/
└── crontab.yaml                      # JOB SCHEDULES: Per-job configurations
```

---

## Configuration Hierarchy (Merge Order)

When loading configuration for a tenant, files are merged in this order (later overrides earlier):

```
1. configs/shared/default_config.yaml           ← Base defaults
2. configs/starrocks/connection_pool.yaml       ← StarRocks-specific (database.*)
3. configs/starrocks/stream_load_defaults.yaml  ← StarRocks-specific (stream_load.*)
4. scheduler/crontab.yaml                       ← Job-specific schedules
5. configs/tenants/{slug}/config.yaml           ← Tenant overrides
6. configs/tenants/{slug}/.env                  ← Tenant secrets
```

**Priority:** Tenant config > Component-specific > Shared defaults

---

## Sources of Truth by Component

### 🗄️ Database Configuration

| Setting | Source of Truth | Notes |
|---------|-----------------|-------|
| **Connection pooling** | `starrocks/connection_pool.yaml` | Pool size, overflow, recycle, pre_ping |
| **Timeouts** | `starrocks/connection_pool.yaml` | Connect, query, stream_load timeouts |
| **Retry logic** | `starrocks/connection_pool.yaml` | Max attempts, backoff factor |
| **Health checks** | `starrocks/connection_pool.yaml` | Queries, intervals, thresholds |
| **Monitoring** | `starrocks/connection_pool.yaml` | Pool metrics, slow query logging |
| **Basic connection** | `shared/default_config.yaml` | Host, port, http_port only |

**Why split?**
- `default_config.yaml` = Simple connection details (host, port)
- `connection_pool.yaml` = Production-grade pooling and monitoring

---

### 📤 Stream Load Configuration

| Setting | Source of Truth | Notes |
|---------|-----------------|-------|
| **Format parameters** | `starrocks/stream_load_defaults.yaml` | Parquet, CSV, JSON settings |
| **Error handling** | `starrocks/stream_load_defaults.yaml` | max_filter_ratio, strict_mode |
| **Batch sizing** | `starrocks/stream_load_defaults.yaml` | Batch size, max bytes |
| **Retry logic** | `starrocks/stream_load_defaults.yaml` | HTTP retry, backoff |
| **Table strategies** | `starrocks/stream_load_defaults.yaml` | Dimension vs fact table defaults |

**Why separate?**
- StarRocks Stream Load has 50+ parameters
- Different strategies for dimension vs fact tables
- Format-specific optimizations (Parquet vs CSV vs JSON)

---

### ⏰ Scheduler Configuration

| Setting | Source of Truth | Notes |
|---------|-----------------|-------|
| **Job schedules** | `scheduler/crontab.yaml` | Cron expressions per job |
| **Per-job timeout** | `scheduler/crontab.yaml` | Job-specific timeout overrides |
| **Per-job retry** | `scheduler/crontab.yaml` | Job-specific retry counts |
| **Job modules** | `scheduler/crontab.yaml` | Job class and module paths |
| **Global defaults** | `shared/default_config.yaml` | Default timeout, retry, concurrency |

**Why split?**
- `crontab.yaml` = Detailed job definitions (schedule, class, timeout)
- `default_config.yaml` = Global execution defaults (timezone, max_concurrent_jobs)

---

### ☁️ Cloud Storage Configuration

| Setting | Source of Truth | Notes |
|---------|-----------------|-------|
| **Provider selection** | `tenants/{slug}/config.yaml` | azure, aws, gcp, minio, local |
| **Provider credentials** | `tenants/{slug}/.env` | Connection strings, keys, tokens |
| **Provider defaults** | `shared/default_config.yaml` | API versions, chunk sizes, retry |

**Why this way?**
- Each tenant chooses their own cloud provider
- Provider-specific defaults are in shared config
- Secrets always in .env files

---

### 💼 Business Constants Configuration

| Setting | Source of Truth | Notes |
|---------|-----------------|-------|
| **Backend selection** | `tenants/{slug}/config.yaml` | postgres, mysql, mongodb, starrocks |
| **Backend credentials** | `tenants/{slug}/.env` | Connection URIs with UUID prefix |
| **Backend defaults** | `shared/default_config.yaml` | Pool sizes, timeouts, table names |

**Why this way?**
- Each tenant chooses their own backend
- Backend-specific connection pooling in shared defaults
- Secrets in .env with unique UUID prefix

---

### 📊 Data Quality Rules

| Setting | Source of Truth | Notes |
|---------|-----------------|-------|
| **Global rules** | `shared/common_business_rules.yaml` | Null handling, data types, ranges |
| **Tenant overrides** | `tenants/{slug}/business_logic/` | Tenant-specific validation rules |

**Why split?**
- Common rules apply to all tenants (e.g., email format validation)
- Tenant-specific rules for custom business logic

---

## Configuration Loading Example

### Example: Load tenant1 (pidilite-mumbai) configuration

```python
# 1. Load shared defaults
default_config = load_yaml("configs/shared/default_config.yaml")
# Result: database.host = "localhost", database.port = 9030

# 2. Load StarRocks connection pool
starrocks_pool = load_yaml("configs/starrocks/connection_pool.yaml")
# Result: pool.size.max = 10, pool.timeout.query = 300

# 3. Load StarRocks stream load
starrocks_stream = load_yaml("configs/starrocks/stream_load_defaults.yaml")
# Result: format = "parquet", max_filter_ratio = 0.0

# 4. Load scheduler crontab
crontab = load_yaml("scheduler/crontab.yaml")
# Result: evening_dimension_sync.schedule = "0 18 * * *"

# 5. Load tenant config
tenant_config = load_yaml("configs/tenants/tenant1/config.yaml")
# Result: storage_provider = "azure", business_constants.backend = "postgres"

# 6. Load tenant secrets
tenant_env = load_env("configs/tenants/tenant1/.env")
# Result: DB_PASSWORD, AZURE_STORAGE_CONNECTION_STRING, BC_3607d64c_PG_URI

# 7. Merge all configs (tenant overrides component-specific overrides defaults)
final_config = deep_merge(
    default_config,
    {"database": {"pool": starrocks_pool["pool"]}},
    {"database": {"stream_load": starrocks_stream}},
    {"scheduler": {"jobs": crontab["schedules"]}},
    tenant_config,
    {"secrets": tenant_env}
)
```

---

## Tenant Override Examples

### Example 1: Override Database Pool Size

```yaml
# configs/tenants/high-volume-tenant/config.yaml
database:
  # Override from starrocks/connection_pool.yaml (pool.size.max = 10)
  pool_size: 20
  max_overflow: 40
```

**Result:** This tenant gets pool_size=20, all other tenants get pool_size=10

---

### Example 2: Override Stream Load Timeout

```yaml
# configs/tenants/large-batches-tenant/config.yaml
database:
  stream_load:
    # Override from starrocks/stream_load_defaults.yaml (timeout = 600)
    timeout: 1800  # 30 minutes for very large batches
```

**Result:** This tenant gets 30-min timeout, others get 10-min

---

### Example 3: Override Job Schedule

```yaml
# configs/tenants/late-running-tenant/config.yaml
scheduler:
  schedules:
    # Override from scheduler/crontab.yaml (evening_jobs = "0 18 * * *")
    evening_jobs: "0 20 * * *"  # 8:00 PM instead of 6:00 PM
```

**Result:** This tenant's evening jobs run at 8 PM

---

## Anti-Patterns to Avoid

### ❌ Don't Duplicate Settings

**Bad:**
```yaml
# configs/shared/default_config.yaml
database:
  pool_size: 10
  max_overflow: 20

# configs/starrocks/connection_pool.yaml
pool:
  size:
    max: 10
    max_overflow: 20
```

**Problem:** Which is the source of truth? Confusing maintenance.

**Good:**
```yaml
# configs/shared/default_config.yaml
database:
  # See configs/starrocks/connection_pool.yaml for pooling settings

# configs/starrocks/connection_pool.yaml (SOURCE OF TRUTH)
pool:
  size:
    max: 10
    max_overflow: 20
```

---

### ❌ Don't Put Secrets in YAML Files

**Bad:**
```yaml
# configs/tenants/tenant1/config.yaml
database:
  password: "my_password_here"  # ❌ NEVER DO THIS
```

**Good:**
```bash
# configs/tenants/tenant1/.env
DB_PASSWORD=my_password_here
```

---

### ❌ Don't Repeat Job Schedules

**Bad:**
```yaml
# configs/shared/default_config.yaml
scheduler:
  schedules:
    evening_dimension_sync: "0 18 * * *"

# scheduler/crontab.yaml
schedules:
  evening_dimension_sync:
    schedule: "0 18 * * *"
```

**Good:** Only define schedules in `scheduler/crontab.yaml`

---

## Configuration Validation

Use the validation script to check configuration consistency:

```bash
# Validate single tenant
python scripts/validate_tenant_config.py tenant1

# Validate all enabled tenants
python scripts/validate_tenant_config.py --all
```

**Checks performed:**
- ✓ No missing required fields
- ✓ Secrets in .env, not YAML
- ✓ Valid provider/backend choices
- ✓ YAML syntax correct
- ✓ File structure complete

---

## Summary: Which File for What?

| Need to configure... | Edit this file |
|---------------------|----------------|
| **New tenant** | `tenant_registry.yaml` + `tenants/{slug}/config.yaml` |
| **Database pooling** | `starrocks/connection_pool.yaml` |
| **Stream Load defaults** | `starrocks/stream_load_defaults.yaml` |
| **Job schedules** | `scheduler/crontab.yaml` |
| **Global defaults** | `shared/default_config.yaml` |
| **Data quality rules** | `shared/common_business_rules.yaml` |
| **Tenant secrets** | `tenants/{slug}/.env` |
| **Tenant overrides** | `tenants/{slug}/config.yaml` |

---

## Best Practices

1. ✅ **Component-specific settings go in component files**
   - Database → `starrocks/`
   - Scheduler → `scheduler/crontab.yaml`
   - Cloud storage → `shared/default_config.yaml` (provider defaults)

2. ✅ **Tenant-specific settings go in tenant config**
   - Provider choice (Azure vs AWS)
   - Backend choice (PostgreSQL vs MongoDB)
   - Business rules
   - Overrides for specific jobs/settings

3. ✅ **Secrets always go in .env**
   - Database passwords
   - Cloud storage credentials
   - API keys

4. ✅ **Document cross-references**
   - If settings are in another file, add a comment
   - Example: "See starrocks/connection_pool.yaml for pooling settings"

5. ✅ **Use validation script before deployment**
   - Catches configuration errors early
   - Ensures all required fields present

---

**Version:** 1.0
**Last Updated:** 2025-01-16
**Maintained by:** DataWiz Multi-Tenant Team
