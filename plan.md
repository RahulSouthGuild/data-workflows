WEEK 1: Config Foundation
✅ Create configs/ directory structure
✅ Write tenant_registry.yaml
✅ Create configs/tenants/tenant1/ and migrate current configs
✅ Create .env file with secrets
✅ Create _template/ directory
✅ Build orchestration/tenant_manager.py
✅ Test: Load tenant1 config successfully
WEEK 2: Core Components
✅ Update core/transformers/file_to_parquet.py (tenant-aware)
✅ Update core/transformers/transformation_engine.py
✅ Update config/database.py (tenant-aware pooling)
✅ Test: Transform test file using tenant1 config
WEEK 3: ETL Pipeline
✅ Update utils/etl_orchestrator.py (accept tenant_config)
✅ Update core/loaders/starrocks_loader.py (tenant-aware)
✅ Test: Run full ETL for tenant1
WEEK 4: Scheduler
✅ Build orchestration/tenant_job_runner.py
✅ Update one job file (e.g., dimension_sync.py)
✅ Update scheduler/orchestrator.py
✅ Test: Run scheduled job for tenant1
WEEK 5: Scale to All Jobs
✅ Update all remaining job files
✅ Create data directories for tenant1
✅ Update logging for per-tenant logs
✅ Test: Full day of jobs for tenant1
WEEK 6: Validation
✅ End-to-end testing
✅ Performance testing
✅ Security audit (secrets not in git)
WEEK 7: Second Tenant
✅ Onboard tenant2
✅ Run jobs for both tenants
✅ Validate isolation