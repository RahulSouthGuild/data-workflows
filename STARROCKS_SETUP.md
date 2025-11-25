# StarRocks Production Setup Guide

## Overview
This document provides comprehensive instructions for deploying StarRocks in a production environment using Docker Compose.

## Prerequisites

### Hardware Requirements
- **Minimum Requirements:**
  - CPU: 8 cores
  - Memory: 16GB RAM
  - Storage: 200GB SSD

- **Production Recommended:**
  - FE Node: 4 CPU cores, 16GB RAM
  - BE Node: 8 CPU cores, 32GB RAM
  - Storage: 500GB+ SSD for data

### Software Requirements
- Docker Engine 20.10+
- Docker Compose 2.0+
- Linux kernel 3.10+ (production)

## Architecture

This deployment includes:

1. **StarRocks FE (Frontend)** - Query coordinator and metadata manager
   - Ports: 8030 (HTTP), 9020 (RPC), 9030 (MySQL)
   - Memory: 8GB heap, 16GB container limit
   - CPU: 4 cores

2. **StarRocks BE (Backend)** - Data storage and computation engine
   - Ports: 8040 (HTTP), 9060 (Thrift), 8060 (bRPC), 9050 (Heartbeat)
   - Memory: 85% of 32GB (~27GB), 32GB container limit
   - CPU: 8 cores
   - Storage: SSD-backed persistent volumes

## Pre-Deployment Steps

### 1. Create Required Directories

```bash
# Create StarRocks data directories
sudo mkdir -p /var/lib/starrocks/{fe-meta,fe-log,be-storage,be-log}

# Set appropriate permissions
sudo chown -R $USER:$USER /var/lib/starrocks
chmod -R 755 /var/lib/starrocks
```

### 2. Configure System Limits

Edit `/etc/security/limits.conf` and add:

```
* soft nofile 65535
* hard nofile 65535
* soft nproc 65535
* hard nproc 65535
```

Apply changes:
```bash
ulimit -n 65535
ulimit -u 65535
```

### 3. Verify Docker Resources

Ensure Docker has sufficient resources allocated:

```bash
docker info | grep -E "CPUs|Total Memory"
```

## Deployment

### 1. Start the Cluster

```bash
# Start all services
docker-compose up -d

# Monitor startup logs
docker-compose logs -f starrocks-fe starrocks-be
```

### 2. Verify FE Health

```bash
# Wait for FE to be healthy (may take 60-90 seconds)
curl http://localhost:8030/api/health

# Check FE logs
docker-compose logs starrocks-fe | tail -50
```

### 3. Register BE with FE

After FE is healthy, register the BE node:

```bash
# Connect to FE
docker exec -it pidilite-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root

# In MySQL prompt, add the BE
ALTER SYSTEM ADD BACKEND "starrocks-be:9050";

# Verify BE registration
SHOW BACKENDS\G

# Exit MySQL
exit
```

### 4. Verify BE Health

```bash
# Check BE health endpoint
curl http://localhost:8040/api/health

# Check BE logs
docker-compose logs starrocks-be | tail -50
```

## Configuration Tuning

### FE Configuration (`/opt/starrocks/fe/conf/fe.conf`)

Key parameters to adjust for production:

```properties
# Connection limits
qe_max_connection = 4096
thrift_server_max_worker_threads = 4096

# Metadata
meta_dir = /opt/starrocks/fe/meta
meta_delay_toleration_second = 300

# Query settings
max_query_retry_time = 3
query_timeout = 300
```

### BE Configuration (`/opt/starrocks/be/conf/be.conf`)

Key parameters to adjust:

```properties
# Memory
mem_limit = 85%

# Storage
storage_root_path = /opt/starrocks/be/storage,medium:ssd
storage_flood_stage_usage_percent = 95

# Performance
num_threads_per_core = 3
scanner_thread_pool_thread_num = 48
```

To apply custom configurations:

1. Create config files locally:
   ```bash
   mkdir -p ./config/starrocks/{fe,be}
   ```

2. Add your `fe.conf` and `be.conf` files

3. Mount them in docker-compose.yml (already configured via volumes)

## Monitoring

### Health Checks

```bash
# FE health
curl http://localhost:8030/api/health

# BE health
curl http://localhost:8040/api/health

# FE metrics
curl http://localhost:8030/metrics

# BE metrics
curl http://localhost:8040/metrics
```

### Container Status

```bash
# Check all containers
docker-compose ps

# View resource usage
docker stats pidilite-starrocks-fe pidilite-starrocks-be

# Check logs
docker-compose logs --tail=100 -f starrocks-fe
docker-compose logs --tail=100 -f starrocks-be
```

### Database Status

```bash
# Connect to StarRocks
mysql -h 127.0.0.1 -P 9030 -u root

# Check cluster status
SHOW FRONTENDS\G
SHOW BACKENDS\G
SHOW PROC '/backends'\G

# Check storage usage
SHOW PROC '/statistic'\G
```

## High Availability Setup

For production HA, you should deploy:

- **3 FE nodes** (1 Leader, 2 Followers)
- **3+ BE nodes** (for data redundancy)

### Adding Additional FE Nodes

1. Update docker-compose.yml with additional FE services
2. Start the new FE as a Follower:
   ```sql
   ALTER SYSTEM ADD FOLLOWER "new-fe-host:9010";
   ```

### Adding Additional BE Nodes

```sql
ALTER SYSTEM ADD BACKEND "new-be-host:9050";
```

## Backup and Recovery

### Backup FE Metadata

```bash
# Stop FE temporarily
docker-compose stop starrocks-fe

# Backup metadata
sudo tar -czf fe-meta-backup-$(date +%Y%m%d).tar.gz /var/lib/starrocks/fe-meta

# Restart FE
docker-compose start starrocks-fe
```

### Backup BE Data

Use StarRocks BACKUP/RESTORE commands:

```sql
-- Create repository
CREATE REPOSITORY `backup_repo`
WITH BROKER
ON LOCATION "hdfs://backup-path/"
PROPERTIES (
    "username" = "user",
    "password" = "password"
);

-- Backup database
BACKUP SNAPSHOT database_name.snapshot_name
TO backup_repo;
```

## Troubleshooting

### FE Won't Start

1. Check logs: `docker-compose logs starrocks-fe`
2. Verify metadata directory permissions
3. Check memory allocation (min 8GB heap)
4. Ensure ports 8030, 9020, 9030 are not in use

### BE Won't Register

1. Verify FE is healthy first
2. Check BE logs: `docker-compose logs starrocks-be`
3. Ensure BE can reach FE on port 9020
4. Manually register: `ALTER SYSTEM ADD BACKEND "starrocks-be:9050";`

### Out of Memory Errors

1. Check container memory limits
2. Adjust `mem_limit` in BE configuration
3. Reduce JVM heap size for FE if needed
4. Monitor with: `docker stats`

### Storage Full

1. Check disk usage:
   ```sql
   SHOW PROC '/backends'\G
   ```
2. Clean up old data/compactions
3. Add more BE nodes for horizontal scaling

## Performance Optimization

### Query Performance

1. Enable query caching
2. Create appropriate indexes
3. Use materialized views
4. Partition large tables

### Load Performance

1. Use STREAM LOAD for real-time ingestion
2. Batch INSERT operations
3. Adjust `load_mem_limit` for large loads
4. Use multiple BE nodes for parallel loading

## Security Considerations

### Network Security

1. Use firewall rules to restrict access to StarRocks ports
2. Deploy behind nginx reverse proxy (included in compose file)
3. Enable SSL/TLS for MySQL connections

### Authentication

```sql
-- Create database users
CREATE USER 'app_user'@'%' IDENTIFIED BY 'strong_password';

-- Grant privileges
GRANT ALL ON database_name.* TO 'app_user'@'%';

-- Change root password
SET PASSWORD FOR 'root'@'%' = PASSWORD('new_root_password');
```

### Row-Level Security

Implement RLS policies in the `db/rls/` directory as per your project structure.

## Integration with Pidilite DataWiz

The ETL application connects to StarRocks via:

- **Host:** `starrocks-fe`
- **Port:** `9030`
- **Protocol:** MySQL
- **User:** `root`
- **Network:** `pidilite-network`

Update your connection configuration:

```python
STARROCKS_CONFIG = {
    'host': 'starrocks-fe',
    'port': 9030,
    'user': 'root',
    'password': os.getenv('STARROCKS_PASSWORD', ''),
    'database': 'pidilite'
}
```

## Scaling Guidelines

### Vertical Scaling

Adjust resource limits in docker-compose.yml:

```yaml
deploy:
  resources:
    limits:
      cpus: '16'      # Increase CPU
      memory: 64G     # Increase memory
```

### Horizontal Scaling

Add more BE nodes for:
- Increased storage capacity
- Better query parallelism
- Higher throughput

## References

- [StarRocks Deployment Overview](https://docs.starrocks.io/docs/deployment/deployment_overview/)
- [FE Configuration](https://docs.starrocks.io/docs/administration/management/FE_configuration/)
- [BE Configuration](https://docs.starrocks.io/docs/administration/management/BE_configuration/)
- [Plan StarRocks Cluster](https://docs.starrocks.io/docs/deployment/plan_cluster)
- [StarRocks Deployment Guide](https://www.starrocks.io/blog/four-simple-ways-to-deploy-starrocks)

## Support

For issues specific to this deployment:
1. Check container logs
2. Review StarRocks documentation
3. Verify resource allocation
4. Check network connectivity between containers
