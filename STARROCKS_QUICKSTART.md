# StarRocks Production Quick Start

## Prerequisites
```bash
# Ensure Docker and Docker Compose are installed
docker --version
docker-compose --version
```

## Quick Start (3 Steps)

### 1. Initialize System
```bash
./init-starrocks.sh all
```

This will:
- Create required directories
- Start containers
- Register backend
- Show cluster status

### 2. Verify Cluster
```bash
# Check health
curl http://localhost:8030/api/health
curl http://localhost:8040/api/health

# Check cluster status
./init-starrocks.sh status
```

### 3. Connect & Test
```bash
# Connect to StarRocks
mysql -h 127.0.0.1 -P 9030 -u root

# Create database
CREATE DATABASE IF NOT EXISTS pidilite;
USE pidilite;

# Test query
SELECT VERSION();
```

## Common Commands

### Container Management
```bash
# Start services
docker-compose up -d starrocks-fe starrocks-be

# Stop services
docker-compose stop starrocks-fe starrocks-be

# Restart services
docker-compose restart starrocks-fe starrocks-be

# View logs
docker-compose logs -f starrocks-fe
docker-compose logs -f starrocks-be

# Remove containers (keeps data)
docker-compose down

# Remove containers and data (WARNING: destructive)
docker-compose down -v
```

### Cluster Operations
```bash
# Connect to MySQL interface
mysql -h 127.0.0.1 -P 9030 -u root

# Check FE nodes
SHOW FRONTENDS\G

# Check BE nodes
SHOW BACKENDS\G

# Check BE details
SHOW PROC '/backends'\G

# Add additional BE
ALTER SYSTEM ADD BACKEND "new-be-host:9050";

# Drop BE (safe - no data loss if replicas exist)
ALTER SYSTEM DROP BACKEND "be-host:9050";
```

### Monitoring
```bash
# Real-time resource usage
docker stats pidilite-starrocks-fe pidilite-starrocks-be

# Check health endpoints
curl http://localhost:8030/api/health
curl http://localhost:8040/api/health

# View metrics
curl http://localhost:8030/metrics
curl http://localhost:8040/metrics

# Disk usage
docker exec pidilite-starrocks-be df -h /opt/starrocks/be/storage
```

### Database Operations
```sql
-- Show databases
SHOW DATABASES;

-- Show tables
SHOW TABLES FROM pidilite;

-- Check table details
SHOW CREATE TABLE pidilite.table_name;

-- Show partitions
SHOW PARTITIONS FROM pidilite.table_name;

-- Check tablet distribution
SHOW TABLET FROM pidilite.table_name;

-- Analyze table statistics
ANALYZE TABLE pidilite.table_name;
```

## Access Points

| Service | URL/Command | Purpose |
|---------|-------------|---------|
| FE Web UI | http://localhost:8030 | Frontend management interface |
| MySQL Protocol | `mysql -h 127.0.0.1 -P 9030 -u root` | SQL queries and admin |
| BE Web UI | http://localhost:8040 | Backend monitoring |
| FE Metrics | http://localhost:8030/metrics | Prometheus metrics |
| BE Metrics | http://localhost:8040/metrics | Prometheus metrics |

## Resource Configuration

### Current Settings

**Frontend (FE):**
- CPU Limit: 4 cores (Reserved: 2 cores)
- Memory Limit: 16GB (Reserved: 8GB)
- JVM Heap: 8GB

**Backend (BE):**
- CPU Limit: 8 cores (Reserved: 4 cores)
- Memory Limit: 32GB (Reserved: 16GB)
- BE Memory: 85% of container (27GB)

### Adjusting Resources

Edit [docker-compose.yml](docker-compose.yml:23-30) and modify:

```yaml
deploy:
  resources:
    limits:
      cpus: '8'      # Adjust as needed
      memory: 32G    # Adjust as needed
```

Then restart:
```bash
docker-compose up -d --force-recreate starrocks-fe starrocks-be
```

## Troubleshooting

### FE Won't Start
```bash
# Check logs
docker-compose logs starrocks-fe

# Check port conflicts
netstat -tlnp | grep -E '8030|9020|9030'

# Verify metadata directory
ls -la /var/lib/starrocks/fe-meta

# Restart with fresh logs
docker-compose restart starrocks-fe
```

### BE Won't Connect
```bash
# Check BE logs
docker-compose logs starrocks-be

# Verify FE is healthy
curl http://localhost:8030/api/health

# Check network connectivity
docker exec pidilite-starrocks-be ping -c 3 starrocks-fe

# Manually register BE
mysql -h 127.0.0.1 -P 9030 -u root -e "ALTER SYSTEM ADD BACKEND 'starrocks-be:9050';"
```

### Query Performance Issues
```sql
-- Check query profile
SET enable_profile = true;
SELECT ...;
SHOW QUERY PROFILE;

-- Check BE load
SHOW PROC '/backends'\G

-- View running queries
SHOW PROCESSLIST;

-- Kill slow query
KILL QUERY connection_id;
```

### Storage Issues
```bash
# Check disk usage
docker exec pidilite-starrocks-be df -h

# Check BE storage
mysql -h 127.0.0.1 -P 9030 -u root -e "SHOW PROC '/backends'\G" | grep -E 'DataUsed|AvailCapacity'

# Clean up old data
# In MySQL:
DROP TABLE old_table;
```

## Backup & Recovery

### Quick Backup
```bash
# Stop FE (optional, for consistent backup)
docker-compose stop starrocks-fe

# Backup metadata
sudo tar -czf fe-meta-backup-$(date +%Y%m%d-%H%M).tar.gz /var/lib/starrocks/fe-meta

# Backup BE data (if small enough)
sudo tar -czf be-storage-backup-$(date +%Y%m%d-%H%M).tar.gz /var/lib/starrocks/be-storage

# Restart FE
docker-compose start starrocks-fe
```

### Quick Restore
```bash
# Stop services
docker-compose stop starrocks-fe starrocks-be

# Restore metadata
sudo tar -xzf fe-meta-backup-YYYYMMDD-HHMM.tar.gz -C /

# Restore BE data
sudo tar -xzf be-storage-backup-YYYYMMDD-HHMM.tar.gz -C /

# Start services
docker-compose up -d starrocks-fe starrocks-be
```

## Security Checklist

- [ ] Change root password
- [ ] Create application-specific users
- [ ] Configure firewall rules
- [ ] Enable SSL/TLS (if exposing externally)
- [ ] Set up regular backups
- [ ] Monitor logs for suspicious activity
- [ ] Restrict Docker host access
- [ ] Use secrets management for passwords

### Change Root Password
```sql
-- Connect as root
mysql -h 127.0.0.1 -P 9030 -u root

-- Set password
SET PASSWORD FOR 'root'@'%' = PASSWORD('your_strong_password');
```

### Create Application User
```sql
-- Create user
CREATE USER 'pidilite_app'@'%' IDENTIFIED BY 'app_password';

-- Grant privileges
GRANT ALL PRIVILEGES ON pidilite.* TO 'pidilite_app'@'%';

-- Verify
SHOW GRANTS FOR 'pidilite_app'@'%';
```

## Production Checklist

Before going live:

- [ ] Verify resource allocation meets requirements
- [ ] Configure proper backups
- [ ] Set up monitoring/alerting
- [ ] Change default passwords
- [ ] Enable logging to persistent storage
- [ ] Configure firewall rules
- [ ] Document connection strings
- [ ] Test failover scenarios (if HA)
- [ ] Tune performance parameters
- [ ] Set up SSL/TLS certificates

## Next Steps

1. **Configure Tables:** Create your schema in `db/schemas/`
2. **Load Data:** Set up ETL pipelines in `core/`
3. **Monitoring:** Integrate with SignOz (already in compose)
4. **Optimization:** Review query performance
5. **Scale:** Add more BE nodes as needed

## Additional Resources

- Full documentation: [STARROCKS_SETUP.md](STARROCKS_SETUP.md)
- Official docs: https://docs.starrocks.io/
- Configuration: [docker-compose.yml](docker-compose.yml)
