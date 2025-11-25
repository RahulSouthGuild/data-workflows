# Docker Deployment Guide - Pidilite DataWiz

## Overview

This guide covers Docker deployment for Pidilite DataWiz ETL pipeline with all dependencies.

## File Locations

```
pidilite_datawiz/
├── docker-compose.yml           # Production environment
├── docker-compose.dev.yml       # Development environment
├── Dockerfile                   # Application container
├── .dockerignore               # Docker build exclusions
└── docker/                     # Docker-related configs
    └── nginx/
        ├── nginx.conf          # Nginx configuration
        └── ssl/                # SSL certificates
```

## Quick Start

### Development Environment

```bash
# Start development services (StarRocks, MongoDB, Mailhog)
docker-compose -f docker-compose.dev.yml up -d

# View logs
docker-compose -f docker-compose.dev.yml logs -f

# Stop services
docker-compose -f docker-compose.dev.yml down
```

### Production Environment

```bash
# Build and start all services
docker-compose up -d --build

# View logs
docker-compose logs -f pidilite-datawiz

# Stop all services
docker-compose down

# Stop and remove volumes (⚠️ deletes data)
docker-compose down -v
```

## Services Included

### Production (docker-compose.yml)

| Service | Port | Description |
|---------|------|-------------|
| starrocks-fe | 9030, 8030 | StarRocks Frontend (query coordinator) |
| starrocks-be | 8040, 9060 | StarRocks Backend (data storage) |
| mongodb | 27017 | MongoDB (business constants) |
| signoz-otel-collector | 4317, 4318 | OpenTelemetry collector |
| signoz-query-service | 8080 | SignOz query service |
| signoz-clickhouse | 9000, 8123 | ClickHouse (metrics storage) |
| signoz-frontend | 3301 | SignOz dashboard UI |
| pidilite-datawiz | - | ETL application |
| nginx | 80, 443 | Reverse proxy (optional) |

### Development (docker-compose.dev.yml)

| Service | Port | Description |
|---------|------|-------------|
| starrocks-fe | 9030, 8030 | StarRocks Frontend |
| starrocks-be | 8040 | StarRocks Backend |
| mongodb | 27017 | MongoDB |
| mailhog | 8025, 1025 | Email testing UI & SMTP |

## Configuration

### 1. Environment Variables

Create `.env` file:

```bash
cp .env.example .env
```

Update for Docker:

```env
# Database (use container name as host)
DB_HOST=starrocks-fe
DB_PORT=9030
DB_NAME=datawiz
DB_USER=root
DB_PASSWORD=

# MongoDB (use container name as host)
MONGODB_URI=mongodb://admin:changeme@mongodb:27017

# SignOz (use container name as host)
SIGNOZ_ENDPOINT=http://signoz-otel-collector:4317
ENABLE_TRACING=true
ENABLE_METRICS=true

# Email (use Mailhog in dev)
SMTP_HOST=mailhog
SMTP_PORT=1025
```

### 2. Azure Blob Storage

Azure connection must be configured in `.env`:

```env
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;...
AZURE_CONTAINER_NAME=your-container
```

**Note**: Azure Blob Storage is not containerized. You need access to your actual Azure account.

## Building the Application

### Build Docker Image

```bash
# Build image
docker build -t pidilite-datawiz:latest .

# Build with specific tag
docker build -t pidilite-datawiz:2.0.0 .

# Build without cache
docker build --no-cache -t pidilite-datawiz:latest .
```

### Using Docker Compose

```bash
# Build services defined in docker-compose.yml
docker-compose build

# Build specific service
docker-compose build pidilite-datawiz

# Build with no cache
docker-compose build --no-cache
```

## Running Services

### Start All Services

```bash
# Production
docker-compose up -d

# Development
docker-compose -f docker-compose.dev.yml up -d

# With build
docker-compose up -d --build
```

### Start Specific Services

```bash
# Only StarRocks
docker-compose up -d starrocks-fe starrocks-be

# Only MongoDB
docker-compose up -d mongodb

# Only monitoring
docker-compose up -d signoz-otel-collector signoz-query-service signoz-frontend
```

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f pidilite-datawiz

# Last 100 lines
docker-compose logs --tail=100 pidilite-datawiz

# Since specific time
docker-compose logs --since 2023-01-01 pidilite-datawiz
```

### Stop Services

```bash
# Stop all
docker-compose stop

# Stop specific service
docker-compose stop pidilite-datawiz

# Stop and remove containers
docker-compose down

# Stop and remove containers + volumes
docker-compose down -v
```

## Database Initialization

### Initialize StarRocks

```bash
# Wait for StarRocks to be ready
docker-compose exec starrocks-fe bash -c "until curl -s http://localhost:9030; do sleep 2; done"

# Connect to StarRocks
docker-compose exec starrocks-fe mysql -h127.0.0.1 -P9030 -uroot

# Or from host (if mysql client installed)
mysql -h127.0.0.1 -P9030 -uroot

# Run initialization scripts
docker-compose exec -T starrocks-fe mysql -h127.0.0.1 -P9030 -uroot < db/create_tables.sql
```

### Initialize MongoDB

```bash
# Connect to MongoDB
docker-compose exec mongodb mongosh -u admin -p changeme

# Or from host (if mongosh installed)
mongosh "mongodb://admin:changeme@localhost:27017"

# Initialize business constants
docker-compose exec pidilite-datawiz python -c "from utils.mongo_client import initialize_business_constants; initialize_business_constants()"
```

## Accessing Services

### Web UIs

- **StarRocks FE UI**: http://localhost:8030
- **StarRocks BE UI**: http://localhost:8040
- **SignOz Dashboard**: http://localhost:3301
- **Mailhog (dev)**: http://localhost:8025

### Database Connections

```bash
# StarRocks (MySQL protocol)
mysql -h127.0.0.1 -P9030 -uroot

# MongoDB
mongosh "mongodb://admin:changeme@localhost:27017"
```

## Data Persistence

### Volumes

Persistent data is stored in Docker volumes:

```bash
# List volumes
docker volume ls | grep pidilite

# Inspect volume
docker volume inspect pidilite_datawiz_starrocks-fe-meta

# Backup volume
docker run --rm -v pidilite_datawiz_starrocks-fe-meta:/data -v $(pwd):/backup ubuntu tar czf /backup/starrocks-fe-backup.tar.gz /data

# Restore volume
docker run --rm -v pidilite_datawiz_starrocks-fe-meta:/data -v $(pwd):/backup ubuntu tar xzf /backup/starrocks-fe-backup.tar.gz -C /
```

### Data Directory Mapping

Application data is mounted from host:

```yaml
volumes:
  - ./data:/app/data          # Data files
  - ./logs:/app/logs          # Log files
  - ./config:/app/config      # Configuration
```

This allows:
- Persistent data across container restarts
- Easy access to logs
- Configuration updates without rebuild

## Troubleshooting

### Check Service Health

```bash
# Check all containers
docker-compose ps

# Check specific service
docker inspect --format='{{.State.Health.Status}}' pidilite-datawiz

# View health check logs
docker inspect --format='{{json .State.Health}}' pidilite-datawiz | jq
```

### Container Not Starting

```bash
# View logs
docker-compose logs pidilite-datawiz

# Check container status
docker ps -a | grep pidilite

# Inspect container
docker inspect pidilite-datawiz

# Enter container for debugging
docker-compose exec pidilite-datawiz bash
```

### Database Connection Issues

```bash
# Test StarRocks connectivity from container
docker-compose exec pidilite-datawiz python -c "
from config.database import create_main_pool
engine = create_main_pool()
print('Connected:', engine)
"

# Test MongoDB connectivity
docker-compose exec pidilite-datawiz python -c "
from utils.mongo_client import get_mongo_client
client = get_mongo_client()
print('Connected:', client.server_info())
"
```

### Network Issues

```bash
# List networks
docker network ls

# Inspect network
docker network inspect pidilite_datawiz_pidilite-network

# Test connectivity between containers
docker-compose exec pidilite-datawiz ping starrocks-fe
docker-compose exec pidilite-datawiz ping mongodb
```

### Resource Issues

```bash
# Check resource usage
docker stats

# Check disk usage
docker system df

# Clean up unused resources
docker system prune -a
docker volume prune
```

## Production Deployment

### Pre-deployment Checklist

- [ ] Update `.env` with production credentials
- [ ] Configure Azure Blob Storage connection
- [ ] Setup SSL certificates (if using nginx)
- [ ] Configure backup strategy
- [ ] Test email notifications
- [ ] Setup monitoring alerts

### Deployment Steps

```bash
# 1. Pull latest code
git pull origin main

# 2. Build images
docker-compose build

# 3. Stop old containers (if any)
docker-compose down

# 4. Start new containers
docker-compose up -d

# 5. Initialize database (first time only)
docker-compose exec -T starrocks-fe mysql -h127.0.0.1 -P9030 -uroot < db/create_tables.sql

# 6. Verify services
docker-compose ps
docker-compose logs -f pidilite-datawiz

# 7. Test ETL job
docker-compose exec pidilite-datawiz python -m scheduler.daily.evening.dimension_sync
```

### Backup Strategy

```bash
# Automated backup script
cat > backup.sh << 'EOF'
#!/bin/bash
BACKUP_DIR=/backups/$(date +%Y%m%d)
mkdir -p $BACKUP_DIR

# Backup StarRocks
docker exec pidilite-starrocks-fe mysqldump -h127.0.0.1 -P9030 -uroot datawiz > $BACKUP_DIR/starrocks.sql

# Backup MongoDB
docker exec pidilite-mongodb mongodump --archive=$BACKUP_DIR/mongodb.archive

# Backup application data
tar czf $BACKUP_DIR/data.tar.gz ./data

# Upload to cloud storage (Azure Blob)
az storage blob upload --file $BACKUP_DIR/* --container backups
EOF

chmod +x backup.sh

# Add to crontab
# 0 2 * * * /path/to/backup.sh
```

## Monitoring

### View Application Metrics

```bash
# SignOz Dashboard
open http://localhost:3301

# Check job execution
docker-compose exec pidilite-datawiz python -c "
from scheduler.job_registry import get_job_history
print(get_job_history())
"
```

### Log Aggregation

```bash
# Centralized logging with Docker
docker-compose logs -f --tail=100 | tee all-services.log

# Export logs
docker-compose logs --no-color > logs-$(date +%Y%m%d).txt
```

## Development Workflow

### Local Development with Docker

```bash
# Start infrastructure only
docker-compose -f docker-compose.dev.yml up -d

# Run application locally
export DB_HOST=localhost
export MONGODB_URI=mongodb://admin:devpassword@localhost:27017
python scheduler/orchestrator.py

# Or develop inside container
docker-compose run --rm pidilite-datawiz bash
```

### Hot Reload

Update `docker-compose.yml` for development:

```yaml
pidilite-datawiz:
  volumes:
    - ./config:/app/config
    - ./core:/app/core
    - ./scheduler:/app/scheduler
    # ... other source directories
  command: python scheduler/orchestrator.py
```

Code changes on host are immediately reflected in container.

## Security Best Practices

1. **Never commit `.env` file**
   ```bash
   echo ".env" >> .gitignore
   ```

2. **Use secrets management**
   ```yaml
   secrets:
     db_password:
       file: ./secrets/db_password.txt
   ```

3. **Limit container permissions**
   ```yaml
   user: "1000:1000"  # Run as non-root
   read_only: true
   ```

4. **Network segmentation**
   ```yaml
   networks:
     frontend:
     backend:
       internal: true  # No external access
   ```

## Scaling

### Horizontal Scaling

```bash
# Scale ETL workers
docker-compose up -d --scale pidilite-datawiz=3

# Use with job queue (Redis/RabbitMQ)
```

### Resource Limits

```yaml
pidilite-datawiz:
  deploy:
    resources:
      limits:
        cpus: '2.0'
        memory: 4G
      reservations:
        cpus: '1.0'
        memory: 2G
```

## Summary

- **Store Docker files at project root**: `docker-compose.yml`, `Dockerfile`
- **Use docker-compose.dev.yml** for local development
- **Mount source code** as volumes for easy development
- **Persist data** using Docker volumes
- **Configure networking** properly for inter-service communication
- **Monitor services** using SignOz dashboard
- **Backup regularly** using automated scripts

For more details, see the main [README_NEW.md](../README_NEW.md) and [QUICK_START.md](QUICK_START.md).
