# Docker Compose Files - Production vs Development

## Quick Comparison

| Aspect | docker-compose.yml (Production) | docker-compose.dev.yml (Development) |
|--------|--------------------------------|-------------------------------------|
| **Services** | 9 services | 4 services |
| **Size** | Full stack | Minimal stack |
| **Memory** | High (12GB+) | Low (6GB) |
| **Monitoring** | SignOz full stack | None |
| **Email** | Real SMTP | Mailhog (fake) |
| **Data Storage** | Docker volumes | Local folders |
| **Purpose** | Production deployment | Local development |
| **Auto-restart** | Yes (unless-stopped) | No |

## Detailed Comparison

### 1. Services Included

**docker-compose.yml (Production) - 9 Services:**
```yaml
✓ starrocks-fe              # Database frontend
✓ starrocks-be              # Database backend
✓ mongodb                   # Business constants
✓ signoz-otel-collector     # Telemetry collection
✓ signoz-query-service      # Metrics queries
✓ signoz-clickhouse         # Metrics storage
✓ signoz-frontend           # Monitoring dashboard
✓ pidilite-datawiz          # ETL application
✓ nginx                     # Reverse proxy
```

**docker-compose.dev.yml (Development) - 4 Services:**
```yaml
✓ starrocks-fe              # Database frontend
✓ starrocks-be              # Database backend
✓ mongodb                   # Business constants
✓ mailhog                   # Email testing
```

### 2. Resource Allocation

**Production (docker-compose.yml):**
```yaml
StarRocks FE:  4GB RAM  (JAVA_OPTS=-Xmx4g)
StarRocks BE:  8GB RAM  (JAVA_OPTS=-Xmx8g)
MongoDB:       Default
SignOz Stack:  ~2GB RAM
Total:         ~14GB RAM
```

**Development (docker-compose.dev.yml):**
```yaml
StarRocks FE:  2GB RAM  (JAVA_OPTS=-Xmx2g)
StarRocks BE:  4GB RAM  (JAVA_OPTS=-Xmx4g)
MongoDB:       Default
Mailhog:       Minimal
Total:         ~6GB RAM
```

**Why?**
- **Production**: Needs to handle real workloads, multiple concurrent jobs
- **Development**: Only for testing, single developer use

### 3. Data Persistence

**Production (docker-compose.yml):**
```yaml
# Uses named Docker volumes (persistent across container restarts)
volumes:
  starrocks-fe-meta:     # Permanent storage
  starrocks-be-storage:  # Permanent storage
  mongodb-data:          # Permanent storage
  clickhouse-data:       # Permanent storage
```

**Development (docker-compose.dev.yml):**
```yaml
# Uses local folders (easy to delete/reset)
volumes:
  - ./data/docker/starrocks-fe:/opt/starrocks/fe/meta
  - ./data/docker/starrocks-be:/opt/starrocks/be/storage
  - ./data/docker/mongodb:/data/db
```

**Why?**
- **Production**: Docker volumes are managed by Docker, backed up easily
- **Development**: Local folders are easy to delete (`rm -rf data/docker/`) to start fresh

### 4. Monitoring Stack

**Production (docker-compose.yml):**
```yaml
✓ SignOz OpenTelemetry Collector  # Collects traces/metrics
✓ SignOz Query Service            # Queries metrics
✓ ClickHouse                      # Stores metrics
✓ SignOz Frontend                 # Dashboard UI

Access: http://localhost:3301
```

**Development (docker-compose.dev.yml):**
```yaml
✗ No monitoring stack
```

**Why?**
- **Production**: Need to monitor job performance, errors, bottlenecks
- **Development**: Monitoring adds overhead, not needed for local testing

### 5. Email Handling

**Production (docker-compose.yml):**
```yaml
# Uses real SMTP server (configured in .env)
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
```

**Development (docker-compose.dev.yml):**
```yaml
# Uses Mailhog (fake SMTP server)
mailhog:
  ports:
    - "1025:1025"  # SMTP server
    - "8025:8025"  # Web UI to view emails

Access: http://localhost:8025
```

**Why?**
- **Production**: Sends real emails to admin@pidilite.com
- **Development**: Mailhog captures emails, no real sending (safe for testing)

### 6. Application Container

**Production (docker-compose.yml):**
```yaml
pidilite-datawiz:
  build: .                    # Builds from Dockerfile
  environment:
    - ENVIRONMENT=production  # Production mode
  restart: unless-stopped     # Auto-restart on failure
  command: python scheduler/orchestrator.py
```

**Development (docker-compose.dev.yml):**
```yaml
# No application container
# Run manually on host machine
```

**Why?**
- **Production**: Application runs automatically, restarts on failure
- **Development**: Run app manually from IDE for debugging

### 7. Networking

**Production (docker-compose.yml):**
```yaml
networks:
  pidilite-network:
    driver: bridge
    ipam:
      config:
        - subnet: 172.28.0.0/16  # Fixed subnet
```

**Development (docker-compose.dev.yml):**
```yaml
networks:
  pidilite-dev-network:
    driver: bridge  # Simple bridge network
```

**Why?**
- **Production**: Fixed IPs for security/firewall rules
- **Development**: Simple networking is enough

### 8. Container Names

**Production (docker-compose.yml):**
```yaml
pidilite-starrocks-fe
pidilite-mongodb
pidilite-datawiz-app
```

**Development (docker-compose.dev.yml):**
```yaml
pidilite-dev-starrocks-fe
pidilite-dev-mongodb
```

**Why?**
- **Production**: Clean names for production
- **Development**: `-dev-` prefix to distinguish from production containers

### 9. Health Checks

**Production (docker-compose.yml):**
```yaml
healthcheck:
  test: ["CMD", "curl", "-f", "http://localhost:9030"]
  interval: 30s
  timeout: 10s
  retries: 3
```

**Development (docker-compose.dev.yml):**
```yaml
# No health checks
```

**Why?**
- **Production**: Monitor service health, restart if unhealthy
- **Development**: Not critical, reduces overhead

### 10. Restart Policy

**Production (docker-compose.yml):**
```yaml
restart: unless-stopped  # Always restart unless manually stopped
```

**Development (docker-compose.dev.yml):**
```yaml
# No restart policy (defaults to "no")
```

**Why?**
- **Production**: Services must stay running, auto-recover from crashes
- **Development**: Manual control, stop when done

## When to Use Each

### Use docker-compose.yml (Production) When:

✅ Deploying to production server
✅ Need full monitoring and observability
✅ Require automatic restarts
✅ Want production-like environment
✅ Testing with real email sending
✅ Performance testing under load
✅ Permanent data storage needed

**Command:**
```bash
docker-compose up -d
```

### Use docker-compose.dev.yml (Development) When:

✅ Local development on laptop
✅ Testing code changes
✅ Debugging issues
✅ Limited RAM/resources
✅ Need to reset database frequently
✅ Testing email notifications (safely)
✅ Running on developer machine

**Command:**
```bash
docker-compose -f docker-compose.dev.yml up -d
```

## Side-by-Side Feature Matrix

| Feature | Production | Development |
|---------|-----------|-------------|
| **StarRocks** | ✅ Full (12GB) | ✅ Lightweight (6GB) |
| **MongoDB** | ✅ Persistent | ✅ Local folder |
| **Monitoring** | ✅ SignOz stack | ❌ None |
| **Email** | ✅ Real SMTP | ✅ Mailhog (fake) |
| **ETL App** | ✅ Container | ❌ Run manually |
| **Nginx** | ✅ Reverse proxy | ❌ Not needed |
| **Auto-restart** | ✅ Yes | ❌ No |
| **Health checks** | ✅ Yes | ❌ No |
| **Memory usage** | 14GB+ | ~6GB |
| **Startup time** | Slower (9 services) | Faster (4 services) |
| **Data persistence** | Docker volumes | Local folders |
| **Network config** | Advanced | Simple |

## Typical Workflow

### Development Flow

```bash
# 1. Start infrastructure
docker-compose -f docker-compose.dev.yml up -d

# 2. Run application from IDE/terminal
export DB_HOST=localhost
export MONGODB_URI=mongodb://admin:devpassword@localhost:27017
export SMTP_HOST=localhost
export SMTP_PORT=1025
python scheduler/orchestrator.py

# 3. View emails (no real sending)
open http://localhost:8025

# 4. Stop infrastructure
docker-compose -f docker-compose.dev.yml down

# 5. Reset database (fresh start)
rm -rf data/docker/*
docker-compose -f docker-compose.dev.yml up -d
```

### Production Flow

```bash
# 1. Start everything
docker-compose up -d --build

# 2. Monitor services
docker-compose logs -f pidilite-datawiz

# 3. View monitoring dashboard
open http://localhost:3301

# 4. Services auto-restart on failure
# No manual intervention needed

# 5. Graceful shutdown
docker-compose down
```

## Environment Variables

### Production (.env):
```env
ENVIRONMENT=production
DB_HOST=starrocks-fe
MONGODB_URI=mongodb://admin:changeme@mongodb:27017
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your_real_email@gmail.com
SIGNOZ_ENDPOINT=http://signoz-otel-collector:4317
ENABLE_TRACING=true
```

### Development (.env.dev):
```env
ENVIRONMENT=development
DB_HOST=localhost
MONGODB_URI=mongodb://admin:devpassword@localhost:27017
SMTP_HOST=localhost
SMTP_PORT=1025
ENABLE_TRACING=false
```

## Memory Requirements

### Production:
```
StarRocks FE:    4GB
StarRocks BE:    8GB
MongoDB:         1GB
SignOz Stack:    2GB
ETL App:         1GB
Total:          16GB minimum
```

### Development:
```
StarRocks FE:    2GB
StarRocks BE:    4GB
MongoDB:         512MB
Mailhog:         64MB
Total:          ~7GB minimum
```

## Port Mappings

### Production (docker-compose.yml):
```
8030  → StarRocks FE UI
9030  → StarRocks MySQL port
8040  → StarRocks BE UI
27017 → MongoDB
4317  → OpenTelemetry (gRPC)
3301  → SignOz Dashboard
80    → Nginx HTTP
443   → Nginx HTTPS
```

### Development (docker-compose.dev.yml):
```
8030  → StarRocks FE UI
9030  → StarRocks MySQL port
8040  → StarRocks BE UI
27017 → MongoDB
8025  → Mailhog UI
1025  → Mailhog SMTP
```

## Summary

### docker-compose.yml (Production)
- **Purpose**: Production deployment
- **Services**: 9 (full stack)
- **Memory**: ~16GB
- **Features**: Monitoring, auto-restart, persistent storage
- **Use for**: Production servers, staging environments

### docker-compose.dev.yml (Development)
- **Purpose**: Local development
- **Services**: 4 (minimal)
- **Memory**: ~7GB
- **Features**: Mailhog, local folders, no monitoring
- **Use for**: Developer laptops, testing, debugging

**Key Insight**: Use development for coding, use production for deployment!
