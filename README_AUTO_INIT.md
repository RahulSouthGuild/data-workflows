# StarRocks Auto-Initialization

This configuration automatically creates a database and user when StarRocks starts for the first time.

## What Gets Created Automatically

When you run `./init-starrocks.sh all` or `docker-compose up -d`, the following will be created automatically:

### 📦 Database
- **Name:** `datawiz`
- **Character Set:** Default UTF-8

### 👤 User
- **Username:** `datawiz_admin`
- **Password:** `0jqhC3X541tP1RmR.5`
- **Host:** `%` (can connect from anywhere)

### 🔐 Permissions
The `datawiz_admin` user has:
- Full privileges on the `datawiz` database (SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, etc.)
- SELECT access to `information_schema.*`
- SELECT access to `_statistics_.*`

## How It Works

1. **FE Container Starts** → StarRocks Frontend initializes
2. **BE Container Starts** → Backend registers with FE (automatic via init script)
3. **Init Container Runs** → Executes [db/init/init-db.sh](db/init/init-db.sh)
   - Waits for FE to be ready
   - Creates database `datawiz`
   - Creates user `datawiz_admin`
   - Grants privileges
   - Exits successfully

## Files Involved

### Configuration Files
- [docker-compose.yml](docker-compose.yml:101-113) - Defines the `starrocks-init` service
- [db/init/init-db.sh](db/init/init-db.sh) - Bash script that creates DB and user
- [db/init/01-init-database.sql](db/init/01-init-database.sql) - SQL initialization script (reference)

### Connection Details
- [.env.starrocks](.env.starrocks) - Environment variables with credentials

## Quick Start

### Start the Cluster
```bash
# Option 1: Using the initialization script (recommended)
./init-starrocks.sh all

# Option 2: Using docker-compose directly
docker-compose up -d
```

### Verify Database Creation
```bash
# Check if database was created
mysql -h 127.0.0.1 -P 9030 -u datawiz_admin -p'0jqhC3X541tP1RmR.5' -e "SHOW DATABASES;"

# Should show:
# +--------------------+
# | Database           |
# +--------------------+
# | datawiz            |
# | information_schema |
# | _statistics_       |
# +--------------------+
```

### Connect to the Database
```bash
# Connect as datawiz_admin
mysql -h 127.0.0.1 -P 9030 -u datawiz_admin -p'0jqhC3X541tP1RmR.5' datawiz

# Or as root (for admin tasks)
mysql -h 127.0.0.1 -P 9030 -u root
```

## Connection Examples

### Command Line
```bash
# From host machine
mysql -h 127.0.0.1 -P 9030 -u datawiz_admin -p'0jqhC3X541tP1RmR.5' datawiz

# From another Docker container
mysql -h starrocks-fe -P 9030 -u datawiz_admin -p'0jqhC3X541tP1RmR.5' datawiz
```

### Python (pymysql)
```python
import pymysql

connection = pymysql.connect(
    host='localhost',      # or 'starrocks-fe' from Docker
    port=9030,
    user='datawiz_admin',
    password='0jqhC3X541tP1RmR.5',
    database='datawiz'
)

with connection.cursor() as cursor:
    cursor.execute("SELECT VERSION()")
    result = cursor.fetchone()
    print(f"StarRocks Version: {result[0]}")

connection.close()
```

### Python (SQLAlchemy)
```python
from sqlalchemy import create_engine

engine = create_engine(
    'mysql+pymysql://datawiz_admin:0jqhC3X541tP1RmR.5@localhost:9030/datawiz'
)

with engine.connect() as conn:
    result = conn.execute("SELECT VERSION()")
    print(result.fetchone())
```

### JDBC (Java)
```java
String url = "jdbc:mysql://localhost:9030/datawiz";
String user = "datawiz_admin";
String password = "0jqhC3X541tP1RmR.5";

Connection conn = DriverManager.getConnection(url, user, password);
```

## Troubleshooting

### Database Not Created
```bash
# Check init container logs
docker logs starrocks-init

# If it failed, restart it
docker-compose up -d starrocks-init

# Or manually create database
mysql -h 127.0.0.1 -P 9030 -u root < db/init/01-init-database.sql
```

### User Cannot Connect
```bash
# Verify user exists
mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT user, host FROM mysql.user WHERE user='datawiz_admin';"

# Check privileges
mysql -h 127.0.0.1 -P 9030 -u root -e "SHOW GRANTS FOR 'datawiz_admin'@'%';"

# If needed, recreate user
mysql -h 127.0.0.1 -P 9030 -u root -e "DROP USER IF EXISTS 'datawiz_admin'@'%';"
mysql -h 127.0.0.1 -P 9030 -u root < db/init/01-init-database.sql
```

### Init Container Keeps Restarting
The init container is designed to run once and exit. If you see it restarting:

```bash
# Check the logs
docker logs starrocks-init

# Common issues:
# 1. FE not ready - wait longer
# 2. Network issues - check docker network
# 3. MySQL client not in container - ensure mysql:8.0 image is used
```

## Re-initialization

If you need to re-run the initialization:

```bash
# Remove the init container
docker rm starrocks-init

# Restart it
docker-compose up -d starrocks-init

# Check logs
docker logs -f starrocks-init
```

## Security Notes

⚠️ **Important Security Considerations:**

1. **Change Default Password** - The password `0jqhC3X541tP1RmR.5` is stored in plain text. For production:
   ```sql
   -- Change datawiz_admin password
   SET PASSWORD FOR 'datawiz_admin'@'%' = PASSWORD('your_new_secure_password');

   -- Set root password
   SET PASSWORD FOR 'root'@'%' = PASSWORD('your_root_password');
   ```

2. **Use Environment Variables** - Store credentials in `.env` file (add to `.gitignore`):
   ```bash
   # .env
   STARROCKS_USER=datawiz_admin
   STARROCKS_PASSWORD=your_secure_password
   ```

3. **Restrict Network Access** - Limit who can connect:
   ```sql
   -- Create user that can only connect from specific IP
   CREATE USER 'datawiz_admin'@'192.168.1.0/255.255.255.0' IDENTIFIED BY 'password';
   ```

4. **Use Secrets Management** - For production, use Docker secrets or vault:
   ```yaml
   secrets:
     starrocks_password:
       external: true
   ```

## Next Steps

After initialization:

1. ✅ Database and user created automatically
2. 📊 Create your tables in the `datawiz` database
3. 📥 Load your data using INSERT, STREAM LOAD, or BROKER LOAD
4. 🔍 Query your data
5. 📈 Monitor via Web UI at http://localhost:8030

## Additional Resources

- [STARROCKS_QUICKSTART.md](STARROCKS_QUICKSTART.md) - Quick reference commands
- [STARROCKS_SETUP.md](STARROCKS_SETUP.md) - Complete production setup guide
- [docker-compose.yml](docker-compose.yml) - Full configuration
- [init-starrocks.sh](init-starrocks.sh) - Initialization script

## Support

If you encounter issues:
1. Check logs: `docker-compose logs starrocks-fe starrocks-be starrocks-init`
2. Verify health: `./init-starrocks.sh status`
3. Review documentation above
