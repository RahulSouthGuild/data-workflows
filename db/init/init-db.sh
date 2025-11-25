#!/bin/bash
# StarRocks Database Initialization Script
# This script waits for FE to be ready and then creates database and user

set -e

# Wait for FE to be ready
echo "Waiting for StarRocks FE to be ready..."
max_attempts=60
attempt=1

while [ $attempt -le $max_attempts ]; do
    if mysql -h starrocks-fe -P 9030 -u root -e "SELECT 1" &> /dev/null; then
        echo "StarRocks FE is ready!"
        break
    fi

    if [ $attempt -eq $max_attempts ]; then
        echo "ERROR: StarRocks FE did not become ready in time"
        exit 1
    fi

    echo "Attempt $attempt/$max_attempts - FE not ready yet, waiting..."
    sleep 2
    attempt=$((attempt + 1))
done

# Execute initialization SQL
echo "Creating database and user..."
mysql -h starrocks-fe -P 9030 -u root << 'EOF'
-- Create the datawiz database
CREATE DATABASE IF NOT EXISTS datawiz;

-- Create the datawiz_admin user with password
CREATE USER IF NOT EXISTS 'datawiz_admin'@'%' IDENTIFIED BY '0jqhC3X541tP1RmR.5';

-- Grant all privileges on datawiz database to datawiz_admin
GRANT ALL PRIVILEGES ON datawiz.* TO 'datawiz_admin'@'%';

-- Grant necessary global privileges for user operations
GRANT SELECT ON information_schema.* TO 'datawiz_admin'@'%';

-- Show databases
SHOW DATABASES;
EOF

if [ $? -eq 0 ]; then
    echo "✓ Database 'datawiz' created successfully"
    echo "✓ User 'datawiz_admin' created successfully"
    echo "✓ Privileges granted"
else
    echo "✗ Database initialization failed"
    exit 1
fi
