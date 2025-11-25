#!/bin/bash
# Backend Registration Script
# This script registers the BE with the FE automatically

set -e

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

# Wait a bit more for FE to fully initialize
sleep 5

echo "Checking if BE is already registered..."
BE_COUNT=$(mysql -h starrocks-fe -P 9030 -u root -sN -e "SHOW BACKENDS" 2>/dev/null | wc -l || echo "0")

if [ "$BE_COUNT" -gt 0 ]; then
    echo "Backend already registered (count: $BE_COUNT)"
    echo "Current backends:"
    mysql -h starrocks-fe -P 9030 -u root -e "SHOW BACKENDS\G"
    exit 0
fi

echo "Registering backend..."
mysql -h starrocks-fe -P 9030 -u root -e "ALTER SYSTEM ADD BACKEND 'starrocks-be:9050';"

# Wait for registration
sleep 3

echo "Verifying backend registration..."
BE_COUNT=$(mysql -h starrocks-fe -P 9030 -u root -sN -e "SHOW BACKENDS" 2>/dev/null | wc -l || echo "0")

if [ "$BE_COUNT" -gt 0 ]; then
    echo "✓ Backend registered successfully!"
    mysql -h starrocks-fe -P 9030 -u root -e "SHOW BACKENDS\G" | head -20
else
    echo "✗ Backend registration failed"
    exit 1
fi
