#!/bin/bash

set -e

HOSTNAME=$1
DELAY=${2:-35}
FE_HOST=${3:-starrocks-fe1}
FE_PORT=${4:-9030}
BE_NODE=${5:-be1}

echo "[START-BE] Starting StarRocks $BE_NODE on $(date)"
echo "[START-BE] Parameters: HOSTNAME=$HOSTNAME, DELAY=$DELAY, FE_HOST=$FE_HOST, FE_PORT=$FE_PORT, NODE=$BE_NODE"

# Increase file descriptor limits
ulimit -n 65535
ulimit -c unlimited

# Create required directories
mkdir -p /opt/starrocks/be/storage
mkdir -p /opt/starrocks/be/log

# Copy configuration from mounted volume
CONFIG_FILE="/config/be_configs/${BE_NODE}.conf"
if [ -f "$CONFIG_FILE" ]; then
    echo "[START-BE] Loading BE configuration from $CONFIG_FILE"
    cp "$CONFIG_FILE" /opt/starrocks/be/conf/be.conf
else
    echo "[START-BE] ERROR: Config file not found at $CONFIG_FILE"
    exit 1
fi

# Wait for FE to be ready
echo "[START-BE] Waiting ${DELAY}s for FE to stabilize..."
sleep $DELAY

# Attempt to drop existing backend first (if it exists)
echo "[START-BE] Attempting to remove existing backend registration (if any)..."
mysql --connect-timeout 10 -h "$FE_HOST" -P "$FE_PORT" -u root \
    -e "ALTER SYSTEM DROP BACKEND \"${HOSTNAME}:9050\";" 2>/dev/null || true

# Wait a moment after dropping
sleep 2

# Attempt to register backend with FE (with retry logic)
echo "[START-BE] Attempting to register $BE_NODE with FE at $FE_HOST:$FE_PORT"

RETRY_COUNT=0
MAX_RETRIES=5

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if mysql --connect-timeout 10 -h "$FE_HOST" -P "$FE_PORT" -u root \
        -e "ALTER SYSTEM ADD BACKEND \"${HOSTNAME}:9050\";" 2>/dev/null; then
        echo "[START-BE] ✓ Successfully registered $BE_NODE with FE"
        break
    else
        RETRY_COUNT=$((RETRY_COUNT + 1))
        if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
            echo "[START-BE] Registration attempt $RETRY_COUNT/$MAX_RETRIES failed, retrying in 5s..."
            sleep 5
        else
            echo "[START-BE] ✗ Failed to register after $MAX_RETRIES attempts"
        fi
    fi
done

# Start the BE service
echo "[START-BE] Starting BE service..."
exec bash /opt/starrocks/be/bin/start_be.sh
