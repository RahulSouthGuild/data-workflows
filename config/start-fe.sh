#!/bin/bash

set -e

FE_NODE=${1:-fe1}

echo "[START-FE] Starting StarRocks $FE_NODE on $(date)"

# Wait for FE1 if this is FE2
if [ "$FE_NODE" = "fe2" ]; then
    echo "[START-FE] This is FE2, waiting 30s for FE1 to stabilize..."
    sleep 30
fi

# Create required directories
mkdir -p /opt/starrocks/fe/meta
mkdir -p /opt/starrocks/fe/log

# Copy configuration from mounted volume
CONFIG_FILE="/config/fe_configs/${FE_NODE}.conf"
if [ -f "$CONFIG_FILE" ]; then
    echo "[START-FE] Loading FE configuration from $CONFIG_FILE"
    cp "$CONFIG_FILE" /opt/starrocks/fe/conf/fe.conf
else
    echo "[START-FE] ERROR: Config file not found at $CONFIG_FILE"
    exit 1
fi

echo "[START-FE] Starting FE service..."
exec bash /opt/starrocks/fe/bin/start_fe.sh
