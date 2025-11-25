#!/bin/bash

################################################################################
# StarRocks Initialization Test Script
# This script tests that the automated initialization works correctly
################################################################################

set -e

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_step() {
    echo ""
    echo -e "${BLUE}===================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}===================================================${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

################################################################################
# Step 1: Clean up existing setup
################################################################################

print_step "Step 1: Cleaning up existing setup"

echo "Stopping and removing containers..."
docker-compose down -v 2>&1 | grep -E "Stopped|Removed" || true

echo "Cleaning data directories (requires sudo)..."
print_info "You may be prompted for your password"
sudo rm -rf /var/lib/starrocks/*
sudo mkdir -p /var/lib/starrocks/{fe-meta,fe-log,be-storage,be-log}
sudo chown -R $USER:$USER /var/lib/starrocks

print_success "Cleanup complete"

################################################################################
# Step 2: Start fresh cluster
################################################################################

print_step "Step 2: Starting fresh StarRocks cluster"

echo "Starting containers..."
docker-compose up -d

echo "Waiting for containers to start..."
sleep 10

print_success "Containers started"

################################################################################
# Step 3: Wait for FE to be healthy
################################################################################

print_step "Step 3: Waiting for FE to be healthy"

max_attempts=30
attempt=1

while [ $attempt -le $max_attempts ]; do
    if curl -sf http://localhost:8030/api/health > /dev/null 2>&1; then
        print_success "FE is healthy!"
        break
    fi

    if [ $attempt -eq $max_attempts ]; then
        print_error "FE did not become healthy"
        echo "FE Logs:"
        docker logs starrocks-fe | tail -50
        exit 1
    fi

    echo -n "."
    sleep 2
    attempt=$((attempt + 1))
done

################################################################################
# Step 4: Wait for BE to be healthy
################################################################################

print_step "Step 4: Waiting for BE to be healthy"

attempt=1
while [ $attempt -le $max_attempts ]; do
    if curl -sf http://localhost:8040/api/health > /dev/null 2>&1; then
        print_success "BE is healthy!"
        break
    fi

    if [ $attempt -eq $max_attempts ]; then
        print_error "BE did not become healthy"
        echo "BE Logs:"
        docker logs starrocks-be | tail -50
        exit 1
    fi

    echo -n "."
    sleep 2
    attempt=$((attempt + 1))
done

################################################################################
# Step 5: Wait for initialization container to complete
################################################################################

print_step "Step 5: Checking database initialization"

echo "Waiting for init container to complete..."
sleep 5

if docker ps -a --filter "name=starrocks-init" --format "{{.Status}}" | grep -q "Exited (0)"; then
    print_success "Init container completed successfully"
    echo ""
    echo "Init container logs:"
    echo "----------------------------------------"
    docker logs starrocks-init
    echo "----------------------------------------"
else
    print_error "Init container failed or did not run"
    echo ""
    echo "Init container status:"
    docker ps -a --filter "name=starrocks-init"
    echo ""
    echo "Init container logs:"
    docker logs starrocks-init
    exit 1
fi

################################################################################
# Step 6: Verify database was created
################################################################################

print_step "Step 6: Verifying database creation"

echo "Checking for 'datawiz' database..."
DATABASES=$(docker exec starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -sN -e "SHOW DATABASES;" 2>&1)

if echo "$DATABASES" | grep -q "datawiz"; then
    print_success "Database 'datawiz' exists"
    echo "Available databases:"
    echo "$DATABASES" | sed 's/^/  - /'
else
    print_error "Database 'datawiz' NOT found"
    echo "Available databases:"
    echo "$DATABASES"
    exit 1
fi

################################################################################
# Step 7: Verify user was created
################################################################################

print_step "Step 7: Verifying user creation"

echo "Checking for 'datawiz_admin' user..."
USER_CHECK=$(docker exec starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -sN -e "SELECT user FROM mysql.user WHERE user='datawiz_admin';" 2>&1)

if [ "$USER_CHECK" = "datawiz_admin" ]; then
    print_success "User 'datawiz_admin' exists"
else
    print_error "User 'datawiz_admin' NOT found"
    echo "Existing users:"
    docker exec starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT user, host FROM mysql.user;"
    exit 1
fi

################################################################################
# Step 8: Verify user privileges
################################################################################

print_step "Step 8: Verifying user privileges"

echo "Checking privileges for 'datawiz_admin'..."
PRIVILEGES=$(docker exec starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -e "SHOW GRANTS FOR 'datawiz_admin'@'%';" 2>&1)

if echo "$PRIVILEGES" | grep -q "datawiz"; then
    print_success "User has privileges on 'datawiz' database"
    echo ""
    echo "Granted privileges:"
    echo "----------------------------------------"
    echo "$PRIVILEGES"
    echo "----------------------------------------"
else
    print_error "User does NOT have privileges on 'datawiz' database"
    echo "$PRIVILEGES"
    exit 1
fi

################################################################################
# Step 9: Test connection with datawiz_admin user
################################################################################

print_step "Step 9: Testing connection with datawiz_admin user"

echo "Attempting to connect as datawiz_admin..."
CONNECTION_TEST=$(docker exec starrocks-fe mysql -h 127.0.0.1 -P 9030 -u datawiz_admin -p'0jqhC3X541tP1RmR.5' -e "SELECT 'Connection successful!' AS status, DATABASE() AS current_db, VERSION() AS version;" 2>&1 | grep -v "Warning")

if echo "$CONNECTION_TEST" | grep -q "Connection successful"; then
    print_success "Successfully connected as datawiz_admin"
    echo ""
    echo "Connection test results:"
    echo "----------------------------------------"
    echo "$CONNECTION_TEST"
    echo "----------------------------------------"
else
    print_error "Failed to connect as datawiz_admin"
    echo "$CONNECTION_TEST"
    exit 1
fi

################################################################################
# Step 10: Test creating a table
################################################################################

print_step "Step 10: Testing table creation"

echo "Creating test table in datawiz database..."
TABLE_CREATE=$(docker exec starrocks-fe mysql -h 127.0.0.1 -P 9030 -u datawiz_admin -p'0jqhC3X541tP1RmR.5' datawiz -e "
CREATE TABLE IF NOT EXISTS test_table (
    id INT,
    name VARCHAR(100),
    created_at DATETIME
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1;" 2>&1 | grep -v "Warning")

if [ $? -eq 0 ]; then
    print_success "Successfully created test table"
else
    print_error "Failed to create test table"
    echo "$TABLE_CREATE"
    exit 1
fi

echo "Inserting test data..."
docker exec starrocks-fe mysql -h 127.0.0.1 -P 9030 -u datawiz_admin -p'0jqhC3X541tP1RmR.5' datawiz -e "
INSERT INTO test_table VALUES
(1, 'Test User 1', NOW()),
(2, 'Test User 2', NOW());" 2>&1 | grep -v "Warning"

echo "Querying test data..."
QUERY_RESULT=$(docker exec starrocks-fe mysql -h 127.0.0.1 -P 9030 -u datawiz_admin -p'0jqhC3X541tP1RmR.5' datawiz -e "SELECT * FROM test_table;" 2>&1 | grep -v "Warning")

if echo "$QUERY_RESULT" | grep -q "Test User"; then
    print_success "Successfully queried test data"
    echo ""
    echo "Test data:"
    echo "----------------------------------------"
    echo "$QUERY_RESULT"
    echo "----------------------------------------"
else
    print_error "Failed to query test data"
    exit 1
fi

################################################################################
# Step 11: Verify BE registration
################################################################################

print_step "Step 11: Verifying BE registration"

BE_STATUS=$(docker exec starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -e "SHOW BACKENDS\G" 2>&1 | grep "Alive:")

if echo "$BE_STATUS" | grep -q "Alive: true"; then
    print_success "Backend is registered and alive"
    echo ""
    docker exec starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -e "SHOW BACKENDS\G" 2>&1 | grep -E "BackendId:|IP:|Alive:|Version:"
else
    print_error "Backend is not alive"
    exit 1
fi

################################################################################
# Final Summary
################################################################################

print_step "🎉 ALL TESTS PASSED! 🎉"

echo ""
echo -e "${GREEN}Automated initialization is working perfectly!${NC}"
echo ""
echo "Summary of what was verified:"
echo "  ✓ Containers started successfully"
echo "  ✓ FE became healthy"
echo "  ✓ BE became healthy"
echo "  ✓ Init script executed successfully"
echo "  ✓ Database 'datawiz' was created"
echo "  ✓ User 'datawiz_admin' was created"
echo "  ✓ User has correct privileges"
echo "  ✓ User can connect successfully"
echo "  ✓ User can create tables and insert data"
echo "  ✓ Backend is registered and alive"
echo ""
echo "Your StarRocks cluster is ready for production use!"
echo ""
echo -e "${BLUE}Access Information:${NC}"
echo "  FE Web UI:  http://localhost:8030"
echo "  BE Web UI:  http://localhost:8040"
echo "  MySQL:      mysql -h 127.0.0.1 -P 9030 -u datawiz_admin -p"
echo "  Password:   0jqhC3X541tP1RmR.5"
echo "  Database:   datawiz"
echo ""
