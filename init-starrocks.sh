#!/bin/bash

################################################################################
# StarRocks Production Initialization Script
#
# This script prepares the system for StarRocks deployment and initializes
# the cluster after container startup.
#
# Usage: ./init-starrocks.sh [prepare|start|register|status|all]
################################################################################

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
STARROCKS_DATA_DIR="/var/lib/starrocks"
FE_CONTAINER="starrocks-fe"
BE_CONTAINER="starrocks-be"
INIT_CONTAINER="starrocks-init"
FE_PORT=9030
FE_HTTP_PORT=8030
BE_HTTP_PORT=8040

################################################################################
# Helper Functions
################################################################################

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_section() {
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

check_command() {
    if ! command -v $1 &> /dev/null; then
        print_error "$1 is not installed. Please install it first."
        exit 1
    fi
}

wait_for_health() {
    local url=$1
    local service=$2
    local max_attempts=30
    local attempt=1

    print_info "Waiting for $service to be healthy..."

    while [ $attempt -le $max_attempts ]; do
        if curl -sf "$url" > /dev/null 2>&1; then
            print_success "$service is healthy!"
            return 0
        fi

        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done

    echo ""
    print_error "$service did not become healthy in time"
    return 1
}

################################################################################
# Step 1: Prepare System
################################################################################

prepare_system() {
    print_section "Step 1: Preparing System"

    # Check required commands
    print_info "Checking required commands..."
    check_command "docker"
    check_command "docker-compose"
    check_command "curl"

    # Check Docker daemon
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker daemon is not running"
        exit 1
    fi
    print_success "Docker daemon is running"

    # Create data directories
    print_info "Creating StarRocks data directories..."
    sudo mkdir -p "$STARROCKS_DATA_DIR"/{fe-meta,fe-log,be-storage,be-log}

    # Set permissions
    print_info "Setting directory permissions..."
    sudo chown -R $USER:$USER "$STARROCKS_DATA_DIR"
    chmod -R 755 "$STARROCKS_DATA_DIR"

    print_success "Data directories created: $STARROCKS_DATA_DIR"

    # Check system limits
    print_info "Checking system limits..."
    local nofile_limit=$(ulimit -n)
    local nproc_limit=$(ulimit -u)

    if [ "$nofile_limit" -lt 65535 ]; then
        print_warning "File descriptor limit is $nofile_limit (recommended: 65535)"
        print_info "To increase, run: ulimit -n 65535"
    else
        print_success "File descriptor limit is adequate: $nofile_limit"
    fi

    if [ "$nproc_limit" -lt 65535 ]; then
        print_warning "Process limit is $nproc_limit (recommended: 65535)"
        print_info "To increase, run: ulimit -u 65535"
    else
        print_success "Process limit is adequate: $nproc_limit"
    fi

    # Check available disk space
    print_info "Checking disk space..."
    local available_space=$(df -BG "$STARROCKS_DATA_DIR" | awk 'NR==2 {print $4}' | sed 's/G//')

    if [ "$available_space" -lt 100 ]; then
        print_warning "Available disk space is ${available_space}GB (recommended: 200GB+)"
    else
        print_success "Available disk space: ${available_space}GB"
    fi

    print_success "System preparation complete!"
}

################################################################################
# Step 2: Start Services
################################################################################

start_services() {
    print_section "Step 2: Starting StarRocks Services"

    # Check if compose file exists
    if [ ! -f "docker-compose.yml" ]; then
        print_error "docker-compose.yml not found in current directory"
        exit 1
    fi

    # Start services
    print_info "Starting Docker Compose services..."
    docker-compose up -d starrocks-fe starrocks-be starrocks-init

    print_info "Waiting for containers to initialize..."
    sleep 5

    # Check container status
    if ! docker ps | grep -q "$FE_CONTAINER"; then
        print_error "FE container is not running"
        docker-compose logs starrocks-fe | tail -20
        exit 1
    fi

    if ! docker ps | grep -q "$BE_CONTAINER"; then
        print_error "BE container is not running"
        docker-compose logs starrocks-be | tail -20
        exit 1
    fi

    print_success "Containers started successfully"

    # Wait for FE health
    wait_for_health "http://localhost:$FE_HTTP_PORT/api/health" "StarRocks FE" || {
        print_error "FE health check failed. Showing logs:"
        docker-compose logs --tail=50 starrocks-fe
        exit 1
    }

    # Wait for BE health
    wait_for_health "http://localhost:$BE_HTTP_PORT/api/health" "StarRocks BE" || {
        print_error "BE health check failed. Showing logs:"
        docker-compose logs --tail=50 starrocks-be
        exit 1
    }

    print_success "All services are healthy!"

    # Wait for database initialization
    print_info "Waiting for database initialization..."
    sleep 5

    if docker ps -a | grep -q "$INIT_CONTAINER"; then
        print_info "Checking database initialization status..."
        docker logs $INIT_CONTAINER

        if docker ps -a --filter "name=$INIT_CONTAINER" --filter "status=exited" --filter "exited=0" | grep -q "$INIT_CONTAINER"; then
            print_success "Database and user initialized successfully!"
        else
            print_warning "Database initialization may have issues. Check logs: docker logs $INIT_CONTAINER"
        fi
    fi
}

################################################################################
# Step 3: Register Backend
################################################################################

register_backend() {
    print_section "Step 3: Registering Backend with Frontend"

    # Check if FE is accessible
    if ! docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -e "SELECT 1" > /dev/null 2>&1; then
        print_error "Cannot connect to FE MySQL interface"
        exit 1
    fi

    print_success "Connected to FE MySQL interface"

    # Check if BE is already registered
    BE_COUNT=$(docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -sN -e "SHOW BACKENDS" 2>/dev/null | wc -l || echo "0")

    if [ "$BE_COUNT" -gt 0 ]; then
        print_warning "Backend already registered (count: $BE_COUNT)"
        print_info "Showing current backends:"
        docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -e "SHOW BACKENDS\G"
        return 0
    fi

    # Register the BE
    print_info "Registering BE node..."
    docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -e "ALTER SYSTEM ADD BACKEND 'starrocks-be:9050';"

    # Wait a moment for registration
    sleep 3

    # Verify registration
    BE_COUNT=$(docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -sN -e "SHOW BACKENDS" 2>/dev/null | wc -l || echo "0")

    if [ "$BE_COUNT" -gt 0 ]; then
        print_success "Backend registered successfully!"
        print_info "Backend details:"
        docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -e "SHOW BACKENDS\G"
    else
        print_error "Backend registration failed"
        exit 1
    fi
}

################################################################################
# Step 4: Check Status
################################################################################

check_status() {
    print_section "Cluster Status"

    # Container status
    print_info "Container Status:"
    docker-compose ps starrocks-fe starrocks-be
    echo ""

    # Health endpoints
    print_info "Health Check Results:"

    if curl -sf "http://localhost:$FE_HTTP_PORT/api/health" > /dev/null 2>&1; then
        print_success "FE Health: OK"
    else
        print_error "FE Health: FAILED"
    fi

    if curl -sf "http://localhost:$BE_HTTP_PORT/api/health" > /dev/null 2>&1; then
        print_success "BE Health: OK"
    else
        print_error "BE Health: FAILED"
    fi
    echo ""

    # Database status
    print_info "Frontend Nodes:"
    docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -e "SHOW FRONTENDS\G" 2>/dev/null || print_error "Cannot query FE status"
    echo ""

    print_info "Backend Nodes:"
    docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -e "SHOW BACKENDS\G" 2>/dev/null || print_error "Cannot query BE status"
    echo ""

    # Database and User Status
    print_info "Database Status:"
    docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -e "SHOW DATABASES" 2>/dev/null || print_error "Cannot query databases"
    echo ""

    print_info "Users:"
    docker exec $FE_CONTAINER mysql -h 127.0.0.1 -P $FE_PORT -u root -e "SELECT user, host FROM mysql.user WHERE user IN ('root', 'datawiz_admin')" 2>/dev/null || print_error "Cannot query users"
    echo ""

    # Resource usage
    print_info "Resource Usage:"
    docker stats --no-stream $FE_CONTAINER $BE_CONTAINER
    echo ""

    # Access information
    print_section "Access Information"
    echo -e "${GREEN}StarRocks FE Web UI:${NC}  http://localhost:8030"
    echo -e "${GREEN}MySQL (root):${NC}        mysql -h 127.0.0.1 -P 9030 -u root"
    echo -e "${GREEN}MySQL (datawiz_admin):${NC} mysql -h 127.0.0.1 -P 9030 -u datawiz_admin -p"
    echo -e "${GREEN}StarRocks BE Web UI:${NC}  http://localhost:8040"
    echo -e "${GREEN}Database:${NC}            datawiz"
    echo ""
}

################################################################################
# Main Script
################################################################################

show_usage() {
    echo "Usage: $0 [prepare|start|register|status|all]"
    echo ""
    echo "Commands:"
    echo "  prepare   - Prepare system (create directories, check requirements)"
    echo "  start     - Start StarRocks containers"
    echo "  register  - Register BE with FE"
    echo "  status    - Show cluster status"
    echo "  all       - Run all steps (prepare, start, register, status)"
    echo ""
}

main() {
    local command=${1:-all}

    case $command in
        prepare)
            prepare_system
            ;;
        start)
            start_services
            ;;
        register)
            register_backend
            ;;
        status)
            check_status
            ;;
        all)
            prepare_system
            start_services
            register_backend
            check_status

            print_section "Initialization Complete!"
            print_success "StarRocks cluster is ready for use"
            echo ""
            echo -e "${GREEN}Next steps:${NC}"
            echo "1. Connect to StarRocks: mysql -h 127.0.0.1 -P 9030 -u root"
            echo "2. Create your database: CREATE DATABASE pidilite;"
            echo "3. Review STARROCKS_SETUP.md for detailed configuration"
            echo "4. Start your ETL application: docker-compose up -d pidilite-datawiz"
            ;;
        *)
            show_usage
            exit 1
            ;;
    esac
}

# Run main function
main "$@"
