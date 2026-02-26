#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
print_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Detect OS
detect_os() {
    case "$(uname -s)" in
        Darwin*) OS="mac" ;;
        Linux*)  OS="linux" ;;
        MINGW*|MSYS*|CYGWIN*) OS="windows" ;;
        *) OS="unknown" ;;
    esac
    print_info "Detected OS: $OS"
}

# Check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Install Docker if not present
install_docker() {
    if command_exists docker; then
        print_info "Docker is already installed"
        return 0
    fi

    print_warn "Docker not found, attempting to install..."

    case "$OS" in
        mac)
            if command_exists brew; then
                print_info "Installing Docker via Homebrew..."
                brew install --cask docker
                print_warn "Please start Docker Desktop manually, then re-run this script"
                exit 1
            else
                print_error "Please install Docker Desktop from https://www.docker.com/products/docker-desktop"
                exit 1
            fi
            ;;
        linux)
            print_info "Installing Docker via official script..."
            curl -fsSL https://get.docker.com | sh
            sudo systemctl start docker
            sudo usermod -aG docker "$USER"
            print_warn "You may need to log out and back in for group changes to take effect"
            ;;
        windows)
            print_error "Please install Docker Desktop from https://www.docker.com/products/docker-desktop"
            print_error "Then re-run this script in Git Bash or WSL"
            exit 1
            ;;
        *)
            print_error "Unsupported OS. Please install Docker manually."
            exit 1
            ;;
    esac
}

# Check if Docker is running
check_docker_running() {
    if ! docker info >/dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker Desktop and try again."
        exit 1
    fi
    print_info "Docker is running"
}

# Install docker-compose if not present
install_docker_compose() {
    if command_exists docker-compose || docker compose version >/dev/null 2>&1; then
        print_info "Docker Compose is available"
        return 0
    fi

    print_warn "Docker Compose not found, attempting to install..."

    case "$OS" in
        mac)
            print_info "Docker Compose should be included with Docker Desktop"
            ;;
        linux)
            print_info "Installing Docker Compose..."
            sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
            sudo chmod +x /usr/local/bin/docker-compose
            ;;
        *)
            print_error "Please install Docker Compose manually"
            exit 1
            ;;
    esac
}

# Run docker-compose command (compatible with v1 and v2)
docker_compose_cmd() {
    if docker compose version >/dev/null 2>&1; then
        docker compose "$@"
    else
        docker-compose "$@"
    fi
}

# Wait for service to be ready
wait_for_service() {
    local service=$1
    local max_attempts=${2:-30}
    local attempt=1

    print_info "Waiting for $service to be ready..."
    while [ $attempt -le $max_attempts ]; do
        if docker_compose_cmd ps | grep -q "$service.*Up\|$service.*running"; then
            print_info "$service is ready"
            return 0
        fi
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    print_error "$service failed to start"
    return 1
}

# Wait for Kafka to be ready
wait_for_kafka() {
    local max_attempts=30
    local attempt=1

    print_info "Waiting for Kafka to be ready..."
    while [ $attempt -le $max_attempts ]; do
        # Try to list topics - if it works, Kafka is ready
        if docker_compose_cmd exec -T kafka /bin/kafka-topics --list --bootstrap-server localhost:9093 >/dev/null 2>&1; then
            print_info "Kafka is ready"
            return 0
        fi
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    print_error "Kafka failed to become ready"
    return 1
}

# Main test function
run_tests() {
    local topic="test-$(date +%s)"
    local test_message="Hello from Kafka Relay at $(date)"

    print_info "Creating test topic: $topic"
    docker_compose_cmd exec -T kafka /bin/kafka-topics \
        --create --topic "$topic" \
        --bootstrap-server localhost:9093 \
        --partitions 1 \
        --replication-factor 1 2>/dev/null || true

    sleep 2

    print_info "Sending test message through proxy..."
    echo "$test_message" | docker_compose_cmd exec -T kafka /bin/kafka-console-producer \
        --broker-list kafka-relay:9092 \
        --topic "$topic" 2>/dev/null

    sleep 2

    print_info "Consuming message through proxy..."
    local received
    received=$(docker_compose_cmd exec -T kafka /bin/kafka-console-consumer \
        --bootstrap-server kafka-relay:9092 \
        --topic "$topic" \
        --from-beginning \
        --timeout-ms 10000 2>/dev/null | head -1)

    if [ "$received" = "$test_message" ]; then
        print_info "✅ Test PASSED! Message sent and received successfully"
        echo ""
        echo "  Sent:     $test_message"
        echo "  Received: $received"
    else
        print_warn "⚠️  Message received but may differ"
        echo "  Sent:     $test_message"
        echo "  Received: $received"
    fi

    echo ""
    print_info "Proxy logs (last 20 lines):"
    echo "----------------------------------------"
    docker_compose_cmd logs --tail=20 kafka-relay
    echo "----------------------------------------"
}

# Cleanup function
cleanup() {
    print_info "Stopping services..."
    docker_compose_cmd down
}

# Main
main() {
    echo "========================================"
    echo "  Kafka Relay Test Script"
    echo "========================================"
    echo ""

    detect_os
    install_docker
    check_docker_running
    install_docker_compose

    echo ""
    print_info "Starting services with docker-compose..."
    docker_compose_cmd up --build -d

    echo ""
    wait_for_service "zookeeper" 30
    wait_for_service "kafka" 30
    wait_for_service "kafka-relay" 30

    sleep 5
    wait_for_kafka

    echo ""
    run_tests

    echo ""
    print_info "Services are still running. Use 'docker-compose down' to stop."
    print_info "Or run: $0 stop"
}

# Handle arguments
case "${1:-}" in
    stop|down)
        cleanup
        ;;
    logs)
        docker_compose_cmd logs -f kafka-relay
        ;;
    *)
        main
        ;;
esac
