#!/bin/bash
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

set -e

function print_usage {
    echo "Usage: $0 {build|start|stop}"
    echo "Start sharded storage components using Docker Compose"
    echo
    echo "Commands:"
    echo "  build    Build Docker images"
    echo "  start    Start master, node and client"
    echo "  stop     Stop all services"
}

# parse arguments
if [ $# -eq 0 ]; then
    echo "Error: No command specified"
    print_usage
    exit 1
fi

COMMAND=$1

case $COMMAND in
    build)
        echo "Building images with docker-compose"
        docker-compose build
        ;;
    start)
        echo "Starting master and node"
        docker-compose up -d
        
        echo "Master and node started successfully"
        
        echo "Starting client"
        docker-compose run --rm client
        
        echo "Client finished"
        ;;
    stop)
        echo "Stopping all services"
        docker-compose down
        ;;
    *)
        echo "Error: Unknown command $COMMAND"
        print_usage
        exit 1
        ;;
esac
