#!/bin/bash

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

function build_project {
    echo "Building project with Gradle"

    # build project
    ./gradlew shadowJar

    # check if JAR files were generated
    if [ ! -f "master/build/libs/master-all.jar" ]; then
        echo "JAR for master not found"
        exit 1
    fi

    if [ ! -f "node/build/libs/node-all.jar" ]; then
        echo "JAR for node not found"
        exit 1
    fi

    if [ ! -f "client/build/libs/client-all.jar" ]; then
        echo "JAR for client not found"
        exit 1
    fi
}

case $COMMAND in
    build)
        echo "Building project"
        build_project

        # build images
        echo "Building images with docker-compose"
        docker-compose build
        ;;
    start)
        # check JARs
        if [ ! -d "client/build/libs" ] || [ ! -d "master/build/libs" ] || [ ! -d "node/build/libs" ]; then
            echo "Build directories are missing. Building project first"
            build_project
        fi

        echo "Starting master and node"
        
        # start master & nodes
        docker-compose up -d
        
        echo "Master and node started successfully"
        
        # start client
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