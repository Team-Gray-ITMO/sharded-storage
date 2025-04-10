#!/bin/bash

# Exit on error
set -e

# Check if Java is installed
if ! command -v java &> /dev/null; then
    echo "Error: Java is not installed. Please install Java 21 or later."
    exit 1
fi

# Build the project
echo "Building the project..."
./gradlew :client:shadowJar

# Run the CLI application
echo "Starting CLI application..."
java -jar client/build/libs/client-all.jar "$@"