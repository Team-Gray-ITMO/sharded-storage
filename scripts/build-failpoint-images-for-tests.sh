#!/bin/bash
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Сборка failpoint-образа discovery-service"
docker build -t sharded_storage_discovery-fp:latest -f Dockerfile.failpoint --target discovery-runtime .

echo "Сборка failpoint-образа master-service"
docker build -t sharded_storage_master-fp:latest -f Dockerfile.failpoint --target master-runtime .