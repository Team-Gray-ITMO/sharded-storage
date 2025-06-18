#!/bin/bash
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ "$#" -lt 1 ]]; then
  echo "Usage: $0 <server_id> [failpoint]"
  exit 1
fi

SERVER_ID="$1"
FAILPOINT_SUFFIX=""
if [[ "$2" == "failpoint" ]]; then
  FAILPOINT_SUFFIX="-fp"
fi

IMAGE_NAME="sharded_storage_node${FAILPOINT_SUFFIX}:latest"
CONTAINER_NAME="node-containter-${SERVER_ID}"
NETWORK_NAME="sharded-storage"

echo "Building image..."
docker buildx bake node${FAILPOINT_SUFFIX}

echo "Creating network..."
./scripts/create-network.sh

echo "Starting container..."
docker run -d \
           --name=${CONTAINER_NAME} \
           --network="${NETWORK_NAME}" \
           -e SERVICE_CONTAINER_NAME=${CONTAINER_NAME} \
           -e SERVICE_ID=${SERVER_ID} \
           -e DISCOVERY_GRPC_HOST=discovery \
           -p 90"${SERVER_ID}"1:9001 \
           ${IMAGE_NAME}

echo "Container started! ID: ${SERVER_ID}"
