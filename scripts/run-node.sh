#!/bin/bash
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ "$#" -ne 1 ]]; then
  echo "Использование: $0 <ID_сервера>"
  exit 1
fi

IMAGE_NAME="sharded_storage_node"
CONTAINER_NAME="node-containter-$1"
NETWORK_NAME="sharded-storage"

echo "Сборка образа..."
docker compose bake node

echo "Создание сети..."
./scripts/create-network.sh

echo "Запуск контейнера..."
docker run -d \
           --name=${CONTAINER_NAME} \
           --network="${NETWORK_NAME}" \
           -e SERVICE_CONTAINER_NAME=node-containter-$1 \
           -e SERVICE_ID=$1 \
           -e DISCOVERY_GRPC_HOST=discovery \
           -p 90"$1"1:9001 \
           ${IMAGE_NAME}

echo "Контейнер запущен! ID: $1"
