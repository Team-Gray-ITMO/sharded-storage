#!/bin/bash

if [[ "$#" -ne 1 ]]; then
  echo "Использование: $0 <порт_для_9001>"
  exit 1
fi

PORT_FOR_9001=$1
PORT_FOR_9003=$2

IMAGE_NAME="sharded_storage_node"
CONTAINER_NAME="node_container_$1"
NETWORK_NAME="sharded_storage_network"

echo "Сборка образа..."
docker build -f node/Dockerfile -t ${IMAGE_NAME} .

echo "Создание сети..."
docker network inspect "${NETWORK_NAME}" > /dev/null || docker network create "${NETWORK_NAME}"

echo "Запуск контейнера..."
docker run -d \
           --name=${CONTAINER_NAME} \
           --network="${NETWORK_NAME}" \
           -p ${PORT_FOR_9001}:9001 \
           ${IMAGE_NAME}

echo "Контейнер запущен!"