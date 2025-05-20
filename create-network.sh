#!/bin/bash

NETWORK_NAME="sharded-storage"

echo "Поиск сети '$NETWORK_NAME'..."
if docker network inspect "${NETWORK_NAME}" > /dev/null 2>&1; then
  echo "Сеть найдена: ${NETWORK_NAME}"
else
  echo "Сеть не найдена. Создаём..."
  docker network create \
    --driver bridge \
    --label com.docker.compose.network="${NETWORK_NAME}" \
    --label com.docker.compose.project="${NETWORK_NAME}" \
    "${NETWORK_NAME}"
  echo "Сеть создана: ${NETWORK_NAME}"
fi
