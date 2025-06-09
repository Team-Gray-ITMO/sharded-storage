#!/bin/bash
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ -z "$1" ]; then
  echo "Usage: $0 <node-id>"
  exit 1
fi

CONTAINER_NAME="node-container-$1"

echo "Stopping node $1..."
docker stop "$CONTAINER_NAME" 2>/dev/null && \
  docker rm "$CONTAINER_NAME" 2>/dev/null

if [ $? -eq 0 ]; then
  echo "Successfully stopped node $1"
else
  echo "Node $1 not found or already stopped"
fi
