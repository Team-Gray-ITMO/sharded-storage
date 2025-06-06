#!/bin/bash

echo "Stopping all Nodes"

docker ps -q --filter "name=node-containter-" | while read -r container_id; do
  docker stop "$container_id"
  docker rm "$container_id"
done

echo "Stopping master and discovery"
docker-compose down -v
