@echo off
setlocal enabledelayedexpansion

if "%1"=="" (
    echo Usage: %0 ^<port_for_9001^>
    exit /b 1
)

set PORT_FOR_9001=%1
set IMAGE_NAME=sharded_storage_node
set CONTAINER_NAME=node-containter-%1
set NETWORK_NAME=sharded_storage_network

echo Building image...
docker build -f node/Dockerfile -t %IMAGE_NAME% .

echo Creating network...
docker network inspect "%NETWORK_NAME%" > nul 2>&1 || docker network create "%NETWORK_NAME%"

echo Starting container...
docker run -d ^
           --name=%CONTAINER_NAME% ^
           --network="%NETWORK_NAME%" ^
           -p %PORT_FOR_9001%:9001 ^
           %IMAGE_NAME%

echo Container started!
