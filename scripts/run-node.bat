@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0.."

if "%1"=="" (
    echo Usage: %0 ^<server_id^>
    exit /b 1
)

set IMAGE_NAME=sharded_storage_node:latest
set CONTAINER_NAME=node-containter-%1
set NETWORK_NAME=sharded-storage

echo Building image...
docker buildx bake node

echo Creating network...
call .\scripts\create-network.bat

echo Starting container...
docker run -d ^
           --name=%CONTAINER_NAME% ^
           --network="%NETWORK_NAME%" ^
           -e SERVICE_CONTAINER_NAME=node-containter-%1 ^
           -e SERVICE_ID=%1 ^
           -e DISCOVERY_GRPC_HOST=discovery ^
           -p 90%11:9001 ^
           %IMAGE_NAME%

echo Container started. ID: %1
