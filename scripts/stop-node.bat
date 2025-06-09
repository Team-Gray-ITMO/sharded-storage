@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0.."

if "%1"=="" (
    echo Usage: %0 ^<server_id^>
    exit /b 1
)

set CONTAINER_NAME=node-containter-%1

if "%1"=="" (
    echo Usage: %0 ^<node-id^>
    exit /b 1
)

set CONTAINER_NAME=node-container-%1
set TIMEOUT_SECONDS=10

echo Stopping node %1...
docker stop !CONTAINER_NAME! >nul 2>&1

if !errorlevel! equ 0 (
    docker rm !CONTAINER_NAME! >nul 2>&1
    echo Successfully stopped node %1
) else (
    echo Node %1 not found or already stopped
)

endlocal
