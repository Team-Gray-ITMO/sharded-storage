@echo off
setlocal enabledelayedexpansion

if "%1"=="" (
    echo Error: No command specified
    call :print_usage
    exit /b 1
)

set COMMAND=%1

if "%COMMAND%"=="build" (
    echo Building images with docker-compose
    docker-compose build
) else if "%COMMAND%"=="start" (
    echo Starting master and node
    docker-compose up -d

    echo Master and node started successfully

    echo Starting client
    docker-compose run --rm client

    echo Client finished
) else if "%COMMAND%"=="stop" (
    echo Stopping all services
    docker-compose down
) else (
    echo Error: Unknown command %COMMAND%
    call :print_usage
    exit /b 1
)

exit /b 0

:print_usage
echo Usage: %0 {build^|start^|stop}
echo Start sharded storage components using Docker Compose
echo.
echo Commands:
echo   build    Build Docker images
echo   start    Start master, node and client
echo   stop     Stop all services
exit /b 0
