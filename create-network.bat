@echo off
setlocal
set "NETWORK_NAME=sharded-storage"

echo Checking if network "%NETWORK_NAME%" exists...

docker network inspect "%NETWORK_NAME%" >nul 2>&1
if not errorlevel 1 (
    echo Network already exists: %NETWORK_NAME%
) else (
    echo Network not found. Creating...
    docker network create ^
        --driver bridge ^
        --label com.docker.compose.network=%NETWORK_NAME% ^
        --label com.docker.compose.project=%NETWORK_NAME% ^
        %NETWORK_NAME%
    if errorlevel 1 (
        echo Failed to create network.
        exit /b 1
    )
    echo Network created: %NETWORK_NAME%
)

endlocal
