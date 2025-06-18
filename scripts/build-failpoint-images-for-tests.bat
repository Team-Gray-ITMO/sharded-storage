@echo off
REM Build failpoint Docker images for integration tests

echo Сборка failpoint-образа discovery-service ...
docker build -t sharded_storage_discovery-fp:latest -f Dockerfile.failpoint --target discovery-runtime .
if errorlevel 1 goto :error

echo Сборка failpoint-образа master-service ...
docker build -t sharded_storage_master-fp:latest -f Dockerfile.failpoint --target master-runtime .
if errorlevel 1 goto :error

echo Сборка failpoint-образа node-service ...
docker build -t sharded_storage_node-fp:latest -f Dockerfile.failpoint --target node-runtime .
if errorlevel 1 goto :error

echo Все образы собраны успешно
goto :eof

:error
echo.
echo Ошибка на этапе сборки одного из образов.
exit /b 1 