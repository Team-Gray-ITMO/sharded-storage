@echo off
setlocal

echo Stopping all Nodes

for /f "tokens=*" %%i in ('docker ps -q --filter "name=node-containter-"') do (
    docker stop %%i
    docker rm %%i
)

echo Stopping master and discovery
docker-compose down -v

endlocal
