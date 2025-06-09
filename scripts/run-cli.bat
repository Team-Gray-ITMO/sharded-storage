@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0.."

:: Check if Java is installed
where java >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo Error: Java is not installed. Please install Java 21 or later.
    exit /b 1
)

echo Building the project...
call gradlew.bat :client:shadowJar

echo Starting CLI application...
java -jar client\build\libs\client-all.jar %*
