@echo off
setlocal
cd /d "%~dp0.."

if "%~1"=="" (
  echo Usage: %~nx0 ^<count^>
  exit /b 1
)

for /L %%i in (1,1,%1) do (
  call .\scripts\run-node.bat %%i
)

endlocal
