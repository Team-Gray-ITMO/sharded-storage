@echo off
setlocal

if "%~1"=="" (
  echo Usage: %~nx0 ^<count^>
  exit /b 1
)

for /L %%i in (1,1,%1) do (
  call run-node.bat %%i
)

endlocal
