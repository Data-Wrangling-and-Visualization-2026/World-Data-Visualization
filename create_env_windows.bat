@echo off
setlocal enabledelayedexpansion

REM Generates .env using the current directory as project root.
set "PROJECT_PATH=%CD%"
set "PROJECT_PATH=%PROJECT_PATH:\=/%"

(
echo # Auto-generated .env
echo.
echo # Project Path
echo PROJECT_PATH=%PROJECT_PATH%
echo.
echo # Data Paths
echo RAW_DATA_PATH=%PROJECT_PATH%/data_pipeline/raw_data/
echo OUTPUT_PATH=%PROJECT_PATH%/data_pipeline/parsed_data/
echo TEMP_PATH=%PROJECT_PATH%/data_pipeline/temp/
echo DATABASE_PATH=%PROJECT_PATH%/database/
echo.
echo # Local LLM Configuration
echo MODEL_NAME=qwen2.5vl:7b
echo MODEL_TEMPERATURE=0.1
echo MODEL_MAX_OUTPUT_TOKENS=8092

echo # PostgreSQL Database configuration
echo DB_HOST=localhost
echo DB_NAME=countries_stats
echo DB_USER=postgres
echo DB_PASSWORD=0
echo DB_PORT=5432
) > .env

echo .env has been generated at: %CD%\.env
endlocal
