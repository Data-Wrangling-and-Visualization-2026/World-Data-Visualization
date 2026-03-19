#!/usr/bin/env bash
set -euo pipefail

# Generates .env using the current directory as project root.
PROJECT_PATH="$(pwd)"

cat > .env <<EOF
# Auto-generated .env

# Project Path
PROJECT_PATH=${PROJECT_PATH}

# Data Paths
RAW_DATA_PATH=${PROJECT_PATH}/data_pipeline/raw_data/
OUTPUT_PATH=${PROJECT_PATH}/data_pipeline/parsed_data/
TEMP_PATH=${PROJECT_PATH}/data_pipeline/temp/
DATABASE_PATH=${PROJECT_PATH}/database/

# Local LLM Configuration
MODEL_NAME=qwen2.5vl:7b
MODEL_TEMPERATURE=0.1
MODEL_MAX_OUTPUT_TOKENS=8092
EOF

echo ".env has been generated at: ${PROJECT_PATH}/.env"
