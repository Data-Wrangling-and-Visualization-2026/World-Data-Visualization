# Interactive 3D World Map Visualization

## Project Overview
We are constructing an interactive 3D chart of a world map with countries colored according to various statistical indicators from a dataset. Users can rotate the Earth sphere and click on a specific country to view detailed information and related plots.

## Team Roles
- **Zamir Safin**: Frontend Visualization (React, D3.js)
- **Denis Beliaev**: Backend Database (Node.js, PostgreSQL)
- **Rustem Gilmetdinov**: Data Pipelining & GenAI Agents (Python, LLMs)

## Technical Stack
- **Frontend**: React
- **Backend**: Node.js
- **Database**: PostgreSQL, SQLite
- **DevOps**: Docker

## Data Strategy
- **Structured Data**: Scrapped from worldometers.info
- **Unstructured Data**: Extracted from PDFs (Demographic Yearbooks)
- **Processing**: GenAI agents used for data extraction and cleaning

## Data Extraction Pipeline

### Tools Used:
1. **PyPDF** - Page pre-extraction from PDF reports
2. **Docling** - PDF to Markdown conversion
3. **Qwen2.5-VL** - Table extraction to CSV

### Process:
#### For unstructured data
1. Identify table pages using PDF inspector
2. Extract specific pages with **PyPDF** (reduces context by ~90%)
3. Convert extracted pages to **Markdown** with **Docling**
4. Send Markdown to **Qwen2.5-VL** for structured **CSV** extraction
5. Validate output with pandas
6. Linearly interpolate missing values

#### For structured data
1. Scrap the data from the website (worldometers.info) and upload to **CSV**
2. Clear the **CSV** by deleting redundant symbols and columns
3. Linearly interpolate missing values

#### Construction of Database
1. Merge the gained **CSV** tables by using INNER JOIN
2. Upload the gained `pd.DataFrame` into SQLite database file `.db`


### Justification:
- Page pre-extraction reduces processing time and increases the accuracy
- Qwen2.5-VL chosen for the ability of reading text in various scenarios (multi-orientation), interpreting tables, charts, diagrams

### How to launch the Data Pipeline Process
#### Linux/macOS
   ```bash
   # Clone the repository
   git clone <REPOSITORY_URL>
   cd "DWaV Project"

   # Create a user-specific `.env` file
   chmod +x create_env_unix.sh
   ./create_env_unix.sh

   # Create and activate a virtual environment
   python3 -m venv .venv
   source .venv/bin/activate

   # Install dependencies
   pip install -r requirements.txt

   # Launch the data pipeline
   python3 data_pipeline/src/main.py
   ```

#### Windows

   ```bat
   REM Clone the repository
   git clone <REPOSITORY_URL>
   cd "DWaV Project"

   REM Create a user-specific `.env` file
   create_env_windows.bat

   REM Create and activate a virtual environment
   python -m venv .venv
   .venv\Scripts\activate

   REM Install dependencies
   pip install -r requirements.txt

   REM Launch the data pipeline:
   python data_pipeline/src/main.py
   ```

>[!warn] Do not forget to edit `DB_USER` and `DB_PASSWORD` with your own values

## Docker

Below you can see the scripts for launching the application via Docker

#### Linux/macOS
```bash
cd $PROJECT_PATH/docker
docker build -t dwav-api .

docker run --rm -p 8000:8000 --env-file .env dwav-api
```

If `host.docker.internal` does not resolve, use:

```bash
docker run --rm -p 8000:8000 --add-host=host.docker.internal:host-gateway --env-file .env dwav-api
```

#### Windows
```bat
@echo off
setlocal EnableExtensions

REM Build and run the Node API from this folder (same as: docker build / docker run).
REM Requires Docker Desktop for Windows. Ensure docker\.env exists with DB_* and PORT.

cd /d "%~dp0"

if not exist ".env" (
    echo ERROR: docker\.env not found in this folder. Create it with DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT, PORT.
    exit /b 1
)

echo Building image dwav-api...
docker build -t dwav-api .
if errorlevel 1 exit /b 1

echo.
echo Starting container on http://localhost:8000  (Ctrl+C to stop)
echo.

docker run --rm -p 8000:8000 --env-file .env dwav-api
exit /b %ERRORLEVEL%
```