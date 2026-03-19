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
- **Database**: SQLite
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
