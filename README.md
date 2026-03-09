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
- **Unstructured Data**: Extracted from PDFs (Energy Inst, WorldBank, UN)
- **Processing**: GenAI agents used for cleaning and microstate filtering.

## Data Extraction Pipeline

### Tools Used:
1. **PyPDF** - Page pre-extraction from PDF reports
2. **Docling** - PDF to Markdown conversion
3. **Google Gemini-1.5-Flash** - Table extraction to CSV

### Process:
1. Identify table pages using PDF inspector
2. Extract specific pages with PyPDF (reduces context by ~90%)
3. Convert extracted pages to Markdown with Docling
4. Send Markdown to Gemini for structured CSV extraction
5. Validate output with pandas

### Justification:
- Page pre-extraction reduces API costs and processing time
- Gemini-1.5-Flash chosen for large context window and speed
- Multiple LLMs available as fallback (Claude, DeepSeek) per project proposal