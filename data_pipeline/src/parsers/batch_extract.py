#!/usr/bin/env python3
"""
Batch PDF Processing for Multiple Data Sources
Handles Energy Inst, WorldBank, and UN PDFs
"""

from pathlib import Path
from pdf_to_csv import PDFToCSVExtractor

import json
import os

from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def batch_process_pdfs():
    """Process all PDFs from different sources with page pre-extraction."""
    # Check API Key
    if not os.getenv("GOOGLE_API_KEY"):
        print("❌ GOOGLE_API_KEY not set in environment.")
        print("💡 Please set GOOGLE_API_KEY in data_pipeline/.env")
        return

    extractor = PDFToCSVExtractor(cleanup_temp=True)

    # Define PDF sources from project proposal
    # Adjust page numbers based on actual PDF content
    pdf_configs = [
        {
            "pdf_path": "data_pipeline/raw_data/energy_inst.pdf",
            "output_filename": "energy_statistics",
            "pages": [0, 1, 2, 3],
            "table_description": "Oil, Gas, Coal production by country and year"
        },
        {
            "pdf_path": "data_pipeline/raw_data/worldbank_water.pdf",
            "output_filename": "water_resources",
            "pages": [0, 1, 2],
            "table_description": "Renewable water resources statistics by country"
        },
        {
            "path": "data_pipeline/raw_data/un_demographics.pdf",
            "output_filename": "demographics",
            "pages": [0, 1, 2, 3, 4],
            "table_description": "Death, Birth, Sex Ratio statistics by country"
        }
    ]

    birth_rate_1950s = {
        "pdf_path": f"{os.getenv('RAW_DATA_PATH')}/demographic_yearbooks/1950_1959_DYB.pdf",
        "output_filename": "birth1950",
        "pages": [214 + i for i in range(0, 16, 2)],
        "table_description": "Birth statsitics by country as indices and years 1949-1958 as headers. The information"
                             "about the continent must be ignored"
    }

    # Process PDFs
    results = extractor.process_pdf(
        pdf_path=birth_rate_1950s["pdf_path"],
        output_filename=birth_rate_1950s["output_filename"],
        pages=birth_rate_1950s["pages"],
        table_description=birth_rate_1950s["table_description"]
    )

    # Save results summary
    summary_path = Path(f"{os.getenv('PROJECT_PATH')}/data_pipeline/processing_summary.json")
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n📄 Processing summary saved to: {summary_path}")

    return results


if __name__ == "__main__":
    batch_process_pdfs()