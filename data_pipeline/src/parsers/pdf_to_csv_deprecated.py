"""
PDF to CSV Extraction Pipeline
Uses Docling for PDF -> Markdown conversion and Qwen 2.5-VL for table extraction
"""

import os
import requests
import pandas as pd
from pathlib import Path
from typing import List, Optional
from dotenv import load_dotenv
from docling.document_converter import DocumentConverter

# Load environment variables
load_dotenv()


class PDFToCSVExtractor:
    def __init__(
            self,
            llm_base_url: str = "http://localhost:11434",
            llm_model: str = "qwen2.5-vl",
            output_dir: str = "data_pipeline/output"
    ):
        """
        Initialize the PDF to CSV extractor.

        Args:
            llm_base_url: Base URL for Ollama API (default: local)
            llm_model: Model name for Qwen 2.5-VL
            output_dir: Directory to save CSV files
        """
        self.llm_base_url = llm_base_url
        self.llm_model = llm_model
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.converter = DocumentConverter()

    def convert_pdf_to_markdown(
            self,
            pdf_path: str,
            pages: Optional[List[int]] = None
    ) -> str:
        """
        Convert PDF to Markdown using Docling.

        Args:
            pdf_path: Path to the PDF file
            pages: List of page numbers to extract (0-indexed). None = all pages

        Returns:
            Markdown string representation of the PDF content
        """
        pdf_path = Path(pdf_path)

        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")

        print(f"📄 Converting PDF to Markdown: {pdf_path.name}")

        try:
            # Convert PDF to Docling document
            result = self.converter.convert(str(pdf_path))

            # Export to Markdown
            # If specific pages requested, we need to filter (Docling limitation)
            # Note: Docling processes all pages, so page filtering requires post-processing
            markdown_content = result.document.export_to_markdown()

            print(f"✅ Markdown conversion complete ({len(markdown_content)} chars)")
            return markdown_content

        except Exception as e:
            print(f"❌ Error converting PDF: {e}")
            raise

    def extract_table_with_llm(
            self,
            markdown_content: str,
            table_description: str = ""
    ) -> str:
        """
        Send Markdown to Qwen 2.5-VL and extract table as CSV.

        Args:
            markdown_content: Markdown content from PDF
            table_description: Description of what table to extract

        Returns:
            CSV formatted string
        """
        # Construct the prompt for table extraction
        prompt = f"""You are a data extraction assistant. Extract the table from the following Markdown content and convert it to CSV format.

Instructions:
1. Identify the main data table in the content
2. Extract all rows and columns accurately
3. Preserve numeric values exactly (no rounding)
4. Include headers as the first row
5. Identify and include row indices as the first column
6. Use comma as delimiter
7. Fill missing values as 'nan'
8. Do not include any explanatory text, only CSV data

{f"Additional context: {table_description}" if table_description else ""}

Output ONLY the CSV data, no markdown code blocks, no explanations:"""

        # Prepare API request for Ollama
        url = f"{self.llm_base_url}/api/generate"
        payload = {
            "model": self.llm_model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0.1,  # Low temperature for accurate extraction
                "num_predict": 4096  # Max tokens for response
            }
        }

        print(f"🤖 Sending request to Qwen 2.5-VL...")

        try:
            response = requests.post(url, json=payload, timeout=300)
            response.raise_for_status()

            result = response.json()
            csv_content = result.get("response", "").strip()

            # Clean up response (remove markdown code blocks if present)
            csv_content = self._clean_csv_response(csv_content)

            print(f"✅ Table extracted ({len(csv_content.split(chr(10)))} rows)")
            return csv_content

        except requests.exceptions.ConnectionError:
            print("❌ Cannot connect to Ollama. Ensure it's running: ollama serve")
            raise
        except Exception as e:
            print(f"❌ LLM extraction failed: {e}")
            raise

    def _clean_csv_response(self, csv_content: str) -> str:
        """Remove markdown code blocks and clean CSV output."""
        # Remove markdown code blocks
        if csv_content.startswith("```csv"):
            csv_content = csv_content[6:]
        elif csv_content.startswith("```"):
            csv_content = csv_content[3:]

        if csv_content.endswith("```"):
            csv_content = csv_content[:-3]

        return csv_content.strip()

    def save_csv(self, csv_content: str, filename: str) -> Path:
        """
        Save CSV content to file.

        Args:
            csv_content: CSV formatted string
            filename: Output filename (without extension)

        Returns:
            Path to saved file
        """
        output_path = self.output_dir / f"{filename}.csv"

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(csv_content)

        print(f"💾 CSV saved to: {output_path}")
        return output_path

    def validate_csv(self, csv_path: Path) -> bool:
        """
        Validate the extracted CSV file.

        Args:
            csv_path: Path to CSV file

        Returns:
            True if valid, False otherwise
        """
        try:
            df = pd.read_csv(csv_path)
            print(f"✅ CSV Validation: {df.shape[0]} rows, {df.shape[1]} columns")
            print(f"   Columns: {list(df.columns)}")
            return True
        except Exception as e:
            print(f"❌ CSV Validation failed: {e}")
            return False

    def process_pdf(
            self,
            pdf_path: str,
            output_filename: str,
            pages: Optional[List[int]] = None,
            table_description: str = ""
    ) -> Path:
        """
        Complete pipeline: PDF → Markdown → CSV → File

        Args:
            pdf_path: Path to input PDF
            output_filename: Name for output CSV (without extension)
            pages: Specific pages to process (optional)
            table_description: Description of table to extract

        Returns:
            Path to saved CSV file
        """
        print(f"\n{'=' * 60}")
        print(f"🚀 Starting PDF to CSV Extraction Pipeline")
        print(f"{'=' * 60}\n")

        # Step 1: Convert PDF to Markdown
        markdown_content = self.convert_pdf_to_markdown(pdf_path, pages)

        # Step 2: Extract table with LLM
        csv_content = self.extract_table_with_llm(markdown_content, table_description)

        # Step 3: Save CSV
        csv_path = self.save_csv(csv_content, output_filename)

        # Step 4: Validate
        self.validate_csv(csv_path)

        print(f"\n{'=' * 60}")
        print(f"✅ Pipeline Complete!")
        print(f"{'=' * 60}\n")

        return csv_path


# Main execution
if __name__ == "__main__":
    # Initialize extractor
    extractor = PDFToCSVExtractor(
        llm_base_url=os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
        llm_model=os.getenv("LLM_MODEL", "qwen2.5-vl"),
        output_dir="data_pipeline/output"
    )

    # Example usage for Energy Statistics PDF
    csv_path = extractor.process_pdf(
        pdf_path="data_pipeline/raw_data/energy_statistics_2024.pdf",
        output_filename="energy_statistics_cleaned",
        pages=[0, 1, 2],  # Specific pages (optional)
        table_description="Extract oil, gas, and coal production statistics by country and year"
    )

    print(f"📊 Output file: {csv_path}")

    # print(Path(r"/Users/acegiqmo/Desktop/PL/Python/DWaV Project/data_pipeline/raw_data/demographic_yearbooks/1950_1959_DYB.pdf").exists())
    # print(Path(r"/Users/acegiqmo/Desktop/PL/Python/DWaV Project/data_pipeline/src/parsers/pdf_to_csv_deprecated.py").exists())