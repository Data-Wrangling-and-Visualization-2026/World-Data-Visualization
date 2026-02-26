"""
PDF to CSV Extraction Pipeline
Uses Docling for PDF -> Markdown conversion and Google Gemini-1.5-Flash for table extraction
"""

import os
import time
import requests
import pandas as pd
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from docling.document_converter import DocumentConverter
from pdf_page_extractor import PDFPageExtractor


class PDFToCSVExtractor:
    def __init__(
            self,
            llm_base_url: str = "http://localhost:11434",
            llm_model: str = "qwen2.5vl:3b",
            output_dir: str = os.getenv("OUTPUT_PATH"),
            temp_dir: str = os.getenv("TEMP_PATH"),
            cleanup_temp: bool = True
    ):
        """
        Initialize the PDF to CSV extractor with Gemini API.

        Args:
            api_key: Google API Key (defaults to env var)
            model_name: Gemini model version
            output_dir: Directory to save CSV files
            temp_dir: Directory for temporary extracted PDFs
            cleanup_temp: Whether to clean up temp files after processing
        """

        self.llm_model = llm_model
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.cleanup_temp_flag = cleanup_temp

        self.page_extractor = PDFPageExtractor(temp_dir=temp_dir)  # Initialize PyPDF extractor

        # Configure Gemini
        self.llm_base_url = llm_base_url

        # Configure Docling converter
        self.converter = DocumentConverter()

    def extract_pages_from_pdf(
            self,
            pdf_path: str,
            pages: Optional[List[int]] = None
    ) -> str:
        """
        Extract specific pages from PDF using PyPDF.

        Args:
            pdf_path: Path to the PDF file
            pages: List of page numbers to extract (0-indexed). None = all pages

        Returns:
            Path to the extracted PDF file
        """
        pdf_path = Path(pdf_path)

        if pages is None:
            # No extraction needed, use original file
            print(f"ℹ️  No page filtering - using full PDF: {pdf_path.name}")
            return str(pdf_path)

        # Extract specified pages
        extracted_path = self.page_extractor.extract_pages(
            pdf_path=str(pdf_path),
            pages=pages
        )

        return extracted_path

    def convert_pdf_to_markdown(
            self,
            pdf_path: str
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
        Send Markdown to Gemini-1.5-Flash and extract table as CSV.

        Args:
            markdown_content: Markdown content from PDF
            table_description: Description of what table to extract

        Returns:
            CSV formatted string
        """

        # Construct the prompt for table extraction
        prompt = f"""You are a data extraction assistant.
Extract the table from the following Markdown content and convert it to CSV format.

Instructions:
1. Identify the main data table in the Markdown content
2. Extract all rows and columns accurately
3. Preserve numeric values exactly (no rounding)
4. Include headers as the first row
5. Identify and include row indices as the first column
6. Use comma as delimiter
7. Fill missing values as 'nan'
8. Output ONLY the CSV data, no markdown code blocks, no explanations

{f"Additional context: {table_description}" if table_description else ""}

The Markdown content: {markdown_content}"""

        url = f"{self.llm_base_url}/api/generate"
        payload = {
            "model": self.llm_model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": os.getenv("MODEL_TEMPERATURE"),  # Low temperature for accurate extraction
                "num_predict": os.getenv("MODEL_MAX_OUTPUT_TOKENS")  # Max tokens for response
            }
        }

        print(f"🤖 Sending request to Qwen2.5-VL:3b...")

        try:
            # Generate content using Qwen
            response = requests.post(url, json=payload, timeout=300)
            response.raise_for_status()

            result = response.json()
            csv_content = result.get("response", "").strip()

            # Clean up response (remove Markdown code blocks if present)
            csv_content = self._clean_csv_response(csv_content)

            print(f"✅ Table extracted ({len(csv_content.split(chr(10)))} rows)")
            return csv_content

        except Exception as e:
            print(f"❌ Gemini extraction failed: {e}")
            raise

    def _clean_csv_response(self, csv_content: str) -> str:
        """Remove Markdown code blocks and clean CSV output."""
        # Remove Markdown code blocks
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
    ) -> dict:
        """
        Complete pipeline: PDF → Page Extraction → Markdown → CSV → File

        Args:
            pdf_path: Path to input PDF
            output_filename: Name for output CSV (without extension)
            pages: Specific pages to process (0-indexed, optional)
            table_description: Description of table to extract

        Returns:
            Dictionary with processing results
        """
        print(f"\n{'=' * 60}")
        print(f"🚀 Starting PDF to CSV Extraction Pipeline (Gemini)")
        print(f"{'=' * 60}\n")

        results = {
            "input_pdf": pdf_path,
            "output_csv": None,
            "pages_extracted": pages,
            "status": "failed"
        }

        extracted_pdf_path = None

        try:
            # Step 1: Extract specific pages (if specified)
            extracted_pdf_path = self.extract_pages_from_pdf(pdf_path, pages)

            # Step 2: Convert PDF to Markdown and save in case of fatal error
            markdown_content = self.convert_pdf_to_markdown(extracted_pdf_path)

            with open(rf"{os.getenv('PROJECT_PATH')}/data_pipeline/markdown/{output_filename}.md", "w") as mdfile:
                mdfile.write(markdown_content)

            quit()

            # Step 3: Extract table with LLM
            csv_content = self.extract_table_with_llm(markdown_content, table_description)

            # Step 4: Save CSV
            csv_path = self.save_csv(csv_content, output_filename)
            results["output_csv"] = str(csv_path)

            # Step 5: Validate
            is_valid = self.validate_csv(csv_path)

            if is_valid:
                results["status"] = "success"

            print(f"\n{'=' * 60}")
            print(f"✅ Pipeline Complete!")
            print(f"{'=' * 60}\n")

        except Exception as e:
            print(f"\n❌ Pipeline failed: {e}")
            results["error"] = str(e)

        finally:
            # Cleanup temporary files
            if self.cleanup_temp_flag and extracted_pdf_path and self.page_extractor.temp_dir in Path(
                    extracted_pdf_path).parents:
                self.page_extractor.cleanup_temp()

        return results

    def process_multiple_pdfs(
            self,
            pdf_configs: List[dict],
            delay_between: float = 2.0
    ) -> List[dict]:
        """
        Process multiple PDF files with configurable settings.

        Args:
            pdf_configs: List of dictionaries with pdf_path, output_filename, pages, table_description
            delay_between: Seconds to wait between API calls

        Returns:
            List of processing results
        """
        all_results = []

        for i, config in enumerate(pdf_configs, 1):
            print(f"\n{'#' * 60}")
            print(f"# Processing PDF {i}/{len(pdf_configs)}: {config.get('output_filename', 'unknown')}")
            print(f"{'#' * 60}\n")

            result = self.process_pdf(
                pdf_path=config["pdf_path"],
                output_filename=config["output_filename"],
                pages=config.get("pages"),
                table_description=config.get("table_description", "")
            )

            all_results.append(result)

            print(f"⏳ Waiting {delay_between}s before next request...")
            time.sleep(delay_between)

            # Summary
        print(f"\n{'=' * 60}")
        print(f"📊 Batch Processing Summary")
        print(f"{'=' * 60}")
        successful = sum(1 for r in all_results if r["status"] == "success")
        print(f"✅ Successful: {successful}/{len(all_results)}")
        print(f"❌ Failed: {len(all_results) - successful}/{len(all_results)}")

        return all_results
