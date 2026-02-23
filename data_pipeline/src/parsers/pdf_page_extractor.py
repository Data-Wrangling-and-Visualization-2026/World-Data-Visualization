#!/usr/bin/env python3
"""
PDF Page Pre-Extraction Module
Uses PyPDF to extract specific pages before Docling conversion
"""

import os
from pathlib import Path
from typing import List, Optional
from pypdf import PdfReader, PdfWriter
import shutil


class PDFPageExtractor:
    """Extract specific pages from PDF files using PyPDF."""

    def __init__(self, temp_dir: str = os.getenv("TEMP_PATH")):
        """
        Initialize the PDF page extractor.

        Args:
            temp_dir: Directory for temporary extracted PDFs
        """
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    def extract_pages(
            self,
            pdf_path: str,
            pages: List[int],
            output_path: Optional[str] = None
    ) -> str:
        """
        Extract specific pages from a PDF file.

        Args:
            pdf_path: Path to the source PDF file
            pages: List of page numbers to extract (0-indexed)
            output_path: Optional path for output file (default: temp directory)

        Returns:
            Path to the extracted PDF file
        """
        pdf_path = Path(pdf_path)

        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")

        # Validate pages
        reader = PdfReader(str(pdf_path))
        total_pages = len(reader.pages)

        # Convert to 0-indexed and validate
        valid_pages = []

        for page in pages:
            if 0 <= page < total_pages:
                valid_pages.append(page)
            else:
                print(f"⚠️ Warning: Page {page} out of range (0-{total_pages - 1}), skipping")

        if not valid_pages:
            raise ValueError(f"❌ No valid pages to extract from {pdf_path.name}")

        # Create output path if not provided
        if output_path is None:
            output_path = self.temp_dir / f"{pdf_path.stem}_pages_{'-'.join(map(str, valid_pages))}.pdf"
        else:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

        # Extract pages
        writer = PdfWriter()

        for page_num in valid_pages:
            writer.add_page(reader.pages[page_num])

        # Write extracted pages
        with open(output_path, "wb") as f:
            writer.write(f)

        print(f"✅ Extracted {len(valid_pages)} pages from {pdf_path.name} -> {output_path.name}")
        return str(output_path)

    def cleanup_temp(self):
        """Remove all temporary extracted PDFs."""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
            print(f"🧹 Cleaned up temporary directory: {self.temp_dir}")


if __name__ == "__main__":
    # Example usage
    extractor = PDFPageExtractor()

    # Extract specific pages (e.g., pages 0, 1, 2 which contain tables)
    extracted_path = extractor.extract_pages(
        pdf_path="data_pipeline/raw_data/energy_inst.pdf",
        pages=[0, 1, 2],
        output_path="data_pipeline/temp/energy_inst_pages_0-2.pdf"
    )

    print(f"✅ Extracted PDF saved to: {extracted_path}")