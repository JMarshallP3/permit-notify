"""
Unit tests for PDF parsing functionality.
"""

import unittest
from unittest.mock import patch, MagicMock
import sys
import os

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from services.enrichment.pdf_parse import parse_reservoir_well_count, extract_text_from_pdf

class TestPDFParse(unittest.TestCase):
    """Test cases for PDF parsing functionality."""
    
    def test_parse_reservoir_well_count_success(self):
        """Test successful reservoir well count extraction."""
        text = """
        TEXAS RAILROAD COMMISSION
        DRILLING PERMIT APPLICATION
        
        Number of Wells on this lease in this Reservoir: 5
        
        OPERATOR: DIAMONDBACK E&P LLC
        """
        
        well_count, confidence, snippet = parse_reservoir_well_count(text)
        
        self.assertEqual(well_count, 5)
        self.assertEqual(confidence, 0.85)
        self.assertIn("Number of Wells on this lease in this Reservoir: 5", snippet)
    
    def test_parse_reservoir_well_count_multiline(self):
        """Test reservoir well count extraction with multiline text."""
        text = """
        TEXAS RAILROAD COMMISSION
        DRILLING PERMIT APPLICATION
        
        Number of Wells on this lease
        in this Reservoir: 3
        
        OPERATOR: OVINTIV USA INC.
        """
        
        well_count, confidence, snippet = parse_reservoir_well_count(text)
        
        self.assertEqual(well_count, 3)
        self.assertEqual(confidence, 0.85)
        self.assertIn("Number of Wells on this lease", snippet)
    
    def test_parse_reservoir_well_count_case_insensitive(self):
        """Test case-insensitive reservoir well count extraction."""
        text = """
        TEXAS RAILROAD COMMISSION
        DRILLING PERMIT APPLICATION
        
        NUMBER OF WELLS ON THIS LEASE IN THIS RESERVOIR: 7
        
        OPERATOR: CHEVRON U. S. A. INC.
        """
        
        well_count, confidence, snippet = parse_reservoir_well_count(text)
        
        self.assertEqual(well_count, 7)
        self.assertEqual(confidence, 0.85)
        self.assertIn("NUMBER OF WELLS ON THIS LEASE IN THIS RESERVOIR: 7", snippet)
    
    def test_parse_reservoir_well_count_no_match(self):
        """Test reservoir well count extraction when no pattern is found."""
        text = """
        TEXAS RAILROAD COMMISSION
        DRILLING PERMIT APPLICATION
        
        OPERATOR: DIAMONDBACK E&P LLC
        LEASE: FASKEN 1A
        """
        
        well_count, confidence, snippet = parse_reservoir_well_count(text)
        
        self.assertIsNone(well_count)
        self.assertEqual(confidence, 0.0)
        self.assertIsInstance(snippet, str)
        self.assertGreater(len(snippet), 0)
    
    def test_parse_reservoir_well_count_empty_text(self):
        """Test reservoir well count extraction with empty text."""
        well_count, confidence, snippet = parse_reservoir_well_count("")
        
        self.assertIsNone(well_count)
        self.assertEqual(confidence, 0.0)
        self.assertEqual(snippet, "")
    
    def test_parse_reservoir_well_count_whitespace_only(self):
        """Test reservoir well count extraction with whitespace-only text."""
        well_count, confidence, snippet = parse_reservoir_well_count("   \n\t   ")
        
        self.assertIsNone(well_count)
        self.assertEqual(confidence, 0.0)
        self.assertEqual(snippet, "")
    
    def test_parse_reservoir_well_count_invalid_number(self):
        """Test reservoir well count extraction with invalid number."""
        text = """
        TEXAS RAILROAD COMMISSION
        DRILLING PERMIT APPLICATION
        
        Number of Wells on this lease in this Reservoir: ABC
        
        OPERATOR: DIAMONDBACK E&P LLC
        """
        
        well_count, confidence, snippet = parse_reservoir_well_count(text)
        
        self.assertIsNone(well_count)
        self.assertEqual(confidence, 0.0)
        self.assertIsInstance(snippet, str)
        self.assertGreater(len(snippet), 0)
    
    def test_parse_reservoir_well_count_large_number(self):
        """Test reservoir well count extraction with large number."""
        text = """
        TEXAS RAILROAD COMMISSION
        DRILLING PERMIT APPLICATION
        
        Number of Wells on this lease in this Reservoir: 12345
        
        OPERATOR: DIAMONDBACK E&P LLC
        """
        
        well_count, confidence, snippet = parse_reservoir_well_count(text)
        
        self.assertEqual(well_count, 12345)
        self.assertEqual(confidence, 0.85)
        self.assertIn("Number of Wells on this lease in this Reservoir: 12345", snippet)
    
    def test_parse_reservoir_well_count_snippet_length(self):
        """Test that snippet is properly truncated."""
        text = "A" * 1000 + "Number of Wells on this lease in this Reservoir: 2" + "B" * 1000
        
        well_count, confidence, snippet = parse_reservoir_well_count(text)
        
        self.assertEqual(well_count, 2)
        self.assertEqual(confidence, 0.85)
        self.assertLessEqual(len(snippet), 450)
        self.assertIn("Number of Wells on this lease in this Reservoir: 2", snippet)
    
    def test_extract_text_from_pdf_success(self):
        """Test successful PDF text extraction."""
        # Mock PDF bytes
        pdf_bytes = b"Mock PDF content"
        
        with patch('services.enrichment.pdf_parse.extract_text') as mock_extract:
            mock_extract.return_value = "Extracted text content"
            
            result = extract_text_from_pdf(pdf_bytes)
            
            self.assertEqual(result, "Extracted text content")
            mock_extract.assert_called_once()
    
    def test_extract_text_from_pdf_failure(self):
        """Test PDF text extraction failure."""
        # Mock PDF bytes
        pdf_bytes = b"Invalid PDF content"
        
        with patch('services.enrichment.pdf_parse.extract_text') as mock_extract:
            mock_extract.side_effect = Exception("PDF extraction failed")
            
            # Mock the fallback low-level API
            with patch('services.enrichment.pdf_parse.PDFResourceManager') as mock_rm:
                with patch('services.enrichment.pdf_parse.TextConverter') as mock_converter:
                    with patch('services.enrichment.pdf_parse.PDFPageInterpreter') as mock_interpreter:
                        with patch('services.enrichment.pdf_parse.PDFPage.get_pages') as mock_pages:
                            mock_pages.return_value = []
                            
                            result = extract_text_from_pdf(pdf_bytes)
                            
                            self.assertEqual(result, "")
    
    def test_parse_reservoir_well_count_complex_text(self):
        """Test reservoir well count extraction with complex text."""
        text = """
        TEXAS RAILROAD COMMISSION
        DRILLING PERMIT APPLICATION
        
        This is a complex document with multiple sections.
        
        Section 1: Basic Information
        Operator: DIAMONDBACK E&P LLC
        Lease: FASKEN 1A
        
        Section 2: Reservoir Information
        Number of Wells on this lease in this Reservoir: 4
        
        Section 3: Additional Details
        Total Depth: 10,000 feet
        """
        
        well_count, confidence, snippet = parse_reservoir_well_count(text)
        
        self.assertEqual(well_count, 4)
        self.assertEqual(confidence, 0.85)
        self.assertIn("Number of Wells on this lease in this Reservoir: 4", snippet)

if __name__ == '__main__':
    unittest.main()
