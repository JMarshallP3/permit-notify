"""
Unit tests for PDF parsing functionality.
"""

import unittest
from unittest.mock import patch, MagicMock
import sys
import os

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from services.enrichment.pdf_parse import parse_w1_content, calculate_pdf_sha256

class TestPDFParse(unittest.TestCase):
    """Test cases for PDF parsing functionality."""
    
    def test_parse_w1_content_field_name_pattern1(self):
        """Test field name extraction with pattern 1: FIELD NAME: value"""
        text = """
        TEXAS RAILROAD COMMISSION
        DRILLING PERMIT APPLICATION
        
        FIELD NAME: EAGLE FORD SHALE
        
        OPERATOR: DIAMONDBACK E&P LLC
        LEASE: FASKEN 1A
        """
        
        field_name, well_count, confidence, snippet = parse_w1_content(text)
        
        self.assertEqual(field_name, "EAGLE FORD SHALE")
        self.assertGreater(confidence, 0.0)
        self.assertIn("EAGLE FORD SHALE", snippet)
    
    def test_parse_w1_content_field_name_pattern2(self):
        """Test field name extraction with pattern 2: FIELD: value"""
        text = """
        TEXAS RAILROAD COMMISSION
        DRILLING PERMIT APPLICATION
        
        FIELD: PERMIAN BASIN
        
        OPERATOR: OVINTIV USA INC.
        LEASE: FASKEN 1B
        """
        
        field_name, well_count, confidence, snippet = parse_w1_content(text)
        
        self.assertEqual(field_name, "PERMIAN BASIN")
        self.assertGreater(confidence, 0.0)
        self.assertIn("PERMIAN BASIN", snippet)
    
    def test_parse_w1_content_well_count_explicit(self):
        """Test well count extraction with explicit labels"""
        text = """
        TEXAS RAILROAD COMMISSION
        DRILLING PERMIT APPLICATION
        
        FIELD NAME: EAGLE FORD SHALE
        
        TOTAL NUMBER OF WELLS: 3
        
        OPERATOR: DIAMONDBACK E&P LLC
        """
        
        field_name, well_count, confidence, snippet = parse_w1_content(text)
        
        self.assertEqual(well_count, 3)
        self.assertGreaterEqual(confidence, 0.6)  # Should get 0.6 for explicit count
    
    def test_parse_w1_content_well_count_alternative(self):
        """Test well count extraction with alternative label"""
        text = """
        TEXAS RAILROAD COMMISSION
        DRILLING PERMIT APPLICATION
        
        FIELD: PERMIAN BASIN
        
        NUMBER OF WELLS: 5
        
        OPERATOR: OVINTIV USA INC.
        """
        
        field_name, well_count, confidence, snippet = parse_w1_content(text)
        
        self.assertEqual(well_count, 5)
        self.assertGreaterEqual(confidence, 0.6)  # Should get 0.6 for explicit count
    
    def test_parse_w1_content_well_count_fallback(self):
        """Test well count extraction with fallback method (counting well IDs)"""
        text = """
        TEXAS RAILROAD COMMISSION
        DRILLING PERMIT APPLICATION
        
        FIELD NAME: EAGLE FORD SHALE
        
        WELL NO. 303HL
        WELL NO. 305HJ
        WELL NO. 308HL
        
        OPERATOR: DIAMONDBACK E&P LLC
        """
        
        field_name, well_count, confidence, snippet = parse_w1_content(text)
        
        self.assertEqual(well_count, 3)  # Should count 3 distinct well numbers
        self.assertGreaterEqual(confidence, 0.3)  # Should get 0.3 for fallback method
    
    def test_parse_w1_content_well_count_fallback_deduplication(self):
        """Test well count extraction with deduplication of well IDs"""
        text = """
        TEXAS RAILROAD COMMISSION
        DRILLING PERMIT APPLICATION
        
        FIELD NAME: EAGLE FORD SHALE
        
        WELL NO. 303HL
        WELL NO. 305HJ
        WELL NO. 303HL  # Duplicate
        WELL NO. 308HL
        
        OPERATOR: DIAMONDBACK E&P LLC
        """
        
        field_name, well_count, confidence, snippet = parse_w1_content(text)
        
        self.assertEqual(well_count, 3)  # Should count 3 distinct well numbers (deduplicated)
        self.assertGreaterEqual(confidence, 0.3)
    
    def test_parse_w1_content_confidence_scoring(self):
        """Test confidence scoring with multiple factors"""
        text = """
        TEXAS RAILROAD COMMISSION
        DRILLING PERMIT APPLICATION
        
        FIELD NAME: EAGLE FORD SHALE
        
        TOTAL NUMBER OF WELLS: 2
        
        WELL NO. 303HL
        WELL NO. 305HJ
        
        OPERATOR: DIAMONDBACK E&P LLC
        """
        
        field_name, well_count, confidence, snippet = parse_w1_content(text)
        
        # Should get: 0.6 (explicit count) + 0.1 (field name) = 0.7
        # Note: fallback well ID counting is only used when no explicit count is found
        self.assertEqual(field_name, "EAGLE FORD SHALE")
        self.assertEqual(well_count, 2)
        self.assertAlmostEqual(confidence, 0.7, places=1)
    
    def test_parse_w1_content_confidence_clipping(self):
        """Test that confidence is clipped to [0, 1] range"""
        text = """
        TEXAS RAILROAD COMMISSION
        DRILLING PERMIT APPLICATION
        
        FIELD NAME: EAGLE FORD SHALE
        
        TOTAL NUMBER OF WELLS: 2
        
        WELL NO. 303HL
        WELL NO. 305HJ
        WELL NO. 308HL
        
        OPERATOR: DIAMONDBACK E&P LLC
        """
        
        field_name, well_count, confidence, snippet = parse_w1_content(text)
        
        # Should be clipped to 1.0 even if it would be higher
        self.assertLessEqual(confidence, 1.0)
        self.assertGreaterEqual(confidence, 0.0)
    
    def test_parse_w1_content_no_matches(self):
        """Test parsing with no matches found"""
        text = """
        TEXAS RAILROAD COMMISSION
        DRILLING PERMIT APPLICATION
        
        OPERATOR: DIAMONDBACK E&P LLC
        LEASE: FASKEN 1A
        """
        
        field_name, well_count, confidence, snippet = parse_w1_content(text)
        
        self.assertIsNone(field_name)
        self.assertIsNone(well_count)
        self.assertEqual(confidence, 0.0)
        self.assertIn("DIAMONDBACK", snippet)
    
    def test_parse_w1_content_empty_text(self):
        """Test parsing with empty text"""
        field_name, well_count, confidence, snippet = parse_w1_content("")
        
        self.assertIsNone(field_name)
        self.assertIsNone(well_count)
        self.assertEqual(confidence, 0.0)
        self.assertEqual(snippet, "")
    
    def test_parse_w1_content_whitespace_only(self):
        """Test parsing with whitespace-only text"""
        field_name, well_count, confidence, snippet = parse_w1_content("   \n\t   ")
        
        self.assertIsNone(field_name)
        self.assertIsNone(well_count)
        self.assertEqual(confidence, 0.0)
        self.assertEqual(snippet, "")
    
    def test_parse_w1_content_snippet_truncation(self):
        """Test that text snippet is properly truncated"""
        long_text = "A" * 1000  # 1000 character text
        
        field_name, well_count, confidence, snippet = parse_w1_content(long_text)
        
        self.assertLessEqual(len(snippet), 800)
        self.assertEqual(snippet, long_text[:800])
    
    def test_calculate_pdf_sha256(self):
        """Test PDF SHA256 calculation"""
        test_bytes = b"test pdf content"
        expected_hash = "a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456"
        
        # Mock the hashlib.sha256 to return a predictable result
        with patch('hashlib.sha256') as mock_sha256:
            mock_sha256.return_value.hexdigest.return_value = expected_hash
            
            result = calculate_pdf_sha256(test_bytes)
            
            self.assertEqual(result, expected_hash)
            mock_sha256.assert_called_once_with(test_bytes)
    
    def test_parse_w1_content_complex_well_ids(self):
        """Test parsing with complex well ID formats"""
        text = """
        TEXAS RAILROAD COMMISSION
        DRILLING PERMIT APPLICATION
        
        FIELD NAME: EAGLE FORD SHALE
        
        WELL NO. F101CF
        WELL NO. F101DE
        WELL NO. 4TB
        WELL NO. 5TB
        
        OPERATOR: DIAMONDBACK E&P LLC
        """
        
        field_name, well_count, confidence, snippet = parse_w1_content(text)
        
        self.assertEqual(well_count, 4)  # Should count all 4 distinct well numbers
        self.assertGreaterEqual(confidence, 0.3)
    
    def test_parse_w1_content_mixed_formats(self):
        """Test parsing with mixed field name and well count formats"""
        text = """
        TEXAS RAILROAD COMMISSION
        DRILLING PERMIT APPLICATION
        
        FIELD: PERMIAN BASIN
        
        NUMBER OF WELLS: 2
        
        WELL NO. 303HL
        WELL NO. 305HJ
        
        OPERATOR: OVINTIV USA INC.
        """
        
        field_name, well_count, confidence, snippet = parse_w1_content(text)
        
        self.assertEqual(field_name, "PERMIAN BASIN")
        self.assertEqual(well_count, 2)
        self.assertGreaterEqual(confidence, 0.7)  # 0.6 (explicit) + 0.1 (field name)

if __name__ == '__main__':
    unittest.main()
