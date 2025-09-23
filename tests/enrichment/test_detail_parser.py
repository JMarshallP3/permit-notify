"""
Unit tests for detail page parser functionality.
"""

import unittest
from unittest.mock import patch, MagicMock
import sys
import os

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from services.enrichment.detail_parser import parse_detail_page

class TestDetailParser(unittest.TestCase):
    """Test cases for detail page parser functionality."""
    
    def test_parse_horizontal_wellbore(self):
        """Test horizontal wellbore extraction."""
        html = """
        <html>
        <body>
            <table>
                <tr>
                    <td>Horizontal Wellbore</td>
                    <td>Yes</td>
                </tr>
            </table>
        </body>
        </html>
        """
        
        result = parse_detail_page(html)
        
        self.assertEqual(result["horizontal_wellbore"], "Yes")
    
    def test_parse_survey_info(self):
        """Test survey/legal location information extraction."""
        html = """
        <html>
        <body>
            <h3>Survey/Legal Location Information</h3>
            <table>
                <tr>
                    <th>Section</th>
                    <th>Block</th>
                    <th>Survey</th>
                    <th>Abstract #</th>
                </tr>
                <tr>
                    <td>1</td>
                    <td>2</td>
                    <td>H&TC</td>
                    <td>123</td>
                </tr>
            </table>
        </body>
        </html>
        """
        
        result = parse_detail_page(html)
        
        self.assertEqual(result["section"], "1")
        self.assertEqual(result["block"], "2")
        self.assertEqual(result["survey"], "H&TC")
        self.assertEqual(result["abstract_no"], "123")
    
    def test_parse_field_info(self):
        """Test field information extraction."""
        html = """
        <html>
        <body>
            <h3>Fields</h3>
            <table>
                <tr>
                    <th>Field Name</th>
                    <th>Acres</th>
                </tr>
                <tr>
                    <td>Eagle Ford Shale</td>
                    <td>640.00</td>
                </tr>
            </table>
        </body>
        </html>
        """
        
        result = parse_detail_page(html)
        
        self.assertEqual(result["field_name"], "Eagle Ford Shale")
        self.assertEqual(result["acres"], 640.00)
    
    def test_parse_w1_pdf_url(self):
        """Test W-1 PDF URL extraction."""
        html = """
        <html>
        <body>
            <a href="/DP/viewW1PdfFormAction.do?permitId=123">View Current W-1</a>
        </body>
        </html>
        """
        
        result = parse_detail_page(html)
        
        self.assertEqual(result["view_w1_pdf_url"], "https://webapps.rrc.state.tx.us/DP/viewW1PdfFormAction.do?permitId=123")
    
    def test_parse_w1_pdf_url_alternative(self):
        """Test W-1 PDF URL extraction with alternative pattern."""
        html = """
        <html>
        <body>
            <a href="/DP/viewW1PdfFormAction.do?permitId=456">View W-1</a>
        </body>
        </html>
        """
        
        result = parse_detail_page(html)
        
        self.assertEqual(result["view_w1_pdf_url"], "https://webapps.rrc.state.tx.us/DP/viewW1PdfFormAction.do?permitId=456")
    
    def test_parse_complete_page(self):
        """Test parsing a complete page with all fields."""
        html = """
        <html>
        <body>
            <table>
                <tr>
                    <td>Horizontal Wellbore</td>
                    <td>Yes</td>
                </tr>
            </table>
            
            <h3>Survey/Legal Location Information</h3>
            <table>
                <tr>
                    <th>Section</th>
                    <th>Block</th>
                    <th>Survey</th>
                    <th>Abstract #</th>
                </tr>
                <tr>
                    <td>1</td>
                    <td>2</td>
                    <td>H&TC</td>
                    <td>123</td>
                </tr>
            </table>
            
            <h3>Fields</h3>
            <table>
                <tr>
                    <th>Field Name</th>
                    <th>Acres</th>
                </tr>
                <tr>
                    <td>Eagle Ford Shale</td>
                    <td>640.00</td>
                </tr>
            </table>
            
            <a href="/DP/viewW1PdfFormAction.do?permitId=789">View Current W-1</a>
        </body>
        </html>
        """
        
        result = parse_detail_page(html)
        
        self.assertEqual(result["horizontal_wellbore"], "Yes")
        self.assertEqual(result["section"], "1")
        self.assertEqual(result["block"], "2")
        self.assertEqual(result["survey"], "H&TC")
        self.assertEqual(result["abstract_no"], "123")
        self.assertEqual(result["field_name"], "Eagle Ford Shale")
        self.assertEqual(result["acres"], 640.00)
        self.assertEqual(result["view_w1_pdf_url"], "https://webapps.rrc.state.tx.us/DP/viewW1PdfFormAction.do?permitId=789")
    
    def test_parse_empty_html(self):
        """Test parsing empty HTML."""
        result = parse_detail_page("")
        
        self.assertIsNone(result["horizontal_wellbore"])
        self.assertIsNone(result["field_name"])
        self.assertIsNone(result["acres"])
        self.assertIsNone(result["section"])
        self.assertIsNone(result["block"])
        self.assertIsNone(result["survey"])
        self.assertIsNone(result["abstract_no"])
        self.assertIsNone(result["view_w1_pdf_url"])
    
    def test_parse_malformed_html(self):
        """Test parsing malformed HTML."""
        html = "<html><body><table><tr><td>Incomplete"
        
        result = parse_detail_page(html)
        
        # Should not crash and return None values
        self.assertIsNone(result["horizontal_wellbore"])
        self.assertIsNone(result["field_name"])
    
    def test_parse_case_insensitive(self):
        """Test case-insensitive parsing."""
        html = """
        <html>
        <body>
            <table>
                <tr>
                    <td>HORIZONTAL WELLBORE</td>
                    <td>YES</td>
                </tr>
            </table>
        </body>
        </html>
        """
        
        result = parse_detail_page(html)
        
        self.assertEqual(result["horizontal_wellbore"], "YES")

if __name__ == '__main__':
    unittest.main()
