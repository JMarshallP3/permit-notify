"""
Simple smoke test for Scraper class.
Tests basic functionality with mocked HTTP responses.
"""

import pytest
from unittest.mock import Mock, patch
import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.scraper.scraper import Scraper


class TestScraperSmoke:
    """Smoke tests for Scraper class."""
    
    def test_scraper_run_with_mocked_table(self):
        """Test that Scraper.run() correctly parses a mocked HTML table."""
        # Create mock HTML with a simple permit table
        mock_html = """
        <html>
        <head><title>Test Permits Page</title></head>
        <body>
            <h1>Drilling Permits</h1>
            <table>
                <thead>
                    <tr>
                        <th>Permit</th>
                        <th>Operator</th>
                        <th>County</th>
                        <th>District</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>12345</td>
                        <td>Test Oil Co</td>
                        <td>Harris</td>
                        <td>1</td>
                    </tr>
                </tbody>
            </table>
        </body>
        </html>
        """
        
        # Create mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = mock_html
        
        # Mock the session.get method
        with patch('services.scraper.scraper.requests.Session.get') as mock_get:
            mock_get.return_value = mock_response
            
            # Create scraper instance and run
            scraper = Scraper()
            result = scraper.run("https://test.example.com/permits")
            
            # Assertions
            assert result is not None
            assert "source_url" in result
            assert "items" in result
            assert "csv_link" in result
            assert "fetched_at" in result
            
            # Check that we found the table and extracted data
            assert len(result["items"]) == 1
            
            # Check the normalized data structure
            permit_item = result["items"][0]
            assert permit_item["permit_no"] == "12345"
            assert permit_item["operator"] == "Test Oil Co"
            assert permit_item["county"] == "Harris"
            assert permit_item["district"] == "1"
            
            # Check that other fields are None (not present in test data)
            assert permit_item["well_name"] is None
            assert permit_item["lease_no"] is None
            assert permit_item["field"] is None
            assert permit_item["submission_date"] is None
            assert permit_item["api_no"] is None
            
            # Verify the mock was called
            mock_get.assert_called_once()
    
    def test_scraper_run_with_no_table(self):
        """Test that Scraper.run() handles pages with no permit table gracefully."""
        # Create mock HTML without a permit table
        mock_html = """
        <html>
        <head><title>No Permits Here</title></head>
        <body>
            <h1>Welcome to our site</h1>
            <p>No permit data available.</p>
        </body>
        </html>
        """
        
        # Create mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = mock_html
        
        # Mock the session.get method
        with patch('services.scraper.scraper.requests.Session.get') as mock_get:
            mock_get.return_value = mock_response
            
            # Create scraper instance and run
            scraper = Scraper()
            result = scraper.run("https://test.example.com/no-permits")
            
            # Assertions
            assert result is not None
            assert "source_url" in result
            assert "items" in result
            assert "csv_link" in result
            assert "fetched_at" in result
            
            # Check that no items were found
            assert len(result["items"]) == 0
            
            # Check that a warning was included
            assert "warning" in result
            assert "No permit table found" in result["warning"]
            
            # Verify the mock was called
            mock_get.assert_called_once()
    
    def test_scraper_initialization(self):
        """Test that Scraper initializes correctly with default settings."""
        scraper = Scraper()
        
        # Check that scraper has expected attributes
        assert hasattr(scraper, 'user_agent')
        assert hasattr(scraper, 'scrape_timeout')
        assert hasattr(scraper, 'session')
        
        # Check default values
        assert scraper.user_agent == 'PermitNotifyBot/1.0 (contact: marshall@craatx.com)'
        assert scraper.scrape_timeout == 15
