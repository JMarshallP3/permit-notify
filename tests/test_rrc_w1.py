"""
Simple test for RRC W-1 scraper functionality.
"""

import pytest
from unittest.mock import patch, MagicMock
from services.scraper.rrc_w1 import RRCW1Client


class TestRRCW1Client:
    """Test cases for RRCW1Client."""
    
    def test_client_initialization(self):
        """Test that RRCW1Client initializes correctly."""
        client = RRCW1Client()
        
        assert client.base_url == "https://webapps.rrc.state.tx.us/DP"
        assert client.timeout == 20
        assert "PermitTrackerBot" in client.user_agent
        assert client.session.headers["User-Agent"] == client.user_agent
    
    def test_client_with_custom_params(self):
        """Test RRCW1Client with custom parameters."""
        client = RRCW1Client(
            base_url="https://test.example.com",
            timeout=30,
            user_agent="TestBot/1.0"
        )
        
        assert client.base_url == "https://test.example.com"
        assert client.timeout == 30
        assert client.user_agent == "TestBot/1.0"
    
    @patch('requests.Session.get')
    def test_get_request(self, mock_get):
        """Test _get method with relative URL."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        client = RRCW1Client()
        response = client._get("/test/path")
        
        assert response == mock_response
        mock_get.assert_called_once()
    
    @patch('requests.Session.post')
    def test_post_request(self, mock_post):
        """Test _post method with form data."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        
        client = RRCW1Client()
        response = client._post("/test/path", {"field": "value"})
        
        assert response == mock_response
        mock_post.assert_called_once()
    
    def test_soup_parsing(self):
        """Test _soup method."""
        client = RRCW1Client()
        html = "<html><body><h1>Test</h1></body></html>"
        
        soup = client._soup(html)
        
        assert soup.find('h1').get_text() == "Test"
    
    def test_infer_submitted_date_names_success(self):
        """Test successful date field name inference."""
        html = """
        <html>
            <body>
                <form>
                    <label>Submitted Date:</label>
                    <input type="text" name="submitDateBegin" />
                    <input type="text" name="submitDateEnd" />
                </form>
            </body>
        </html>
        """
        
        client = RRCW1Client()
        soup = client._soup(html)
        
        begin_name, end_name = client._infer_submitted_date_names(soup)
        
        assert begin_name == "submitDateBegin"
        assert end_name == "submitDateEnd"
    
    def test_infer_submitted_date_names_fallback(self):
        """Test date field name inference with fallback strategy."""
        html = """
        <html>
            <body>
                <form>
                    <input type="text" name="dateFrom" />
                    <input type="text" name="dateTo" />
                </form>
            </body>
        </html>
        """
        
        client = RRCW1Client()
        soup = client._soup(html)
        
        begin_name, end_name = client._infer_submitted_date_names(soup)
        
        assert begin_name == "dateFrom"
        assert end_name == "dateTo"
    
    def test_parse_table_success(self):
        """Test successful table parsing."""
        html = """
        <html>
            <body>
                <table>
                    <tr>
                        <th>Status Date</th>
                        <th>Status</th>
                        <th>API No.</th>
                        <th>Operator</th>
                    </tr>
                    <tr>
                        <td>01/15/2024</td>
                        <td>12345</td>
                        <td>42-000-00001</td>
                        <td>Test Oil Co</td>
                    </tr>
                </table>
            </body>
        </html>
        """
        
        client = RRCW1Client()
        soup = client._soup(html)
        
        rows = client._parse_table(soup)
        
        assert len(rows) == 1
        assert rows[0]["status_date"] == "01/15/2024"
        assert rows[0]["status"] == "12345"
        assert rows[0]["api_no"] == "42-000-00001"
        assert rows[0]["operator"] == "Test Oil Co"
    
    def test_next_page_url_found(self):
        """Test next page URL detection."""
        html = """
        <html>
            <body>
                <a href="next_page.html">Next ></a>
            </body>
        </html>
        """
        
        client = RRCW1Client()
        soup = client._soup(html)
        
        next_url = client._next_page_url(soup, "https://test.com/current")
        
        assert next_url == "https://test.com/next_page.html"
    
    def test_next_page_url_not_found(self):
        """Test when no next page URL is found."""
        html = """
        <html>
            <body>
                <p>No pagination links</p>
            </body>
        </html>
        """
        
        client = RRCW1Client()
        soup = client._soup(html)
        
        next_url = client._next_page_url(soup, "https://test.com/current")
        
        assert next_url is None


if __name__ == "__main__":
    pytest.main([__file__])
