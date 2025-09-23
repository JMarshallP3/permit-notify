"""
Simple test for RRC W-1 scraper functionality.
"""

import pytest
from unittest.mock import patch, MagicMock
from services.scraper.rrc_w1 import RRCW1Client, RequestsEngine, PlaywrightEngine


class TestRRCW1Client:
    """Test cases for RRCW1Client."""
    
    def test_client_initialization(self):
        """Test that RRCW1Client initializes correctly."""
        client = RRCW1Client()
        
        assert client.base_url == "https://webapps.rrc.state.tx.us"
        assert client.timeout == 30  # Default from environment
        assert client.primary_engine == 'requests'  # Default engine
    
    def test_client_with_custom_params(self):
        """Test RRCW1Client with custom parameters."""
        client = RRCW1Client(base_url="https://test.example.com")
        
        assert client.base_url == "https://test.example.com"
        assert client.timeout == 30  # Still uses environment default
    
    @patch('services.scraper.rrc_w1.RequestsEngine')
    def test_fetch_all_requests_engine_success(self, mock_requests_engine):
        """Test fetch_all method using RequestsEngine successfully."""
        # Mock the engine and its fetch_all method
        mock_engine_instance = MagicMock()
        mock_engine_instance.fetch_all.return_value = {
            "source_root": "https://webapps.rrc.state.tx.us",
            "query_params": {"begin": "01/01/2024", "end": "01/31/2024"},
            "pages": 1,
            "count": 5,
            "items": [
                {"status_no": "12345", "operator_name": "Test Oil Co"},
                {"status_no": "12346", "operator_name": "Test Oil Co 2"}
            ],
            "fetched_at": "2024-01-15T10:00:00Z",
            "method": "requests",
            "success": True
        }
        mock_requests_engine.return_value = mock_engine_instance
        
        client = RRCW1Client()
        result = client.fetch_all("01/01/2024", "01/31/2024")
        
        assert result["success"] is True
        assert result["count"] == 5
        assert result["method"] == "requests"
        assert len(result["items"]) == 2
        mock_requests_engine.assert_called_once()
        mock_engine_instance.fetch_all.assert_called_once_with("01/01/2024", "01/31/2024", None)
    
    @patch('services.scraper.rrc_w1.PlaywrightEngine')
    @patch('services.scraper.rrc_w1.RequestsEngine')
    def test_fetch_all_fallback_to_playwright(self, mock_requests_engine, mock_playwright_engine):
        """Test fetch_all method falling back to PlaywrightEngine when RequestsEngine fails."""
        from services.scraper.rrc_w1 import EngineRedirectToLogin
        
        # Mock RequestsEngine to raise EngineRedirectToLogin
        mock_requests_instance = MagicMock()
        mock_requests_instance.fetch_all.side_effect = EngineRedirectToLogin("Redirected to login")
        mock_requests_engine.return_value = mock_requests_instance
        
        # Mock PlaywrightEngine to succeed
        mock_playwright_instance = MagicMock()
        mock_playwright_instance.fetch_all.return_value = {
            "source_root": "https://webapps.rrc.state.tx.us",
            "query_params": {"begin": "01/01/2024", "end": "01/31/2024"},
            "pages": 1,
            "count": 3,
            "items": [
                {"status_no": "12347", "operator_name": "Test Oil Co 3"}
            ],
            "fetched_at": "2024-01-15T10:00:00Z",
            "method": "playwright",
            "success": True
        }
        mock_playwright_engine.return_value = mock_playwright_instance
        
        client = RRCW1Client()
        result = client.fetch_all("01/01/2024", "01/31/2024")
        
        assert result["success"] is True
        assert result["count"] == 3
        assert result["method"] == "playwright"
        assert len(result["items"]) == 1
        
        # Verify both engines were called
        mock_requests_engine.assert_called_once()
        mock_playwright_engine.assert_called_once()
    
    @patch('services.scraper.rrc_w1.PlaywrightEngine')
    @patch('services.scraper.rrc_w1.RequestsEngine')
    def test_fetch_all_both_engines_fail(self, mock_requests_engine, mock_playwright_engine):
        """Test fetch_all method when both engines fail."""
        # Mock both engines to fail
        mock_requests_instance = MagicMock()
        mock_requests_instance.fetch_all.side_effect = Exception("Requests engine failed")
        mock_requests_engine.return_value = mock_requests_instance
        
        mock_playwright_instance = MagicMock()
        mock_playwright_instance.fetch_all.side_effect = Exception("Playwright engine failed")
        mock_playwright_engine.return_value = mock_playwright_instance
        
        client = RRCW1Client()
        result = client.fetch_all("01/01/2024", "01/31/2024")
        
        assert result["success"] is False
        assert result["count"] == 0
        assert "error" in result
        assert "Playwright engine failed" in result["error"]
    
    def test_fetch_all_with_max_pages(self):
        """Test fetch_all method with max_pages parameter."""
        with patch('services.scraper.rrc_w1.RequestsEngine') as mock_requests_engine:
            mock_engine_instance = MagicMock()
            mock_engine_instance.fetch_all.return_value = {
                "source_root": "https://webapps.rrc.state.tx.us",
                "query_params": {"begin": "01/01/2024", "end": "01/31/2024"},
                "pages": 2,
                "count": 10,
                "items": [],
                "fetched_at": "2024-01-15T10:00:00Z",
                "method": "requests",
                "success": True
            }
            mock_requests_engine.return_value = mock_engine_instance
            
            client = RRCW1Client()
            result = client.fetch_all("01/01/2024", "01/31/2024", max_pages=2)
            
            assert result["success"] is True
            mock_engine_instance.fetch_all.assert_called_once_with("01/01/2024", "01/31/2024", 2)


class TestRequestsEngine:
    """Test cases for RequestsEngine."""
    
    def test_requests_engine_initialization(self):
        """Test that RequestsEngine initializes correctly."""
        engine = RequestsEngine()
        
        assert engine.base_url == "https://webapps.rrc.state.tx.us"
        assert engine.dp_base == "https://webapps.rrc.state.tx.us/DP"
        assert engine.timeout == 30
        assert "PermitTrackerBot" in engine.user_agent
    
    def test_requests_engine_with_custom_params(self):
        """Test RequestsEngine with custom parameters."""
        engine = RequestsEngine(base_url="https://test.example.com", timeout=60)
        
        assert engine.base_url == "https://test.example.com"
        assert engine.dp_base == "https://test.example.com/DP"
        assert engine.timeout == 60


class TestPlaywrightEngine:
    """Test cases for PlaywrightEngine."""
    
    def test_playwright_engine_initialization(self):
        """Test that PlaywrightEngine initializes correctly."""
        engine = PlaywrightEngine()
        
        assert engine.base_url == "https://webapps.rrc.state.tx.us"
        assert engine.dp_base == "https://webapps.rrc.state.tx.us/DP"
        assert engine.timeout == 30000  # Default timeout in milliseconds
    
    def test_playwright_engine_with_custom_params(self):
        """Test PlaywrightEngine with custom parameters."""
        engine = PlaywrightEngine(base_url="https://test.example.com", timeout=60000)
        
        assert engine.base_url == "https://test.example.com"
        assert engine.dp_base == "https://test.example.com/DP"
        assert engine.timeout == 60000


if __name__ == "__main__":
    pytest.main([__file__])
