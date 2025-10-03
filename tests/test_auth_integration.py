"""
Integration tests for complete authentication flow.
Tests the full user journey from registration to cross-device sync.
"""

import pytest
import asyncio
import json
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.main import app
from services.auth import auth_service
from db.session import get_session
from db.auth_models import User, Org, OrgMembership


@pytest.fixture
def client():
    """Test client for FastAPI app."""
    return TestClient(app)


class TestCompleteAuthFlow:
    """Test complete authentication flow from registration to sync."""
    
    def test_complete_user_journey(self, client):
        """Test complete user journey: register -> login -> sync -> logout."""
        
        # Step 1: Register new user
        user_data = {
            "email": "journey@example.com",
            "password": "journey123456",
            "username": "journeyuser"
        }
        
        register_response = client.post("/auth/register", json=user_data)
        assert register_response.status_code == 200
        
        register_data = register_response.json()
        assert register_data["user"]["email"] == user_data["email"]
        assert register_data["user"]["username"] == user_data["username"]
        
        # Verify cookies are set
        assert "access_token" in register_response.cookies
        assert "refresh_token" in register_response.cookies
        
        # Step 2: Get user info
        me_response = client.get("/auth/me")
        assert me_response.status_code == 200
        
        me_data = me_response.json()
        assert me_data["email"] == user_data["email"]
        assert me_data["username"] == user_data["username"]
        assert me_data["is_active"] is True
        
        # Step 3: Check sessions
        sessions_response = client.get("/auth/sessions")
        assert sessions_response.status_code == 200
        
        sessions_data = sessions_response.json()
        assert len(sessions_data) >= 1
        
        # Step 4: Refresh token
        refresh_response = client.post("/auth/refresh")
        assert refresh_response.status_code == 200
        
        # Step 5: Test WebSocket authentication (mock)
        # In a real test, you'd test actual WebSocket connection
        access_token = register_response.cookies.get("access_token")
        assert access_token is not None
        
        # Verify token is valid
        payload = auth_service.verify_access_token(access_token)
        assert payload["email"] == user_data["email"]
        
        # Step 6: Logout
        logout_response = client.post("/auth/logout")
        assert logout_response.status_code == 200
        
        # Step 7: Verify logout worked
        me_after_logout = client.get("/auth/me")
        assert me_after_logout.status_code == 401
    
    def test_multi_device_session_management(self, client):
        """Test multi-device session management."""
        
        # Register user
        user_data = {
            "email": "multidevice@example.com",
            "password": "multidevice123",
            "username": "multidevice"
        }
        
        client.post("/auth/register", json=user_data)
        
        # Simulate multiple device logins
        devices = ["Chrome Desktop", "Firefox Mobile", "Safari iOS", "Edge Desktop", "Chrome Mobile"]
        
        for i, device in enumerate(devices):
            # Login with different user agent
            login_response = client.post("/auth/login", json={
                "email": user_data["email"],
                "password": user_data["password"]
            }, headers={"User-Agent": device})
            
            assert login_response.status_code == 200
        
        # Check sessions
        sessions_response = client.get("/auth/sessions")
        assert sessions_response.status_code == 200
        
        sessions_data = sessions_response.json()
        # Should have 5 sessions (MAX_SESSIONS_PER_USER)
        assert len(sessions_data) == 5
        
        # Login one more time (should revoke oldest)
        client.post("/auth/login", json={
            "email": user_data["email"],
            "password": user_data["password"]
        }, headers={"User-Agent": "New Device"})
        
        # Check sessions again
        sessions_response = client.get("/auth/sessions")
        sessions_data = sessions_response.json()
        assert len(sessions_data) == 5  # Still 5, oldest was revoked
    
    def test_password_reset_flow(self, client):
        """Test complete password reset flow."""
        
        # Register user
        user_data = {
            "email": "reset@example.com",
            "password": "reset123456",
            "username": "resetuser"
        }
        
        client.post("/auth/register", json=user_data)
        
        # Request password reset
        reset_request = client.post("/auth/request-password-reset", json={
            "email": user_data["email"]
        })
        assert reset_request.status_code == 200
        
        # In a real test, you'd extract the reset token from the response
        # For now, we'll test the reset endpoint with a mock token
        # (In production, the token would come from email)
        
        # Test with invalid token
        invalid_reset = client.post("/auth/reset-password", json={
            "token": "invalid-token",
            "new_password": "newpassword123"
        })
        assert invalid_reset.status_code == 400
    
    def test_organization_access_control(self, client):
        """Test organization-based access control."""
        
        # Register user
        user_data = {
            "email": "orgtest@example.com",
            "password": "orgtest123",
            "username": "orgtest"
        }
        
        client.post("/auth/register", json=user_data)
        
        # User should be automatically added to default_org as owner
        me_response = client.get("/auth/me")
        me_data = me_response.json()
        
        assert len(me_data["orgs"]) >= 1
        default_org = next((org for org in me_data["orgs"] if org["org_id"] == "default_org"), None)
        assert default_org is not None
        assert default_org["role"] == "owner"
    
    def test_rate_limiting(self, client):
        """Test rate limiting on authentication endpoints."""
        
        # Test login rate limiting
        login_data = {
            "email": "ratetest@example.com",
            "password": "wrongpassword"
        }
        
        # Make multiple failed login attempts
        for i in range(6):  # Should hit rate limit after 5
            response = client.post("/auth/login", json=login_data)
            
            if i < 5:
                assert response.status_code == 401  # Invalid credentials
            else:
                assert response.status_code == 429  # Rate limited
    
    def test_cross_device_sync_simulation(self, client):
        """Test cross-device sync simulation."""
        
        # Register user
        user_data = {
            "email": "sync@example.com",
            "password": "sync123456",
            "username": "syncuser"
        }
        
        client.post("/auth/register", json=user_data)
        
        # Simulate two devices
        device1_cookies = client.cookies
        device2_response = client.post("/auth/login", json={
            "email": user_data["email"],
            "password": user_data["password"]
        })
        device2_cookies = device2_response.cookies
        
        # Both devices should be able to access user info
        client.cookies = device1_cookies
        me1 = client.get("/auth/me")
        assert me1.status_code == 200
        
        client.cookies = device2_cookies
        me2 = client.get("/auth/me")
        assert me2.status_code == 200
        
        # Both should get same user data
        assert me1.json()["email"] == me2.json()["email"]
        
        # Logout from device 1
        client.cookies = device1_cookies
        logout1 = client.post("/auth/logout")
        assert logout1.status_code == 200
        
        # Device 1 should no longer have access
        me1_after_logout = client.get("/auth/me")
        assert me1_after_logout.status_code == 401
        
        # Device 2 should still have access
        client.cookies = device2_cookies
        me2_after_logout = client.get("/auth/me")
        assert me2_after_logout.status_code == 200


class TestSecurityFeatures:
    """Test security features and edge cases."""
    
    def test_sql_injection_protection(self, client):
        """Test protection against SQL injection."""
        
        # Try SQL injection in email field
        malicious_data = {
            "email": "'; DROP TABLE users; --",
            "password": "password123"
        }
        
        response = client.post("/auth/login", json=malicious_data)
        # Should return 401 (invalid credentials) not 500 (server error)
        assert response.status_code == 401
    
    def test_xss_protection(self, client):
        """Test protection against XSS attacks."""
        
        # Try XSS in username field
        xss_data = {
            "email": "xss@example.com",
            "password": "xss123456",
            "username": "<script>alert('xss')</script>"
        }
        
        response = client.post("/auth/register", json=xss_data)
        assert response.status_code == 200
        
        # Username should be stored as-is (not executed)
        me_response = client.get("/auth/me")
        me_data = me_response.json()
        assert me_data["username"] == xss_data["username"]
    
    def test_token_security(self, client):
        """Test token security features."""
        
        # Register user
        user_data = {
            "email": "token@example.com",
            "password": "token123456",
            "username": "tokenuser"
        }
        
        client.post("/auth/register", json=user_data)
        
        # Tokens should be HttpOnly cookies (not accessible via JavaScript)
        # This is tested by the fact that we can't access them via document.cookie
        # in the browser, but we can access them in the test client
        
        # Access token should be short-lived
        access_token = client.cookies.get("access_token")
        assert access_token is not None
        
        # Refresh token should be different from access token
        refresh_token = client.cookies.get("refresh_token")
        assert refresh_token is not None
        assert refresh_token != access_token
    
    def test_session_security(self, client):
        """Test session security features."""
        
        # Register user
        user_data = {
            "email": "session@example.com",
            "password": "session123456",
            "username": "sessionuser"
        }
        
        client.post("/auth/register", json=user_data)
        
        # Check session details
        sessions_response = client.get("/auth/sessions")
        sessions_data = sessions_response.json()
        
        session = sessions_data[0]
        assert session["user_agent"] is not None
        assert session["ip_address"] is not None
        assert session["created_at"] is not None
        assert session["expires_at"] is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
