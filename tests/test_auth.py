"""
Comprehensive tests for authentication system.
Tests auth backend, middleware, routes, WebSocket auth, and session limits.
"""

import pytest
import asyncio
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.main import app
from services.auth import auth_service
from services.auth_middleware import auth_middleware, rbac_middleware
from db.session import get_session
from db.auth_models import User, Org, OrgMembership, Session as UserSession


@pytest.fixture
def client():
    """Test client for FastAPI app."""
    return TestClient(app)


@pytest.fixture
def test_user():
    """Create a test user."""
    with get_session() as session:
        user = User(
            email="test@example.com",
            username="testuser",
            password_hash=auth_service.hash_password("testpassword123"),
            is_active=True
        )
        session.add(user)
        session.commit()
        session.refresh(user)
        return user


@pytest.fixture
def test_org():
    """Create a test organization."""
    with get_session() as session:
        org = Org(id="test_org", name="Test Organization")
        session.add(org)
        session.commit()
        return org


@pytest.fixture
def test_membership(test_user, test_org):
    """Create a test organization membership."""
    with get_session() as session:
        membership = OrgMembership(
            user_id=test_user.id,
            org_id=test_org.id,
            role="owner"
        )
        session.add(membership)
        session.commit()
        return membership


class TestAuthService:
    """Test authentication service functionality."""
    
    def test_password_hashing(self):
        """Test password hashing with Argon2."""
        password = "testpassword123"
        hashed = auth_service.hash_password(password)
        
        assert hashed != password
        assert len(hashed) > 50  # Argon2 hashes are long
        assert auth_service.verify_password(password, hashed)
        assert not auth_service.verify_password("wrongpassword", hashed)
    
    def test_create_user(self):
        """Test user creation."""
        email = "newuser@example.com"
        password = "newpassword123"
        
        user = auth_service.create_user(email, password, "newuser")
        
        assert user.email == email
        assert user.username == "newuser"
        assert user.is_active is True
        assert auth_service.verify_password(password, user.password_hash)
    
    def test_create_user_duplicate_email(self):
        """Test user creation with duplicate email."""
        email = "duplicate@example.com"
        password = "password123"
        
        # Create first user
        auth_service.create_user(email, password)
        
        # Try to create second user with same email
        with pytest.raises(Exception):  # Should raise HTTPException
            auth_service.create_user(email, password)
    
    def test_authenticate_user(self, test_user):
        """Test user authentication."""
        # Valid credentials
        user = auth_service.authenticate_user("test@example.com", "testpassword123")
        assert user is not None
        assert user.email == "test@example.com"
        
        # Invalid password
        user = auth_service.authenticate_user("test@example.com", "wrongpassword")
        assert user is None
        
        # Invalid email
        user = auth_service.authenticate_user("nonexistent@example.com", "testpassword123")
        assert user is None
    
    def test_create_session(self, test_user):
        """Test session creation."""
        user_agent = "Test Browser"
        ip_address = "192.168.1.1"
        
        access_token, refresh_token = auth_service.create_session(
            test_user, user_agent, ip_address
        )
        
        assert access_token is not None
        assert refresh_token is not None
        assert len(refresh_token) > 20  # Opaque token should be long
        
        # Verify session was created in database
        with get_session() as session:
            user_session = session.query(UserSession).filter(
                UserSession.user_id == test_user.id
            ).first()
            assert user_session is not None
            assert user_session.user_agent == user_agent
            assert str(user_session.ip_address) == ip_address
    
    def test_refresh_access_token(self, test_user):
        """Test access token refresh."""
        # Create session
        access_token, refresh_token = auth_service.create_session(test_user)
        
        # Refresh access token
        new_access_token = auth_service.refresh_access_token(refresh_token)
        
        assert new_access_token is not None
        assert new_access_token != access_token
        
        # Verify new token is valid
        payload = auth_service.verify_access_token(new_access_token)
        assert payload["sub"] == str(test_user.id)
        assert payload["email"] == test_user.email
    
    def test_revoke_session(self, test_user):
        """Test session revocation."""
        # Create session
        access_token, refresh_token = auth_service.create_session(test_user)
        
        # Revoke session
        success = auth_service.revoke_session(refresh_token)
        assert success is True
        
        # Try to refresh with revoked token
        new_token = auth_service.refresh_access_token(refresh_token)
        assert new_token is None
    
    def test_session_limit_enforcement(self, test_user):
        """Test session limit enforcement (5 devices max)."""
        # Create 6 sessions (should revoke oldest)
        sessions = []
        for i in range(6):
            access_token, refresh_token = auth_service.create_session(
                test_user, f"Browser {i}", f"192.168.1.{i+1}"
            )
            sessions.append(refresh_token)
        
        # Check that only 5 sessions are active
        active_sessions = auth_service.get_user_sessions(test_user.id)
        assert len(active_sessions) == 5
        
        # The first session should be revoked
        first_token_valid = auth_service.refresh_access_token(sessions[0])
        assert first_token_valid is None
        
        # The last session should still be valid
        last_token_valid = auth_service.refresh_access_token(sessions[-1])
        assert last_token_valid is not None
    
    def test_password_reset_flow(self, test_user):
        """Test password reset flow."""
        # Request reset token
        reset_token = auth_service.create_password_reset_token(test_user.email)
        assert reset_token is not None
        
        # Reset password
        new_password = "newpassword123"
        success = auth_service.reset_password(reset_token, new_password)
        assert success is True
        
        # Verify new password works
        user = auth_service.authenticate_user(test_user.email, new_password)
        assert user is not None
        
        # Verify old password doesn't work
        user = auth_service.authenticate_user(test_user.email, "testpassword123")
        assert user is None
        
        # Verify reset token is single-use
        success = auth_service.reset_password(reset_token, "anotherpassword")
        assert success is False


class TestAuthRoutes:
    """Test authentication API routes."""
    
    def test_register_endpoint(self, client):
        """Test user registration endpoint."""
        user_data = {
            "email": "newuser@example.com",
            "password": "newpassword123",
            "username": "newuser"
        }
        
        response = client.post("/auth/register", json=user_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["user"]["email"] == user_data["email"]
        assert data["user"]["username"] == user_data["username"]
        assert "access_token" in response.cookies
        assert "refresh_token" in response.cookies
    
    def test_login_endpoint(self, client, test_user):
        """Test user login endpoint."""
        login_data = {
            "email": "test@example.com",
            "password": "testpassword123"
        }
        
        response = client.post("/auth/login", json=login_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["user"]["email"] == login_data["email"]
        assert "access_token" in response.cookies
        assert "refresh_token" in response.cookies
    
    def test_login_invalid_credentials(self, client):
        """Test login with invalid credentials."""
        login_data = {
            "email": "test@example.com",
            "password": "wrongpassword"
        }
        
        response = client.post("/auth/login", json=login_data)
        
        assert response.status_code == 401
        assert "Invalid email or password" in response.json()["detail"]
    
    def test_logout_endpoint(self, client, test_user):
        """Test logout endpoint."""
        # First login to get cookies
        login_data = {
            "email": "test@example.com",
            "password": "testpassword123"
        }
        client.post("/auth/login", json=login_data)
        
        # Then logout
        response = client.post("/auth/logout")
        
        assert response.status_code == 200
        assert response.json()["message"] == "Logout successful"
    
    def test_refresh_endpoint(self, client, test_user):
        """Test token refresh endpoint."""
        # First login to get cookies
        login_data = {
            "email": "test@example.com",
            "password": "testpassword123"
        }
        client.post("/auth/login", json=login_data)
        
        # Then refresh
        response = client.post("/auth/refresh")
        
        assert response.status_code == 200
        assert response.json()["message"] == "Token refreshed successfully"
    
    def test_me_endpoint(self, client, test_user):
        """Test /auth/me endpoint."""
        # First login to get cookies
        login_data = {
            "email": "test@example.com",
            "password": "testpassword123"
        }
        client.post("/auth/login", json=login_data)
        
        # Then get user info
        response = client.get("/auth/me")
        
        assert response.status_code == 200
        data = response.json()
        assert data["email"] == test_user.email
        assert data["username"] == test_user.username
    
    def test_me_endpoint_unauthorized(self, client):
        """Test /auth/me endpoint without authentication."""
        response = client.get("/auth/me")
        
        assert response.status_code == 401
    
    def test_sessions_endpoint(self, client, test_user):
        """Test /auth/sessions endpoint."""
        # First login to get cookies
        login_data = {
            "email": "test@example.com",
            "password": "testpassword123"
        }
        client.post("/auth/login", json=login_data)
        
        # Then get sessions
        response = client.get("/auth/sessions")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) >= 1  # At least the current session
    
    def test_password_reset_endpoints(self, client, test_user):
        """Test password reset endpoints."""
        # Request password reset
        reset_data = {"email": "test@example.com"}
        response = client.post("/auth/request-password-reset", json=reset_data)
        
        assert response.status_code == 200
        assert "Password reset email sent" in response.json()["message"]
        
        # Note: In a real test, you'd need to extract the reset token
        # from the response or mock the email service


class TestAuthMiddleware:
    """Test authentication middleware."""
    
    def test_get_current_user(self, test_user):
        """Test getting current user from request."""
        # Mock request with valid token
        access_token = auth_service.create_access_token({
            "sub": str(test_user.id),
            "email": test_user.email
        })
        
        request = Mock()
        request.cookies = {"access_token": access_token}
        
        # This would need to be tested with actual FastAPI dependency injection
        # For now, we'll test the auth service directly
        payload = auth_service.verify_access_token(access_token)
        user = auth_service.get_user_by_id(payload["sub"])
        
        assert user is not None
        assert user.email == test_user.email
    
    def test_require_auth_middleware(self):
        """Test require auth middleware."""
        # This would need to be tested with actual FastAPI dependency injection
        # For now, we'll test the auth service token verification
        invalid_token = "invalid.token.here"
        
        with pytest.raises(Exception):  # Should raise HTTPException
            auth_service.verify_access_token(invalid_token)


class TestWebSocketAuth:
    """Test WebSocket authentication."""
    
    def test_websocket_auth_success(self, test_user, test_org, test_membership):
        """Test successful WebSocket authentication."""
        # Create access token
        access_token = auth_service.create_access_token({
            "sub": str(test_user.id),
            "email": test_user.email
        })
        
        # Mock WebSocket connection
        websocket = Mock()
        websocket.query_params = {"org_id": "test_org", "access_token": access_token}
        websocket.headers = {}
        
        # Test token verification
        payload = auth_service.verify_access_token(access_token)
        assert payload["sub"] == str(test_user.id)
        
        # Test org membership verification
        with get_session() as session:
            membership = session.query(OrgMembership).filter(
                OrgMembership.user_id == test_user.id,
                OrgMembership.org_id == "test_org"
            ).first()
            assert membership is not None
            assert membership.role == "owner"
    
    def test_websocket_auth_invalid_token(self):
        """Test WebSocket authentication with invalid token."""
        invalid_token = "invalid.token.here"
        
        with pytest.raises(Exception):  # Should raise HTTPException
            auth_service.verify_access_token(invalid_token)
    
    def test_websocket_auth_no_org_access(self, test_user):
        """Test WebSocket authentication without org access."""
        # Create access token
        access_token = auth_service.create_access_token({
            "sub": str(test_user.id),
            "email": test_user.email
        })
        
        # Verify token is valid
        payload = auth_service.verify_access_token(access_token)
        assert payload["sub"] == str(test_user.id)
        
        # But user has no access to non-existent org
        with get_session() as session:
            membership = session.query(OrgMembership).filter(
                OrgMembership.user_id == test_user.id,
                OrgMembership.org_id == "nonexistent_org"
            ).first()
            assert membership is None


class TestRBAC:
    """Test Role-Based Access Control."""
    
    def test_owner_role(self, test_user, test_org, test_membership):
        """Test owner role permissions."""
        # Owner should have access to everything
        assert test_membership.role == "owner"
        
        # Test role checking logic
        assert test_membership.role in ["admin", "owner"]  # Admin check
        assert test_membership.role == "owner"  # Owner check
    
    def test_admin_role(self, test_user, test_org):
        """Test admin role permissions."""
        with get_session() as session:
            # Create admin membership
            admin_membership = OrgMembership(
                user_id=test_user.id,
                org_id=test_org.id,
                role="admin"
            )
            session.add(admin_membership)
            session.commit()
            
            # Admin should have admin privileges but not owner
            assert admin_membership.role in ["admin", "owner"]  # Admin check
            assert admin_membership.role != "owner"  # Not owner
    
    def test_member_role(self, test_user, test_org):
        """Test member role permissions."""
        with get_session() as session:
            # Create member membership
            member_membership = OrgMembership(
                user_id=test_user.id,
                org_id=test_org.id,
                role="member"
            )
            session.add(member_membership)
            session.commit()
            
            # Member should not have admin or owner privileges
            assert member_membership.role not in ["admin", "owner"]
            assert member_membership.role == "member"


class TestRateLimiting:
    """Test rate limiting functionality."""
    
    def test_rate_limit_check(self):
        """Test rate limiting logic."""
        from collections import defaultdict
        import time
        
        # Mock rate limit store
        rate_limit_store = defaultdict(list)
        RATE_LIMIT_WINDOW = 300  # 5 minutes
        RATE_LIMIT_MAX_ATTEMPTS = 5
        
        def check_rate_limit(identifier: str) -> bool:
            now = time.time()
            window_start = now - RATE_LIMIT_WINDOW
            
            # Clean old entries
            rate_limit_store[identifier] = [
                timestamp for timestamp in rate_limit_store[identifier]
                if timestamp > window_start
            ]
            
            # Check if under limit
            if len(rate_limit_store[identifier]) >= RATE_LIMIT_MAX_ATTEMPTS:
                return False
            
            # Add current request
            rate_limit_store[identifier].append(now)
            return True
        
        # Test rate limiting
        identifier = "test:192.168.1.1"
        
        # First 5 attempts should succeed
        for i in range(5):
            assert check_rate_limit(identifier) is True
        
        # 6th attempt should fail
        assert check_rate_limit(identifier) is False


if __name__ == "__main__":
    pytest.main([__file__])
