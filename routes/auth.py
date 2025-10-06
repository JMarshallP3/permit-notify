"""
Authentication routes for user registration, login, logout, and session management.
"""

import os
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from fastapi import APIRouter, HTTPException, status, Depends, Request, Response, Query
from fastapi.security import HTTPBearer
from pydantic import BaseModel, EmailStr, validator
from sqlalchemy.orm import Session

from services.auth import auth_service
from services.auth_middleware import require_auth, get_auth_context, AuthContext, require_authenticated_user
from db.session import get_session
from db.auth_models import User, OrgMembership, Session as UserSession, Org


# Configuration
AUTH_COOKIE_DOMAIN = os.getenv("AUTH_COOKIE_DOMAIN", "localhost")
AUTH_COOKIE_SECURE = os.getenv("AUTH_COOKIE_SECURE", "false").lower() == "true"

# Router
router = APIRouter(prefix="/auth", tags=["authentication"])

# Security
security = HTTPBearer(auto_error=False)


# Pydantic models
class UserRegister(BaseModel):
    email: EmailStr
    password: str
    username: Optional[str] = None
    
    @validator('password')
    def validate_password(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        return v


class UserLogin(BaseModel):
    email: EmailStr
    password: str


class PasswordResetRequest(BaseModel):
    email: EmailStr


class PasswordReset(BaseModel):
    token: str
    new_password: str
    
    @validator('new_password')
    def validate_password(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        return v


class UserResponse(BaseModel):
    id: str
    email: str
    username: Optional[str]
    is_active: bool
    created_at: datetime
    orgs: List[Dict[str, Any]]


class SessionResponse(BaseModel):
    id: str
    user_agent: Optional[str]
    ip_address: Optional[str]
    created_at: datetime
    expires_at: datetime
    is_current: bool = False


class LoginResponse(BaseModel):
    user: UserResponse
    message: str


# Rate limiting (simple in-memory implementation)
from collections import defaultdict
from datetime import timedelta
import time

rate_limit_store = defaultdict(list)
RATE_LIMIT_WINDOW = 300  # 5 minutes
RATE_LIMIT_MAX_ATTEMPTS = 5


def check_rate_limit(identifier: str) -> bool:
    """Check if request is within rate limit."""
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


def set_cookies(response: Response, access_token: str, refresh_token: str):
    """Set authentication cookies."""
    # Access token cookie (short-lived)
    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        secure=AUTH_COOKIE_SECURE,
        samesite="lax",
        domain=AUTH_COOKIE_DOMAIN if AUTH_COOKIE_DOMAIN != "localhost" else None,
        max_age=900  # 15 minutes
    )
    
    # Refresh token cookie (long-lived)
    response.set_cookie(
        key="refresh_token",
        value=refresh_token,
        httponly=True,
        secure=AUTH_COOKIE_SECURE,
        samesite="lax",
        domain=AUTH_COOKIE_DOMAIN if AUTH_COOKIE_DOMAIN != "localhost" else None,
        max_age=2592000  # 30 days
    )


def clear_cookies(response: Response):
    """Clear authentication cookies."""
    response.delete_cookie(
        key="access_token",
        domain=AUTH_COOKIE_DOMAIN if AUTH_COOKIE_DOMAIN != "localhost" else None
    )
    response.delete_cookie(
        key="refresh_token",
        domain=AUTH_COOKIE_DOMAIN if AUTH_COOKIE_DOMAIN != "localhost" else None
    )


@router.post("/register", response_model=LoginResponse)
async def register(
    user_data: UserRegister,
    request: Request,
    response: Response
):
    """Register new user account."""
    # Rate limiting
    client_ip = request.client.host
    if not check_rate_limit(f"register:{client_ip}"):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many registration attempts"
        )
    
    try:
        # Create user with proper error handling
        user_id = None
        try:
            user_id = auth_service.create_user(
                email=user_data.email,
                password=user_data.password,
                username=user_data.username
            )
        except HTTPException as http_exc:
            # Re-raise HTTP exceptions directly (they have proper status codes)
            raise http_exc
        except Exception as e:
            # Handle unexpected errors
            error_msg = str(e)
            if "duplicate key" in error_msg.lower() or "unique constraint" in error_msg.lower():
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Email or username already exists"
                )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create user account: {error_msg}"
            )
        
        # Create default org membership using user_id string
        try:
            with get_session() as session:
                # Ensure default org exists
                default_org = session.query(Org).filter(Org.id == "default_org").first()
                if not default_org:
                    default_org = Org(id="default_org", name="Default Organization")
                    session.add(default_org)
                    session.commit()
                
                # Create membership as owner using user_id string
                membership = OrgMembership(
                    user_id=user_id,
                    org_id="default_org",
                    role="owner"
                )
                session.add(membership)
                session.commit()
        except Exception as e:
            # Enhanced error logging
            import traceback
            error_details = f"Org membership error: {str(e)} | User ID: {user_id} | Traceback: {traceback.format_exc()}"
            
            # If org creation fails, we should clean up the user
            try:
                with get_session() as cleanup_session:
                    user_to_delete = cleanup_session.query(User).filter(User.id == user_id).first()
                    if user_to_delete:
                        cleanup_session.delete(user_to_delete)
                        cleanup_session.commit()
            except:
                pass  # Best effort cleanup
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create organization membership: {error_details}"
            )
        
        # Create session and return user data
        try:
            # Get user object for session creation and response
            with get_session() as session:
                user = session.query(User).filter(User.id == user_id).first()
                if not user:
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="User not found after creation"
                    )
                
                user_agent = request.headers.get("user-agent")
                ip_address = request.client.host
                access_token, refresh_token = auth_service.create_session(
                    user, user_agent, ip_address
                )
                
                # Set cookies
                set_cookies(response, access_token, refresh_token)
                
                # Get user orgs
                user_orgs = auth_service.get_user_orgs(user.id)
                
                return LoginResponse(
                    user=UserResponse(
                        id=str(user.id),
                        email=user.email,
                        username=user.username,
                        is_active=user.is_active,
                        created_at=user.created_at,
                        orgs=user_orgs
                    ),
                    message="Registration successful"
                )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create user session: {str(e)}"
            )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Registration failed"
        )


@router.post("/login", response_model=LoginResponse)
async def login(
    login_data: UserLogin,
    request: Request,
    response: Response
):
    """Login user and create session."""
    # Rate limiting
    client_ip = request.client.host
    if not check_rate_limit(f"login:{client_ip}"):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many login attempts"
        )
    
    # Authenticate user
    user = auth_service.authenticate_user(login_data.email, login_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password"
        )
    
    # Create session
    user_agent = request.headers.get("user-agent")
    ip_address = request.client.host
    access_token, refresh_token = auth_service.create_session(
        user, user_agent, ip_address
    )
    
    # Set cookies
    set_cookies(response, access_token, refresh_token)
    
    # Get user orgs
    user_orgs = auth_service.get_user_orgs(user.id)
    
    return LoginResponse(
        user=UserResponse(
            id=str(user.id),
            email=user.email,
            username=user.username,
            is_active=user.is_active,
            created_at=user.created_at,
            orgs=user_orgs
        ),
        message="Login successful"
    )


@router.post("/logout")
async def logout(
    request: Request,
    response: Response,
    current_user: User = Depends(require_auth)
):
    """Logout user and revoke session."""
    refresh_token = request.cookies.get("refresh_token")
    if refresh_token:
        auth_service.revoke_session(refresh_token)
    
    # Clear cookies
    clear_cookies(response)
    
    return {"message": "Logout successful"}


@router.post("/logout-all")
async def logout_all(
    response: Response,
    auth_context: AuthContext = Depends(get_auth_context)
):
    """Logout user from all devices."""
    revoked_count = auth_service.revoke_all_sessions(auth_context.user_id)
    
    # Clear cookies
    clear_cookies(response)
    
    return {
        "message": f"Logged out from {revoked_count} devices",
        "revoked_sessions": revoked_count
    }


@router.post("/refresh")
async def refresh_token(
    request: Request,
    response: Response
):
    """Refresh access token using refresh token."""
    refresh_token = request.cookies.get("refresh_token")
    if not refresh_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token not found"
        )
    
    # Refresh access token
    access_token = auth_service.refresh_access_token(refresh_token)
    if not access_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token"
        )
    
    # Set new access token cookie
    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        secure=AUTH_COOKIE_SECURE,
        samesite="lax",
        domain=AUTH_COOKIE_DOMAIN if AUTH_COOKIE_DOMAIN != "localhost" else None,
        max_age=900  # 15 minutes
    )
    
    return {"message": "Token refreshed successfully"}


@router.get("/me", response_model=UserResponse)
async def get_current_user_info(
    user: User = Depends(require_authenticated_user)
):
    """Get current user information."""
    try:
        user_orgs = auth_service.get_user_orgs(user.id)
    except Exception as e:
        user_orgs = []  # Default to empty if org lookup fails
    
    return UserResponse(
        id=str(user.id),
        email=user.email,
        username=user.username,
        is_active=user.is_active,
        created_at=user.created_at,
        orgs=user_orgs
    )


@router.get("/sessions", response_model=List[SessionResponse])
async def get_user_sessions(
    auth_context: AuthContext = Depends(get_auth_context)
):
    """Get user's active sessions."""
    sessions = auth_service.get_user_sessions(auth_context.user_id)
    current_refresh_token = None  # We'd need to pass this from the request
    
    return [
        SessionResponse(
            id=str(session.id),
            user_agent=session.user_agent,
            ip_address=str(session.ip_address) if session.ip_address else None,
            created_at=session.created_at,
            expires_at=session.expires_at,
            is_current=False  # Would need to compare with current session
        )
        for session in sessions
    ]


@router.delete("/sessions/{session_id}")
async def revoke_session(
    session_id: str,
    auth_context: AuthContext = Depends(get_auth_context)
):
    """Revoke a specific session."""
    with get_session() as session:
        user_session = session.query(UserSession).filter(
            UserSession.id == session_id,
            UserSession.user_id == auth_context.user_id
        ).first()
        
        if not user_session:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Session not found"
            )
        
        user_session.revoked_at = datetime.now(timezone.utc)
        session.commit()
        
        return {"message": "Session revoked successfully"}


@router.post("/request-password-reset")
async def request_password_reset(
    reset_data: PasswordResetRequest,
    request: Request
):
    """Request password reset token."""
    # Rate limiting
    client_ip = request.client.host
    if not check_rate_limit(f"reset:{client_ip}"):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many password reset attempts"
        )
    
    # Create reset token
    reset_token = auth_service.create_password_reset_token(reset_data.email)
    
    # In production, send email with reset token
    # For now, just return success (token would be sent via email)
    if reset_token:
        # Log the token for development (remove in production)
        print(f"Password reset token for {reset_data.email}: {reset_token}")
        
        return {
            "message": "Password reset email sent",
            "note": "In development, check server logs for reset token"
        }
    else:
        # Don't reveal if email exists or not
        return {
            "message": "If the email exists, a password reset email has been sent"
        }


@router.post("/reset-password")
async def reset_password(
    reset_data: PasswordReset,
    request: Request
):
    """Reset password using reset token."""
    # Rate limiting
    client_ip = request.client.host
    if not check_rate_limit(f"reset_password:{client_ip}"):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many password reset attempts"
        )
    
    # Reset password
    success = auth_service.reset_password(reset_data.token, reset_data.new_password)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired reset token"
        )
    
    return {"message": "Password reset successfully"}


@router.get("/health")
async def auth_health():
    """Health check for auth service."""
    return {
        "status": "healthy",
        "service": "authentication",
        "features": {
            "totp_2fa": os.getenv("FEATURE_TOTP_2FA", "false").lower() == "true",
            "webauthn": os.getenv("FEATURE_WEBAUTHN", "false").lower() == "true"
        }
    }
