"""
Authentication backend with password hashing, JWT tokens, and session management.
"""

import os
import secrets
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List
from uuid import uuid4

from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy.orm import Session
from fastapi import HTTPException, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from db.session import get_session
from db.auth_models import User, Org, OrgMembership, Session as UserSession, PasswordReset


# Configuration
AUTH_JWT_SECRET = os.getenv("AUTH_JWT_SECRET", secrets.token_urlsafe(32))
AUTH_ACCESS_TTL = os.getenv("AUTH_ACCESS_TTL", "15m")
AUTH_REFRESH_TTL = os.getenv("AUTH_REFRESH_TTL", "30d")
AUTH_COOKIE_DOMAIN = os.getenv("AUTH_COOKIE_DOMAIN", None)  # None = use current domain
FEATURE_TOTP_2FA = os.getenv("FEATURE_TOTP_2FA", "false").lower() == "true"
FEATURE_WEBAUTHN = os.getenv("FEATURE_WEBAUTHN", "false").lower() == "true"
MAX_SESSIONS_PER_USER = int(os.getenv("MAX_SESSIONS_PER_USER", "5"))

# Password hashing
ph = PasswordHasher()
bcrypt_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# JWT settings
ALGORITHM = "HS256"


class AuthService:
    """Authentication service with password hashing, JWT tokens, and session management."""
    
    def __init__(self):
        self.password_hasher = ph
        self.bcrypt_context = bcrypt_context
    
    def hash_password(self, password: str) -> str:
        """Hash password using Argon2 (with bcrypt fallback)."""
        try:
            return self.password_hasher.hash(password)
        except Exception:
            # Fallback to bcrypt if Argon2 fails
            return self.bcrypt_context.hash(password)
    
    def verify_password(self, password: str, hashed: str) -> bool:
        """Verify password against hash (Argon2 with bcrypt fallback)."""
        try:
            # Try Argon2 first
            self.password_hasher.verify(hashed, password)
            return True
        except VerifyMismatchError:
            return False
        except Exception:
            # Fallback to bcrypt
            try:
                return self.bcrypt_context.verify(password, hashed)
            except Exception:
                return False
    
    def create_access_token(self, data: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
        """Create JWT access token."""
        to_encode = data.copy()
        
        if expires_delta:
            expire = datetime.now(timezone.utc) + expires_delta
        else:
            # Parse TTL string (e.g., "15m", "1h", "7d")
            expire = self._parse_ttl(AUTH_ACCESS_TTL)
        
        to_encode.update({"exp": expire, "type": "access"})
        encoded_jwt = jwt.encode(to_encode, AUTH_JWT_SECRET, algorithm=ALGORITHM)
        return encoded_jwt
    
    def create_refresh_token(self) -> str:
        """Create opaque refresh token."""
        return secrets.token_urlsafe(32)
    
    def verify_access_token(self, token: str) -> Dict[str, Any]:
        """Verify and decode JWT access token."""
        try:
            payload = jwt.decode(token, AUTH_JWT_SECRET, algorithms=[ALGORITHM])
            if payload.get("type") != "access":
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid token type"
                )
            return payload
        except JWTError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials"
            )
    
    def _parse_ttl(self, ttl_str: str) -> datetime:
        """Parse TTL string to datetime."""
        now = datetime.now(timezone.utc)
        
        if ttl_str.endswith('m'):
            minutes = int(ttl_str[:-1])
            return now + timedelta(minutes=minutes)
        elif ttl_str.endswith('h'):
            hours = int(ttl_str[:-1])
            return now + timedelta(hours=hours)
        elif ttl_str.endswith('d'):
            days = int(ttl_str[:-1])
            return now + timedelta(days=days)
        else:
            # Default to 15 minutes
            return now + timedelta(minutes=15)
    
    def hash_token(self, token: str) -> str:
        """Hash token for storage."""
        return hashlib.sha256(token.encode()).hexdigest()
    
    def create_user(self, email: str, password: str, username: Optional[str] = None) -> str:
        """Create new user account and return user ID."""
        with get_session() as session:
            # Check if user already exists
            existing_user = session.query(User).filter(User.email == email).first()
            if existing_user:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Email already registered"
                )
            
            if username:
                existing_username = session.query(User).filter(User.username == username).first()
                if existing_username:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Username already taken"
                    )
            
            # Create user
            user = User(
                email=email,
                username=username,
                password_hash=self.hash_password(password),
                is_active=True
            )
            session.add(user)
            session.commit()
            session.refresh(user)
            
            # Return user ID as string to avoid session issues
            return str(user.id)
    
    def authenticate_user(self, email: str, password: str) -> Optional[str]:
        """Authenticate user with email and password, return user ID."""
        with get_session() as session:
            user = session.query(User).filter(User.email == email).first()
            if not user or not user.is_active:
                return None
            
            if not self.verify_password(password, user.password_hash):
                return None
            
            return str(user.id)
    
    def create_session(self, user: User, user_agent: Optional[str] = None, ip_address: Optional[str] = None) -> tuple[str, str]:
        """Create user session with access and refresh tokens."""
        with get_session() as session:
            # Enforce session limit
            self._enforce_session_limit(session, user.id)
            
            # Create refresh token
            refresh_token = self.create_refresh_token()
            refresh_token_hash = self.hash_token(refresh_token)
            
            # Calculate expiration
            expires_at = self._parse_ttl(AUTH_REFRESH_TTL)
            
            # Create session record
            user_session = UserSession(
                user_id=user.id,
                refresh_token_hash=refresh_token_hash,
                user_agent=user_agent,
                ip_address=ip_address,
                expires_at=expires_at
            )
            session.add(user_session)
            session.commit()
            
            # Create access token
            access_token = self.create_access_token({
                "sub": str(user.id),
                "email": user.email,
                "username": user.username
            })
            
            return access_token, refresh_token
    
    def refresh_access_token(self, refresh_token: str) -> Optional[str]:
        """Refresh access token using refresh token."""
        with get_session() as session:
            refresh_token_hash = self.hash_token(refresh_token)
            
            # Find active session
            user_session = session.query(UserSession).filter(
                UserSession.refresh_token_hash == refresh_token_hash,
                UserSession.revoked_at.is_(None),
                UserSession.expires_at > datetime.now(timezone.utc)
            ).first()
            
            if not user_session:
                return None
            
            # Get user
            user = session.query(User).filter(User.id == user_session.user_id).first()
            if not user or not user.is_active:
                return None
            
            # Create new access token
            access_token = self.create_access_token({
                "sub": str(user.id),
                "email": user.email,
                "username": user.username
            })
            
            return access_token
    
    def revoke_session(self, refresh_token: str) -> bool:
        """Revoke user session."""
        with get_session() as session:
            refresh_token_hash = self.hash_token(refresh_token)
            
            user_session = session.query(UserSession).filter(
                UserSession.refresh_token_hash == refresh_token_hash,
                UserSession.revoked_at.is_(None)
            ).first()
            
            if user_session:
                user_session.revoked_at = datetime.now(timezone.utc)
                session.commit()
                return True
            
            return False
    
    def revoke_all_sessions(self, user_id: str) -> int:
        """Revoke all sessions for a user."""
        with get_session() as session:
            now = datetime.now(timezone.utc)
            result = session.query(UserSession).filter(
                UserSession.user_id == user_id,
                UserSession.revoked_at.is_(None)
            ).update({"revoked_at": now})
            session.commit()
            return result
    
    def get_user_sessions(self, user_id: str) -> List[UserSession]:
        """Get all active sessions for a user."""
        with get_session() as session:
            return session.query(UserSession).filter(
                UserSession.user_id == user_id,
                UserSession.revoked_at.is_(None),
                UserSession.expires_at > datetime.now(timezone.utc)
            ).all()
    
    def _enforce_session_limit(self, session: Session, user_id: str):
        """Enforce maximum sessions per user."""
        active_sessions = session.query(UserSession).filter(
            UserSession.user_id == user_id,
            UserSession.revoked_at.is_(None),
            UserSession.expires_at > datetime.now(timezone.utc)
        ).order_by(UserSession.created_at.asc()).all()
        
        if len(active_sessions) >= MAX_SESSIONS_PER_USER:
            # Revoke oldest sessions
            sessions_to_revoke = len(active_sessions) - MAX_SESSIONS_PER_USER + 1
            for i in range(sessions_to_revoke):
                active_sessions[i].revoked_at = datetime.now(timezone.utc)
    
    def create_password_reset_token(self, email: str) -> Optional[str]:
        """Create password reset token."""
        with get_session() as session:
            user = session.query(User).filter(User.email == email).first()
            if not user or not user.is_active:
                return None
            
            # Create reset token
            reset_token = secrets.token_urlsafe(32)
            token_hash = self.hash_token(reset_token)
            
            # Expires in 1 hour
            expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
            
            # Create reset record
            password_reset = PasswordReset(
                user_id=user.id,
                token_hash=token_hash,
                expires_at=expires_at
            )
            session.add(password_reset)
            session.commit()
            
            return reset_token
    
    def reset_password(self, reset_token: str, new_password: str) -> bool:
        """Reset password using reset token."""
        with get_session() as session:
            token_hash = self.hash_token(reset_token)
            
            # Find valid reset record
            password_reset = session.query(PasswordReset).filter(
                PasswordReset.token_hash == token_hash,
                PasswordReset.used_at.is_(None),
                PasswordReset.expires_at > datetime.now(timezone.utc)
            ).first()
            
            if not password_reset:
                return False
            
            # Update user password
            user = session.query(User).filter(User.id == password_reset.user_id).first()
            if not user:
                return False
            
            user.password_hash = self.hash_password(new_password)
            password_reset.used_at = datetime.now(timezone.utc)
            
            # Revoke all existing sessions
            self.revoke_all_sessions(user.id)
            
            session.commit()
            return True
    
    def get_user_by_id(self, user_id: str) -> Optional[User]:
        """Get user by ID."""
        with get_session() as session:
            return session.query(User).filter(User.id == user_id).first()
    
    def get_user_orgs(self, user_id: str) -> List[Dict[str, Any]]:
        """Get user's organization memberships."""
        with get_session() as session:
            memberships = session.query(OrgMembership).filter(
                OrgMembership.user_id == user_id
            ).all()
            
            return [
                {
                    "org_id": membership.org_id,
                    "role": membership.role,
                    "created_at": membership.created_at.isoformat()
                }
                for membership in memberships
            ]


# Global auth service instance
auth_service = AuthService()


class AuthDependency:
    """FastAPI dependency for authentication."""
    
    def __init__(self):
        self.security = HTTPBearer(auto_error=False)
    
    async def __call__(self, request: Request) -> Optional[Dict[str, Any]]:
        """Extract and validate authentication from request."""
        # Try to get token from Authorization header first
        credentials: HTTPAuthorizationCredentials = await self.security(request)
        if credentials:
            try:
                payload = auth_service.verify_access_token(credentials.credentials)
                return payload
            except HTTPException:
                pass
        
        # Try to get token from cookies
        access_token = request.cookies.get("access_token")
        if access_token:
            try:
                payload = auth_service.verify_access_token(access_token)
                return payload
            except HTTPException:
                pass
        
        return None


# Global auth dependency
get_auth = AuthDependency()
