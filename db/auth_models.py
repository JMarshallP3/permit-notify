"""
Authentication models for user accounts, sessions, and org memberships.
"""

import datetime
import uuid
from sqlalchemy import Column, String, Boolean, DateTime, Text, ForeignKey, Index, CheckConstraint
from sqlalchemy.dialects.postgresql import UUID, INET
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .session import Base


def utcnow():
    """Get current UTC datetime."""
    return datetime.datetime.now(datetime.timezone.utc)


class User(Base):
    """User accounts with email-as-username support."""
    __tablename__ = "users"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String(255), unique=True, nullable=False, index=True)
    username = Column(String(100), unique=True, nullable=True, index=True)  # Optional username
    password_hash = Column(Text, nullable=False)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)
    
    # Relationships
    memberships = relationship("OrgMembership", back_populates="user", cascade="all, delete-orphan")
    sessions = relationship("Session", back_populates="user", cascade="all, delete-orphan")
    password_resets = relationship("PasswordReset", back_populates="user", cascade="all, delete-orphan")
    
    # Indexes
    __table_args__ = (
        Index('idx_users_email', 'email'),
        Index('idx_users_username', 'username'),
        Index('idx_users_active', 'is_active'),
    )
    
    def __repr__(self):
        return f"<User(id={self.id}, email='{self.email}', username='{self.username}')>"


class Org(Base):
    """Organizations (tenants) - extends existing org concept."""
    __tablename__ = "orgs"
    
    id = Column(String(50), primary_key=True)  # Keep existing org_id format
    name = Column(String(200), nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)
    
    # Relationships
    memberships = relationship("OrgMembership", back_populates="org", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Org(id='{self.id}', name='{self.name}')>"


class OrgMembership(Base):
    """User membership in organizations with roles."""
    __tablename__ = "org_memberships"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    org_id = Column(String(50), ForeignKey("orgs.id", ondelete="CASCADE"), nullable=False)
    role = Column(String(20), nullable=False)  # 'owner', 'admin', 'member'
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)
    
    # Relationships
    user = relationship("User", back_populates="memberships")
    org = relationship("Org", back_populates="memberships")
    
    # Constraints and indexes
    __table_args__ = (
        CheckConstraint("role IN ('owner', 'admin', 'member')", name='ck_org_memberships_role'),
        Index('idx_org_memberships_user_org', 'user_id', 'org_id', unique=True),
        Index('idx_org_memberships_org_role', 'org_id', 'role'),
        Index('idx_org_memberships_user', 'user_id'),
    )
    
    def __repr__(self):
        return f"<OrgMembership(user_id={self.user_id}, org_id='{self.org_id}', role='{self.role}')>"


class Session(Base):
    """User sessions with refresh tokens for multi-device support."""
    __tablename__ = "sessions"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    refresh_token_hash = Column(Text, nullable=False)  # Hashed refresh token
    user_agent = Column(Text, nullable=True)
    ip_address = Column(INET, nullable=True)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    revoked_at = Column(DateTime(timezone=True), nullable=True)
    
    # Relationships
    user = relationship("User", back_populates="sessions")
    
    # Indexes
    __table_args__ = (
        Index('idx_sessions_user_active', 'user_id', 'revoked_at'),
        Index('idx_sessions_expires', 'expires_at'),
        Index('idx_sessions_refresh_hash', 'refresh_token_hash'),
    )
    
    @property
    def is_active(self):
        """Check if session is active (not revoked and not expired)."""
        now = utcnow()
        return (
            self.revoked_at is None and 
            self.expires_at > now
        )
    
    def __repr__(self):
        return f"<Session(id={self.id}, user_id={self.user_id}, active={self.is_active})>"


class PasswordReset(Base):
    """Password reset tokens with expiration."""
    __tablename__ = "password_resets"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    token_hash = Column(Text, nullable=False)  # Hashed reset token
    expires_at = Column(DateTime(timezone=True), nullable=False)
    used_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    
    # Relationships
    user = relationship("User", back_populates="password_resets")
    
    # Indexes
    __table_args__ = (
        Index('idx_password_resets_user', 'user_id'),
        Index('idx_password_resets_token_hash', 'token_hash'),
        Index('idx_password_resets_expires', 'expires_at'),
    )
    
    @property
    def is_valid(self):
        """Check if reset token is valid (not used and not expired)."""
        now = utcnow()
        return (
            self.used_at is None and 
            self.expires_at > now
        )
    
    def __repr__(self):
        return f"<PasswordReset(id={self.id}, user_id={self.user_id}, valid={self.is_valid})>"
