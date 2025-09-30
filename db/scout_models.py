"""
Scout v2.1 Database Models
Additive models for Scout insights system - does not modify existing permit/completion tables
"""

from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, JSON, Enum, ForeignKey, Index
from sqlalchemy.dialects.postgresql import UUID, ARRAY
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime, timezone
import uuid
import enum

Base = declarative_base()

class ClaimType(enum.Enum):
    CONFIRMED = "confirmed"
    LIKELY = "likely" 
    RUMOR = "rumor"
    SPECULATION = "speculation"

class ConfidenceLevel(enum.Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"

class InsightUserState(enum.Enum):
    DEFAULT = "default"
    KEPT = "kept"
    DISMISSED = "dismissed"
    ARCHIVED = "archived"

class Signal(Base):
    """Normalized public web signals from MRF, press releases, etc."""
    __tablename__ = 'signals'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(String(50), nullable=False, index=True)
    found_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    source_url = Column(Text, nullable=False)
    source_type = Column(String(50), nullable=False)  # 'mrf', 'press_release', 'sec_filing', etc.
    state = Column(String(2), nullable=True)  # TX, OK, etc.
    county = Column(String(100), nullable=True, index=True)
    play_basin = Column(String(100), nullable=True)
    
    # Arrays for flexible matching
    operators = Column(ARRAY(String), nullable=False, default=list)
    unit_tokens = Column(ARRAY(String), nullable=False, default=list)  # pad, unit, lease, abstract tokens
    keywords = Column(ARRAY(String), nullable=False, default=list)
    
    claim_type = Column(Enum(ClaimType), nullable=False, default=ClaimType.RUMOR)
    timeframe = Column(String(100), nullable=True)  # "Q1 2024", "next 60 days", etc.
    summary = Column(Text, nullable=False)
    raw_excerpt = Column(Text, nullable=True)
    
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    
    # Indexes for performance
    __table_args__ = (
        Index('idx_signals_org_found_at', 'org_id', 'found_at'),
        Index('idx_signals_org_county', 'org_id', 'county'),
        Index('idx_signals_operators_gin', 'operators', postgresql_using='gin'),
        Index('idx_signals_unit_tokens_gin', 'unit_tokens', postgresql_using='gin'),
        Index('idx_signals_keywords_gin', 'keywords', postgresql_using='gin'),
    )

class ScoutInsight(Base):
    """User-visible, deduped, analytics-enriched insights"""
    __tablename__ = 'scout_insights'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(String(50), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    
    # Content fields
    title = Column(String(90), nullable=False)  # ≤90 chars per spec
    what_happened = Column(Text, nullable=False)  # JSON array of facts
    why_it_matters = Column(Text, nullable=False)  # JSON array of impacts
    
    confidence = Column(Enum(ConfidenceLevel), nullable=False)
    confidence_reasons = Column(Text, nullable=False)  # JSON array of reasons
    next_checks = Column(Text, nullable=False)  # JSON array of todos
    source_urls = Column(Text, nullable=False)  # JSON array of {url, label}
    
    # Relationships
    related_permit_ids = Column(ARRAY(String), nullable=False, default=list)  # permit status_nos
    county = Column(String(100), nullable=True, index=True)
    state = Column(String(2), nullable=True)
    operator_keys = Column(ARRAY(String), nullable=False, default=list, index=True)
    
    # Deep analytics (jsonb for flexible querying)
    analytics = Column(JSON, nullable=False, default=dict)
    
    # Deduplication tracking
    dedup_key = Column(String(255), nullable=True, index=True)  # for 72h dedup
    
    # Indexes
    __table_args__ = (
        Index('idx_scout_insights_org_created', 'org_id', 'created_at'),
        Index('idx_scout_insights_org_county', 'org_id', 'county'),
        Index('idx_scout_insights_operator_keys_gin', 'operator_keys', postgresql_using='gin'),
    )

class ScoutInsightUserState(Base):
    """Per-user state for insights (Keep/Dismiss/Archive with undo)"""
    __tablename__ = 'scout_insight_user_state'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(String(50), nullable=False)
    user_id = Column(String(100), nullable=False)  # Future: FK to users table
    insight_id = Column(UUID(as_uuid=True), ForeignKey('scout_insights.id'), nullable=False)
    
    state = Column(Enum(InsightUserState), nullable=False, default=InsightUserState.DEFAULT)
    
    # State timestamps
    kept_at = Column(DateTime(timezone=True), nullable=True)
    dismissed_at = Column(DateTime(timezone=True), nullable=True)
    dismiss_reason = Column(Text, nullable=True)
    archived_at = Column(DateTime(timezone=True), nullable=True)
    
    # Undo support
    undo_token = Column(UUID(as_uuid=True), nullable=True)
    undo_expires_at = Column(DateTime(timezone=True), nullable=True)
    
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    
    # Relationship
    insight = relationship("ScoutInsight", backref="user_states")
    
    # Unique constraint and indexes
    __table_args__ = (
        Index('idx_scout_user_state_unique', 'org_id', 'user_id', 'insight_id', unique=True),
        Index('idx_scout_user_state_query', 'org_id', 'user_id', 'state', 'insight_id'),
        Index('idx_scout_user_state_archive', 'org_id', 'user_id', 'archived_at'),
    )
