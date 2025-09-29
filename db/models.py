"""
Database models for permit notification system.
Updated to match RRC W-1 Search Results columns.
"""

import datetime
import sqlalchemy as sa
from sqlalchemy import Column, Integer, String, Date, DateTime, Index, Boolean, Numeric, JSON, event as sqla_event
from sqlalchemy.sql import func, insert
from .session import Base

def utcnow():
    """Get current UTC datetime."""
    return datetime.datetime.now(datetime.timezone.utc)

class Permit(Base):
    """
    Permit model representing drilling permit data from RRC W-1 Search Results.
    """
    __tablename__ = "permits"
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Tenant isolation - ALL queries must filter by this
    org_id = Column(String(50), nullable=False, index=True, default='default_org')
    
    # RRC W-1 Search Results fields (matching your requirements)
    status_date = Column(Date, nullable=True, index=True)  # Status Date (MM-DD-YYYY format)
    status_no = Column(String(50), unique=True, nullable=False, index=True)  # Status #
    api_no = Column(String(50), nullable=True, index=True)  # API No.
    operator_name = Column(String(200), nullable=True, index=True)  # Operator Name (cleaned, no number)
    operator_number = Column(String(50), nullable=True)  # Operator number (extracted from name)
    lease_name = Column(String(200), nullable=True)  # Lease Name
    well_no = Column(String(50), nullable=True)  # Well #
    district = Column(String(50), nullable=True, index=True)  # Dist.
    county = Column(String(100), nullable=True, index=True)  # County
    wellbore_profile = Column(String(50), nullable=True)  # Wellbore Profile
    filing_purpose = Column(String(100), nullable=True)  # Filing Purpose
    amend = Column(Boolean, nullable=True)  # Amend (Yes/No converted to boolean)
    total_depth = Column(Numeric(10, 2), nullable=True)  # Total Depth
    stacked_lateral_parent_well_dp = Column(String(100), nullable=True)  # Stacked Lateral Parent Well DP
    current_queue = Column(String(100), nullable=True)  # Current Queue
    
    # HTML detail page fields
    horizontal_wellbore = sa.Column(sa.Text)  # Horizontal Wellbore
    field_name = sa.Column(sa.Text)  # Field Name
    acres = sa.Column(sa.Numeric(12,2))  # Acres
    section = sa.Column(sa.Text)  # Section
    block = sa.Column(sa.Text)  # Block
    survey = sa.Column(sa.Text)  # Survey
    abstract_no = sa.Column(sa.Text)  # Abstract Number
    detail_url = sa.Column(sa.Text)  # Detail URL
    
    # PDF fields & bookkeeping
    reservoir_well_count = sa.Column(sa.Integer)  # Reservoir Well Count
    w1_pdf_url = sa.Column(sa.Text)  # W-1 PDF URL
    w1_parse_status = sa.Column(sa.Text)  # W-1 Parse Status (ok, no_pdf, download_error, parse_error)
    w1_parse_confidence = sa.Column(sa.Numeric)  # W-1 Parse Confidence
    w1_text_snippet = sa.Column(sa.Text)  # W-1 Text Snippet
    w1_last_enriched_at = sa.Column(sa.DateTime(timezone=True))  # W-1 Last Enriched At
    
    # Optimistic concurrency control
    version = Column(Integer, nullable=False, default=1)
    
    # Metadata (moved to end)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)  # Moved to last
    
    # Indexes for common queries (all include org_id for tenant isolation)
    __table_args__ = (
        Index('idx_permit_org_status_no', 'org_id', 'status_no'),
        Index('idx_permit_org_api_no', 'org_id', 'api_no'),
        Index('idx_permit_org_operator', 'org_id', 'operator_name'),
        Index('idx_permit_org_county', 'org_id', 'county'),
        Index('idx_permit_org_district', 'org_id', 'district'),
        Index('idx_permit_org_status_date', 'org_id', 'status_date'),
        Index('idx_permit_org_created', 'org_id', 'created_at'),
        Index('idx_permit_org_updated', 'org_id', 'updated_at'),
    )
    
    def to_dict(self):
        """Convert model to dictionary."""
        # Format status_date as MM-DD-YYYY
        status_date_formatted = None
        if self.status_date:
            status_date_formatted = self.status_date.strftime('%m-%d-%Y')
        
        return {
            'id': self.id,
            'status_date': status_date_formatted,  # MM-DD-YYYY format
            'status_no': self.status_no,
            'api_no': self.api_no,
            'operator_name': self.operator_name,
            'operator_number': self.operator_number,
            'lease_name': self.lease_name,
            'well_no': self.well_no,
            'district': self.district,
            'county': self.county,
            'wellbore_profile': self.wellbore_profile,
            'filing_purpose': self.filing_purpose,
            'amend': self.amend,
            'total_depth': float(self.total_depth) if self.total_depth else None,
            'stacked_lateral_parent_well_dp': self.stacked_lateral_parent_well_dp,
            'current_queue': self.current_queue,
            # HTML detail page fields
            'horizontal_wellbore': self.horizontal_wellbore,
            'field_name': self.field_name,
            'acres': float(self.acres) if self.acres else None,
            'section': self.section,
            'block': self.block,
            'survey': self.survey,
            'abstract_no': self.abstract_no,
            'detail_url': self.detail_url,
            # PDF fields & bookkeeping
            'reservoir_well_count': self.reservoir_well_count,
            'w1_pdf_url': self.w1_pdf_url,
            'w1_parse_status': self.w1_parse_status,
            'w1_parse_confidence': float(self.w1_parse_confidence) if self.w1_parse_confidence else None,
            'w1_text_snippet': self.w1_text_snippet,
            'w1_last_enriched_at': self.w1_last_enriched_at.isoformat() if self.w1_last_enriched_at else None,
            # Metadata (created_at moved to end)
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }
    
    def __repr__(self):
        return f"<Permit(id={self.id}, org_id='{self.org_id}', status_no='{self.status_no}', operator='{self.operator_name}')>"


class Event(Base):
    """Event sourcing table for tracking all changes to permits."""
    __tablename__ = "events"

    id = Column(Integer, primary_key=True, index=True)
    type = Column(String(32), nullable=False)              # e.g., "created", "updated"
    entity = Column(String(32), nullable=False)            # e.g., "permit"
    entity_id = Column(Integer, nullable=False)            # points to permits.id
    org_id = Column(String(50), nullable=False, index=True)  # tenant isolation
    payload = Column(JSON, nullable=True)                  # optional; lightweight payload
    ts = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        Index('idx_events_org_entity', 'org_id', 'entity', 'entity_id'),
        Index('idx_events_org_ts', 'org_id', 'ts'),
    )

    def __repr__(self):
        return f"<Event(id={self.id}, org_id='{self.org_id}', type='{self.type}', entity='{self.entity}', entity_id={self.entity_id})>"


# --- Auto-increment version on any update ---
@sqla_event.listens_for(Permit, "before_update")
def _bump_version(mapper, connection, target):
    try:
        target.version = (target.version or 1) + 1
    except Exception:
        target.version = 1


# --- Write Event rows after insert/update (works even if other processes modify via ORM) ---
@sqla_event.listens_for(Permit, "after_insert")
def _emit_created_event(mapper, connection, target):
    connection.execute(
        insert(Event.__table__).values(
            type="created",
            entity="permit",
            entity_id=target.id,
            org_id=target.org_id,
            payload={"id": target.id, "status_no": target.status_no},
        )
    )


@sqla_event.listens_for(Permit, "after_update")
def _emit_updated_event(mapper, connection, target):
    connection.execute(
        insert(Event.__table__).values(
            type="updated",
            entity="permit",
            entity_id=target.id,
            org_id=target.org_id,
            payload={"id": target.id, "version": target.version, "status_no": target.status_no},
        )
    )


# FieldCorrection model is defined in db/field_corrections.py to avoid conflicts