"""
Database models for permit notification system.
Updated to match RRC W-1 Search Results columns.
"""

import datetime
from sqlalchemy import Column, Integer, String, Date, DateTime, Index, Boolean, Numeric
from sqlalchemy.sql import func
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
    
    # RRC W-1 Search Results fields (matching your requirements)
    status_date = Column(Date, nullable=True, index=True)  # Status Date
    status_no = Column(String(50), unique=True, nullable=False, index=True)  # Status #
    api_no = Column(String(50), nullable=True, index=True)  # API No.
    operator_name = Column(String(200), nullable=True, index=True)  # Operator Name/Number
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
    
    # Legacy fields (keeping for backward compatibility)
    permit_no = Column(String(50), nullable=True, index=True)  # Legacy field
    operator = Column(String(200), nullable=True, index=True)  # Legacy field (same as operator_name)
    well_name = Column(String(200), nullable=True)  # Legacy field (same as well_no)
    lease_no = Column(String(100), nullable=True)  # Legacy field (same as lease_name)
    field = Column(String(200), nullable=True)  # Legacy field
    submission_date = Column(Date, nullable=True)  # Legacy field (same as status_date)
    
    # Metadata
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)
    
    # Indexes for common queries
    __table_args__ = (
        Index('idx_permit_status_no', 'status_no'),
        Index('idx_permit_api_no', 'api_no'),
        Index('idx_permit_operator_name', 'operator_name'),
        Index('idx_permit_county', 'county'),
        Index('idx_permit_district', 'district'),
        Index('idx_permit_status_date', 'status_date'),
        Index('idx_permit_created', 'created_at'),
    )
    
    def to_dict(self):
        """Convert model to dictionary."""
        return {
            'id': self.id,
            'status_date': self.status_date.isoformat() if self.status_date else None,
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
            # Legacy fields
            'permit_no': self.permit_no,
            'operator': self.operator,
            'well_name': self.well_name,
            'lease_no': self.lease_no,
            'field': self.field,
            'submission_date': self.submission_date.isoformat() if self.submission_date else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
    
    def __repr__(self):
        return f"<Permit(id={self.id}, status_no='{self.status_no}', operator='{self.operator_name}')>"
