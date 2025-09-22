"""
Database models for permit notification system.
Currently matches Railway database schema - will be updated via migration.
"""

import datetime
from sqlalchemy import Column, Integer, String, Date, DateTime, Index
from sqlalchemy.sql import func
from .session import Base

def utcnow():
    """Get current UTC datetime."""
    return datetime.datetime.now(datetime.timezone.utc)

class Permit(Base):
    """
    Permit model representing drilling permit data.
    Currently matches the existing Railway database schema.
    """
    __tablename__ = "permits"
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Current Railway database fields (matching the screenshot)
    permit_no = Column(String(50), unique=True, nullable=False, index=True)
    operator = Column(String(200), nullable=True, index=True)
    county = Column(String(100), nullable=True, index=True)
    district = Column(String(50), nullable=True)
    well_name = Column(String(200), nullable=True)
    lease_no = Column(String(100), nullable=True)
    field = Column(String(200), nullable=True)
    submission_date = Column(Date, nullable=True)
    api_no = Column(String(50), nullable=True)
    
    # Metadata
    created_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    
    # Indexes for common queries
    __table_args__ = (
        Index('idx_permit_operator', 'operator'),
        Index('idx_permit_county', 'county'),
        Index('idx_permit_created', 'created_at'),
    )
    
    def to_dict(self):
        """Convert model to dictionary."""
        return {
            'id': self.id,
            'permit_no': self.permit_no,
            'operator': self.operator,
            'county': self.county,
            'district': self.district,
            'well_name': self.well_name,
            'lease_no': self.lease_no,
            'field': self.field,
            'submission_date': self.submission_date.isoformat() if self.submission_date else None,
            'api_no': self.api_no,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }
    
    def __repr__(self):
        return f"<Permit(id={self.id}, permit_no='{self.permit_no}', operator='{self.operator}')>"
