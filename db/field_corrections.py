#!/usr/bin/env python3
"""Database model for field name corrections and learning."""

from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from db.models import Base

class FieldCorrection(Base):
    """Store user corrections for field names to enable learning."""
    
    __tablename__ = 'field_corrections'
    
    id = Column(Integer, primary_key=True)
    
    # Tenant isolation
    org_id = Column(String(50), nullable=False, index=True, default='default_org')
    
    # Original permit info
    permit_id = Column(Integer, nullable=False)
    status_no = Column(String(50), nullable=False)
    lease_name = Column(String(255))
    operator_name = Column(String(255))
    
    # Field name correction
    wrong_field_name = Column(String(255), nullable=False)  # What system extracted
    correct_field_name = Column(String(255), nullable=False)  # What user corrected to
    
    # Learning context
    detail_url = Column(Text)  # URL where correction was made
    html_context = Column(Text)  # Relevant HTML snippet for pattern learning
    
    # Metadata
    corrected_by = Column(String(100), default='user')
    corrected_at = Column(DateTime, server_default=func.now())
    applied_to_permit = Column(Boolean, default=False)
    
    # Pattern learning
    extraction_pattern = Column(Text)  # Pattern that caused wrong extraction
    correction_pattern = Column(Text)  # Pattern that should be used instead
    
    def __repr__(self):
        return f"<FieldCorrection(status_no='{self.status_no}', wrong='{self.wrong_field_name}', correct='{self.correct_field_name}')>"
