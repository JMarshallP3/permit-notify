"""
Database repository functions for permit operations.
"""

import logging
from typing import List, Dict, Any
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy import and_

from .session import get_session
from .models import Permit

logger = logging.getLogger(__name__)

def upsert_permits(items: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Insert new permits or update existing ones.
    
    Args:
        items: List of normalized permit dictionaries from scraper
        
    Returns:
        Dictionary with 'inserted' and 'updated' counts
    """
    inserted_count = 0
    updated_count = 0
    
    with get_session() as session:
        for item in items:
            try:
                # Check if permit already exists
                existing_permit = session.query(Permit).filter(
                    Permit.permit_no == item.get('permit_no')
                ).first()
                
                if existing_permit:
                    # Update existing permit
                    updated = False
                    for field, value in item.items():
                        if field != 'permit_no' and hasattr(existing_permit, field):
                            current_value = getattr(existing_permit, field)
                            if current_value != value:
                                setattr(existing_permit, field, value)
                                updated = True
                    
                    if updated:
                        updated_count += 1
                        logger.debug(f"Updated permit: {item.get('permit_no')}")
                else:
                    # Insert new permit
                    permit = Permit(**item)
                    session.add(permit)
                    inserted_count += 1
                    logger.debug(f"Inserted new permit: {item.get('permit_no')}")
                    
            except IntegrityError as e:
                logger.warning(f"Integrity error for permit {item.get('permit_no')}: {e}")
                session.rollback()
                continue
            except Exception as e:
                logger.error(f"Error processing permit {item.get('permit_no')}: {e}")
                continue
    
    logger.info(f"Permit upsert completed: {inserted_count} inserted, {updated_count} updated")
    return {"inserted": inserted_count, "updated": updated_count}

def get_recent_permits(limit: int = 50) -> List[Dict[str, Any]]:
    """
    Get recent permits ordered by creation date.
    
    Args:
        limit: Maximum number of permits to return
        
    Returns:
        List of permit dictionaries
    """
    with get_session() as session:
        permits = session.query(Permit).order_by(
            Permit.created_at.desc()
        ).limit(limit).all()
        
        logger.debug(f"Retrieved {len(permits)} recent permits")
        return [permit.to_dict() for permit in permits]

def get_permit_by_number(permit_no: str) -> Dict[str, Any]:
    """
    Get a specific permit by permit number.
    
    Args:
        permit_no: Permit number to search for
        
    Returns:
        Permit dictionary or None if not found
    """
    with get_session() as session:
        permit = session.query(Permit).filter(
            Permit.permit_no == permit_no
        ).first()
        
        if permit:
            logger.debug(f"Found permit: {permit_no}")
            return permit.to_dict()
        else:
            logger.debug(f"Permit not found: {permit_no}")
            return None

def search_permits(
    operator: str = None,
    county: str = None,
    limit: int = 50
) -> List[Dict[str, Any]]:
    """
    Search permits by operator and/or county.
    
    Args:
        operator: Filter by operator name (partial match)
        county: Filter by county name (partial match)
        limit: Maximum number of results
        
    Returns:
        List of matching permit dictionaries
    """
    with get_session() as session:
        query = session.query(Permit)
        
        # Build filters
        filters = []
        if operator:
            filters.append(Permit.operator.ilike(f"%{operator}%"))
        if county:
            filters.append(Permit.county.ilike(f"%{county}%"))
        
        if filters:
            query = query.filter(and_(*filters))
        
        permits = query.order_by(Permit.created_at.desc()).limit(limit).all()
        
        logger.debug(f"Search returned {len(permits)} permits")
        return [permit.to_dict() for permit in permits]
