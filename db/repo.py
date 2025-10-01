"""
Database repository functions for permit operations.
"""

import logging
from typing import List, Dict, Any
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc, func, text
from datetime import datetime, timedelta

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
    error_count = 0
    
    with get_session() as session:
        for item in items:
            # Process each permit in its own transaction to prevent cascade failures
            try:
                # Use status_no as primary identifier, fallback to permit_no for legacy data
                primary_key = item.get('status_no') or item.get('permit_no')
                if not primary_key:
                    logger.warning(f"Skipping item without primary key: {item}")
                    continue
                
                # Remove any fields that don't exist in the current model
                clean_item = {}
                for field, value in item.items():
                    if hasattr(Permit, field):
                        clean_item[field] = value
                    else:
                        logger.debug(f"Skipping unknown field '{field}' for permit {primary_key}")
                
                # Check if permit already exists
                existing_permit = session.query(Permit).filter(
                    Permit.status_no == primary_key
                ).first()
                
                if not existing_permit and clean_item.get('permit_no'):
                    # Fallback to legacy permit_no lookup
                    existing_permit = session.query(Permit).filter(
                        Permit.permit_no == clean_item.get('permit_no')
                    ).first()
                
                if existing_permit:
                    # Update existing permit - always update key fields that might change
                    updated = False
                    
                    # Fields that should always be updated when permit is refiled
                    always_update_fields = [
                        'filing_purpose', 'current_queue', 'amend', 'updated_at',
                        'w1_parse_status', 'w1_parse_confidence', 'w1_text_snippet'
                    ]
                    
                    for field, value in clean_item.items():
                        if field not in ['id', 'status_no', 'permit_no', 'created_at'] and hasattr(existing_permit, field):
                            current_value = getattr(existing_permit, field)
                            
                            # Always update certain fields, or update if value actually changed
                            if (field in always_update_fields and value is not None) or (current_value != value):
                                # Special handling for None values - don't overwrite existing data with None
                                if value is not None or current_value is None:
                                    setattr(existing_permit, field, value)
                                    updated = True
                                    logger.debug(f"Updated {field} for permit {primary_key}: {current_value} -> {value}")
                    
                    if updated:
                        session.commit()  # Commit this update
                        updated_count += 1
                        logger.info(f"Updated permit: {primary_key}")
                    else:
                        logger.debug(f"No changes needed for permit: {primary_key}")
                else:
                    # Insert new permit
                    permit = Permit(**clean_item)
                    session.add(permit)
                    session.commit()  # Commit this insert
                    inserted_count += 1
                    logger.info(f"Inserted new permit: {primary_key}")
                    
            except IntegrityError as e:
                logger.warning(f"Integrity error for permit {primary_key}: {e}")
                session.rollback()
                error_count += 1
                continue
            except Exception as e:
                logger.error(f"Error processing permit {primary_key}: {e}")
                session.rollback()
                error_count += 1
                continue
    
    logger.info(f"Permit upsert completed: {inserted_count} inserted, {updated_count} updated, {error_count} errors")
    return {"inserted": inserted_count, "updated": updated_count, "errors": error_count}


def get_reservoir_trends(days_back: int = 90, specific_reservoirs: List[str] = None, view_type: str = "daily", reservoir_mappings: Dict[str, str] = None) -> Dict[str, Any]:
    """
    Get historical reservoir permit trends for charting.
    
    Args:
        days_back: Number of days to look back from today
        specific_reservoirs: List of specific reservoir names to include
        view_type: View type - "daily" for daily counts or "cumulative" for cumulative counts
        reservoir_mappings: Dictionary of field_name -> reservoir_name mappings from frontend
        
    Returns:
        Dictionary with reservoir trend data formatted for Chart.js
    """
    with get_session() as session:
        # Calculate the date range
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back)
        
        # Base query for permits with field names in the date range
        base_query = session.query(Permit).filter(
            and_(
                Permit.status_date >= start_date,
                Permit.status_date <= end_date,
                Permit.field_name.isnot(None),
                Permit.field_name != ''
            )
        )
        
        # If specific reservoirs requested, filter by them
        if specific_reservoirs:
            # Create a filter for field names that contain any of the specified reservoirs
            reservoir_filters = []
            for reservoir in specific_reservoirs:
                reservoir_filters.append(Permit.field_name.ilike(f'%{reservoir}%'))
            
            if reservoir_filters:
                from sqlalchemy import or_
                base_query = base_query.filter(or_(*reservoir_filters))
        
        # Get all permits in the date range
        permits = base_query.all()
        
        # Process permits to extract reservoir names and group by date
        reservoir_data = {}
        date_labels = []
        
        # Generate all dates in the range
        current_date = start_date
        while current_date <= end_date:
            date_labels.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
        
        # Get field corrections from database to apply to trends
        field_corrections = {}
        try:
            from db.field_corrections import FieldCorrection
            corrections = session.query(FieldCorrection).all()
            for correction in corrections:
                field_corrections[correction.wrong_field_name] = correction.correct_field_name
        except Exception as e:
            logger.warning(f"Could not load field corrections: {e}")
        
        # Merge database corrections with frontend mappings
        all_mappings = {}
        all_mappings.update(field_corrections)  # Database corrections first
        if reservoir_mappings:
            all_mappings.update(reservoir_mappings)  # Frontend mappings override
        
        # Process each permit
        for permit in permits:
            if not permit.field_name:
                continue
                
            # Apply corrections to get the correct field name
            corrected_field_name = all_mappings.get(permit.field_name, permit.field_name)
            
            # Extract reservoir name from corrected field name
            reservoir = extract_reservoir_name(corrected_field_name)
            
            if reservoir not in reservoir_data:
                reservoir_data[reservoir] = {date: 0 for date in date_labels}
            
            permit_date = permit.status_date.strftime('%Y-%m-%d')
            if permit_date in reservoir_data[reservoir]:
                reservoir_data[reservoir][permit_date] += 1
        
        # Format data for Chart.js
        datasets = []
        colors = [
            '#3B82F6',  # Blue
            '#10B981',  # Green
            '#F59E0B',  # Yellow
            '#EF4444',  # Red
            '#8B5CF6',  # Purple
            '#06B6D4',  # Cyan
            '#F97316',  # Orange
            '#84CC16',  # Lime
            '#EC4899',  # Pink
            '#6B7280',  # Gray
        ]
        
        for i, (reservoir, daily_counts) in enumerate(reservoir_data.items()):
            # Get daily data
            daily_data = [daily_counts[date] for date in date_labels]
            
            # Calculate cumulative data if requested
            if view_type == "cumulative":
                cumulative_data = []
                running_total = 0
                for count in daily_data:
                    running_total += count
                    cumulative_data.append(running_total)
                chart_data = cumulative_data
            else:
                chart_data = daily_data
            
            datasets.append({
                'label': reservoir,
                'data': chart_data,
                'borderColor': colors[i % len(colors)],
                'backgroundColor': colors[i % len(colors)] + '20',  # Add transparency
                'tension': 0.4,
                'fill': False,
                'pointRadius': 3,
                'pointHoverRadius': 6
            })
        
        return {
            'labels': date_labels,
            'datasets': datasets,
            'reservoirs': list(reservoir_data.keys()),
            'date_range': {
                'start': start_date.strftime('%Y-%m-%d'),
                'end': end_date.strftime('%Y-%m-%d')
            }
        }


def extract_reservoir_name(field_name: str) -> str:
    """
    Extract reservoir name from field name using similar logic to JavaScript.
    """
    if not field_name:
        return 'UNKNOWN'
    
    import re
    
    # Predefined mappings (same as JavaScript)
    mappings = {
        'HAWKVILLE (AUSTIN CHALK)': 'AUSTIN CHALK',
        'SPRABERRY (TREND AREA)': 'SPRABERRY',
        'PHANTOM (WOLFCAMP)': 'WOLFCAMP',
        'SUGARKANE (EAGLE FORD)': 'EAGLE FORD',
        'EMMA (BARNETT SHALE)': 'BARNETT SHALE',
        'EAGLE FORD': 'EAGLE FORD',
        'WOLFCAMP': 'WOLFCAMP',
        'AUSTIN CHALK': 'AUSTIN CHALK',
        'BARNETT SHALE': 'BARNETT SHALE'
    }
    
    # Check exact mappings first
    if field_name in mappings:
        return mappings[field_name]
    
    # Pattern matching
    patterns = [
        r'\(([^)]+)\)$',  # Pattern: "FIELD NAME (RESERVOIR NAME)"
        r'^([A-Z\s]+)\s*\(',  # Pattern: "RESERVOIR NAME (ADDITIONAL INFO)"
        r'^([A-Z\s]+)$'  # Pattern: Just the field name if no parentheses
    ]
    
    for pattern in patterns:
        match = re.search(pattern, field_name)
        if match:
            reservoir = match.group(1).strip()
            
            # Clean up common reservoir names
            reservoir = re.sub(r'\bTREND\s+AREA\b', '', reservoir)
            reservoir = re.sub(r'\bSHALE\b', 'SHALE', reservoir)
            reservoir = re.sub(r'\bFORD\b', 'FORD', reservoir)
            reservoir = re.sub(r'\bCHALK\b', 'CHALK', reservoir)
            reservoir = re.sub(r'\bCAMP\b', 'CAMP', reservoir)
            reservoir = reservoir.strip()
            
            return reservoir if reservoir else field_name
    
    return field_name

def get_recent_permits(limit: int = 50, days_back: int = 30) -> List[Dict[str, Any]]:
    """
    Get recent permits from the last N days, ordered by filing date, then creation date.
    
    Args:
        limit: Maximum number of permits to return
        days_back: Number of days back to look for permits (default: 30)
        
    Returns:
        List of permit dictionaries
    """
    from datetime import datetime, timedelta
    
    with get_session() as session:
        # Calculate the cutoff date
        cutoff_date = datetime.now() - timedelta(days=days_back)
        
        permits = session.query(Permit).filter(
            Permit.status_date >= cutoff_date
        ).order_by(
            Permit.status_date.desc(),
            Permit.created_at.desc()
        ).limit(limit).all()
        
        logger.debug(f"Retrieved {len(permits)} permits from last {days_back} days")
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
