#!/usr/bin/env python3
"""
Script to scrape permits and save them to the PostgreSQL database.
"""

import os
import sys
import logging
from datetime import datetime, date
from typing import List, Dict, Any

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.scraper.rrc_w1 import RRCW1Client
from db.models import Permit
from db.session import get_session, SessionLocal

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_status_date(status_date_str: str) -> date:
    """Parse status date string to date object."""
    if not status_date_str:
        return None
    
    try:
        # Handle formats like "Submitted 08/01/2024 Approved 09/06/2024"
        if "Submitted" in status_date_str and "Approved" in status_date_str:
            # Extract the submitted date
            parts = status_date_str.split("Submitted")[1].split("Approved")[0].strip()
            return datetime.strptime(parts, "%m/%d/%Y").date()
        elif "Submitted" in status_date_str:
            # Just submitted date
            parts = status_date_str.split("Submitted")[1].strip()
            return datetime.strptime(parts, "%m/%d/%Y").date()
        else:
            # Try direct parsing
            return datetime.strptime(status_date_str, "%m/%d/%Y").date()
    except Exception as e:
        logger.warning(f"Could not parse date '{status_date_str}': {e}")
        return None

def parse_operator_info(operator_str: str) -> tuple:
    """Parse operator string to extract name and number."""
    if not operator_str:
        return None, None
    
    try:
        # Format: "COMPANY NAME (123456)"
        if "(" in operator_str and ")" in operator_str:
            name = operator_str.split("(")[0].strip()
            number = operator_str.split("(")[1].split(")")[0].strip()
            return name, number
        else:
            return operator_str, None
    except Exception as e:
        logger.warning(f"Could not parse operator '{operator_str}': {e}")
        return operator_str, None

def parse_amend_field(amend_str: str) -> bool:
    """Parse amend field to boolean."""
    if not amend_str:
        return None
    
    amend_str = str(amend_str).strip().lower()
    if amend_str in ['yes', 'y', 'true', '1']:
        return True
    elif amend_str in ['no', 'n', 'false', '0', '-']:
        return False
    else:
        return None

def parse_total_depth(depth_str: str) -> float:
    """Parse total depth to float."""
    if not depth_str:
        return None
    
    try:
        # Remove any non-numeric characters except decimal point
        import re
        cleaned = re.sub(r'[^\d.]', '', str(depth_str))
        if cleaned:
            return float(cleaned)
        return None
    except Exception as e:
        logger.warning(f"Could not parse depth '{depth_str}': {e}")
        return None

def save_permits_to_database(permits: List[Dict[str, Any]]) -> int:
    """Save permits to database, returning count of saved permits."""
    if not permits:
        logger.info("No permits to save")
        return 0
    
    session = SessionLocal()
    saved_count = 0
    skipped_count = 0
    
    try:
        for i, permit_data in enumerate(permits):
            logger.info(f"Processing permit {i+1}: {permit_data}")
            
            # Skip header rows (check if this is a header row)
            if (permit_data.get('status_no') == 'Status #' or 
                permit_data.get('status_date') == 'Status Date' or
                permit_data.get('operator_name') == 'Operator Name/Number' or
                permit_data.get('api_no') == 'API No.' or
                permit_data.get('lease_name') == 'Lease Name' or
                permit_data.get('district') == 'Dist.' or
                permit_data.get('county') == 'County'):
                logger.info(f"Skipping header row: {permit_data}")
                continue
            
            # Skip if no meaningful data (all fields are None or empty)
            if not any(v for v in permit_data.values() if v and str(v).strip()):
                logger.debug("Skipping empty permit row")
                continue
            
            # Skip if this looks like a header row (all values are column names)
            header_values = ['Status Date', 'Status #', 'API No.', 'Operator Name/Number', 'Lease Name', 'Well #', 'Dist.', 'County', 'Wellbore Profile', 'Filing Purpose', 'Amend', 'Total Depth', 'Stacked Lateral Parent Well DP', 'Current Queue']
            if all(str(v) in header_values for v in permit_data.values() if v):
                logger.info(f"Skipping header row (all values are column names): {permit_data}")
                continue
            
            # Use API number as unique identifier if status_no is not available
            # For permits without API numbers, use operator_name + lease_name as unique identifier
            unique_id = permit_data.get('status_no') or permit_data.get('api_no')
            if not unique_id:
                # Create a unique identifier from operator and lease name
                operator = permit_data.get('operator_name', '')
                lease = permit_data.get('lease_name', '')
                if operator and lease:
                    unique_id = f"{operator}_{lease}"
                    logger.debug(f"Using operator+lease as unique ID: {unique_id}")
                else:
                    logger.debug("Skipping permit with no unique identifier")
                    continue
            
            # Check if permit already exists (by status_no, api_no, or operator+lease)
            existing = None
            if permit_data.get('status_no'):
                existing = session.query(Permit).filter_by(status_no=permit_data.get('status_no')).first()
            elif permit_data.get('api_no'):
                existing = session.query(Permit).filter_by(api_no=permit_data.get('api_no')).first()
            else:
                # Check by operator_name + lease_name for permits without API numbers
                operator = permit_data.get('operator_name', '')
                lease = permit_data.get('lease_name', '')
                if operator and lease:
                    existing = session.query(Permit).filter_by(
                        operator_name=operator, 
                        lease_name=lease
                    ).first()
            
            if existing:
                logger.debug(f"Permit {unique_id} already exists, skipping")
                skipped_count += 1
                continue
            
            # Parse and prepare data
            operator_name, operator_number = parse_operator_info(permit_data.get('operator_name'))
            status_date = parse_status_date(permit_data.get('status_date'))
            amend = parse_amend_field(permit_data.get('amend'))
            total_depth = parse_total_depth(permit_data.get('total_depth'))
            
            # Create permit object
            permit = Permit(
                status_date=status_date,
                status_no=permit_data.get('status_no') or permit_data.get('api_no'),  # Use API number if status_no is not available
                api_no=permit_data.get('api_no'),
                operator_name=operator_name,
                operator_number=operator_number,
                lease_name=permit_data.get('lease_name'),
                well_no=permit_data.get('well_no'),
                district=permit_data.get('district'),
                county=permit_data.get('county'),
                wellbore_profile=permit_data.get('wellbore_profile'),
                filing_purpose=permit_data.get('filing_purpose'),
                amend=amend,
                total_depth=total_depth,
                stacked_lateral_parent_well_dp=permit_data.get('stacked_lateral_parent_well_dp'),
                current_queue=permit_data.get('current_queue'),
                # Legacy fields for backward compatibility
                submission_date=status_date,
                # Detail URL for enrichment
                detail_url=permit_data.get('detail_href')
            )
            
            session.add(permit)
            saved_count += 1
            
            if saved_count % 10 == 0:
                logger.info(f"Processed {saved_count} permits...")
        
        # Commit all changes
        session.commit()
        logger.info(f"Successfully saved {saved_count} permits to database")
        if skipped_count > 0:
            logger.info(f"Skipped {skipped_count} existing permits")
        
        return saved_count
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error saving permits to database: {e}")
        raise
    finally:
        session.close()

def main():
    """Main function to scrape and save permits."""
    # Get date range from environment or use today
    begin_date = os.getenv('SCRAPE_BEGIN_DATE', date.today().strftime('%m/%d/%Y'))
    end_date = os.getenv('SCRAPE_END_DATE', date.today().strftime('%m/%d/%Y'))
    max_pages = int(os.getenv('SCRAPE_MAX_PAGES', '10'))  # Increased to ensure we get all pages
    
    logger.info(f"Starting permit scraping: {begin_date} to {end_date}")
    logger.info(f"Max pages: {max_pages}")
    
    try:
        # Initialize scraper
        client = RRCW1Client()
        
        # Scrape permits
        logger.info("Scraping permits...")
        result = client.fetch_all(begin_date, end_date, max_pages)
        
        if not result.get('success'):
            logger.error(f"Scraping failed: {result.get('error', 'Unknown error')}")
            return 1
        
        logger.info(f"Scraped {result['count']} permits using {result['method']} engine")
        
        # Save to database
        if result.get('items'):
            logger.info("Saving permits to database...")
            saved_count = save_permits_to_database(result['items'])
            logger.info(f"Successfully saved {saved_count} permits to database")
        else:
            logger.info("No permits found to save")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
