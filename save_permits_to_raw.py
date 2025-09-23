#!/usr/bin/env python3
"""
Script to scrape permits and save them to the permits_raw table.
This is the new approach that separates raw data ingestion from normalization.
"""

import os
import sys
import logging
from datetime import datetime, date
from typing import List, Dict, Any

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.scraper.rrc_w1 import RRCW1Client
from app.ingest import insert_raw_record, get_raw_record_count
from app.db import healthcheck

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def save_permits_to_raw(permits: List[Dict[str, Any]], source_url: str) -> int:
    """Save permits to permits_raw table, returning count of saved permits."""
    if not permits:
        logger.info("No permits to save")
        return 0
    
    saved_count = 0
    skipped_count = 0
    
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
        
        # Save to raw table
        try:
            success = insert_raw_record(
                source_url=source_url,
                payload=permit_data,
                status='new'
            )
            
            if success:
                saved_count += 1
                logger.debug(f"Saved permit {i+1} to raw table")
            else:
                skipped_count += 1
                logger.debug(f"Skipped duplicate permit {i+1}")
                
        except Exception as e:
            logger.error(f"Error saving permit {i+1}: {e}")
            continue
        
        if saved_count % 10 == 0:
            logger.info(f"Processed {saved_count} permits...")
    
    logger.info(f"Successfully saved {saved_count} permits to raw table")
    if skipped_count > 0:
        logger.info(f"Skipped {skipped_count} duplicate permits")
    
    return saved_count

def main():
    """Main function to scrape and save permits to raw table."""
    # Test database connection first
    if not healthcheck():
        logger.error("Database connection failed")
        return 1
    
    # Get date range from environment or use today
    begin_date = os.getenv('SCRAPE_BEGIN_DATE', date.today().strftime('%m/%d/%Y'))
    end_date = os.getenv('SCRAPE_END_DATE', date.today().strftime('%m/%d/%Y'))
    max_pages = int(os.getenv('SCRAPE_MAX_PAGES', '10'))
    
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
        
        # Save to raw table
        if result.get('items'):
            logger.info("Saving permits to raw table...")
            source_url = f"RRC W-1 Search: {begin_date} to {end_date}"
            saved_count = save_permits_to_raw(result['items'], source_url)
            logger.info(f"Successfully saved {saved_count} permits to raw table")
            
            # Show raw table stats
            raw_stats = get_raw_record_count()
            logger.info(f"Raw table stats: {raw_stats}")
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
