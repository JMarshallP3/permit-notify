#!/usr/bin/env python3
"""
Debug script to understand why we're only getting 21 permits instead of 25.
"""

import os
import sys
import logging
from datetime import date

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.scraper.rrc_w1 import RRCW1Client

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Debug the scraper to see what's happening."""
    begin_date = date.today().strftime('%m/%d/%Y')
    end_date = date.today().strftime('%m/%d/%Y')
    
    logger.info(f"Debugging scraper for: {begin_date} to {end_date}")
    
    try:
        # Initialize scraper
        client = RRCW1Client()
        
        # Scrape permits
        logger.info("Scraping permits...")
        result = client.fetch_all(begin_date, end_date, max_pages=10)
        
        if not result.get('success'):
            logger.error(f"Scraping failed: {result.get('error', 'Unknown error')}")
            return 1
        
        logger.info(f"Scraped {result['count']} permits using {result['method']} engine")
        logger.info(f"Pages processed: {result['pages']}")
        
        # Analyze the results
        items = result.get('items', [])
        logger.info(f"Total items returned: {len(items)}")
        
        # Count permits with and without API numbers
        with_api = 0
        without_api = 0
        header_rows = 0
        
        for i, item in enumerate(items):
            if (item.get('status_no') == 'Status #' or 
                item.get('status_date') == 'Status Date' or
                item.get('operator_name') == 'Operator Name/Number' or
                item.get('api_no') == 'API No.'):
                header_rows += 1
                logger.info(f"Header row {header_rows}: {item}")
            elif item.get('api_no'):
                with_api += 1
            else:
                without_api += 1
        
        logger.info(f"Header rows: {header_rows}")
        logger.info(f"Permits with API numbers: {with_api}")
        logger.info(f"Permits without API numbers: {without_api}")
        logger.info(f"Total valid permits: {with_api + without_api}")
        
        # Show first few items for debugging
        logger.info("First 5 items:")
        for i, item in enumerate(items[:5]):
            logger.info(f"  Item {i+1}: {item}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error in debug: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
