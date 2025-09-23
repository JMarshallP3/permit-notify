#!/usr/bin/env python3
"""
Test script to run the RRC W-1 scraper locally and see detailed logs.
"""

import sys
import os
import logging

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Set up logging to see all the debug output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

from services.scraper.rrc_w1 import RRCW1Client, EngineRedirectToLogin

def test_scraper():
    """Test the RRC W-1 scraper with debug logging."""
    print("=" * 60)
    print("Testing RRC W-1 Scraper")
    print("=" * 60)
    
    # Create scraper instance
    client = RRCW1Client()
    
    # Test with a broader date range
    begin_date = "08/01/2024"
    end_date = "08/31/2024"
    
    print(f"Testing date range: {begin_date} to {end_date}")
    print("-" * 40)
    
    try:
        # Run the scraper
        result = client.fetch_all(begin_date, end_date, max_pages=1)
        
        print("\n" + "=" * 60)
        print("SCRAPER RESULT:")
        print("=" * 60)
        print(f"Success: {result.get('success', False)}")
        print(f"Count: {result.get('count', 0)}")
        print(f"Pages: {result.get('pages', 0)}")
        print(f"Method: {result.get('method', 'unknown')}")
        
        if result.get('error'):
            print(f"Error: {result['error']}")
        
        if result.get('items'):
            print(f"Items found: {len(result['items'])}")
            for i, item in enumerate(result['items'][:3]):  # Show first 3 items
                print(f"  Item {i+1}: {item}")
        else:
            print("No items found")
            
    except EngineRedirectToLogin as e:
        print(f"LOGIN REDIRECT: {e}")
        print("This is expected - the scraper will fall back to PlaywrightEngine")
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_scraper()