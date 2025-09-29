"""
Scrape RRC permits from September 22, 2025 through today.
This will get all permits with their correct status_date and detail_url values.
"""

import os
import sys
from datetime import datetime, date, timedelta
import logging

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from services.scraper.rrc_w1 import RRCW1Client
from save_permits_to_db import save_permits_to_database

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def scrape_date_range(start_date: date, end_date: date):
    """Scrape RRC permits for a date range."""
    
    print(f"🚀 Starting RRC scrape from {start_date} to {end_date}")
    
    # Initialize scraper
    scraper = RRCW1Client()
    
    current_date = start_date
    total_permits = 0
    
    while current_date <= end_date:
        print(f"\n📅 Scraping permits for {current_date}")
        
        try:
            # Format date for RRC search (MM/DD/YYYY)
            date_str = current_date.strftime("%m/%d/%Y")
            
            # Search RRC for permits on this date
            result = scraper.fetch_all(
                begin=date_str,
                end=date_str,
                max_pages=None  # Get all permits for the day
            )
            
            permits = result.get('permits', [])
            
            if permits:
                print(f"   📋 Found {len(permits)} permits for {current_date}")
                
                # Save permits to database
                saved_count = save_permits_to_database(permits)
                print(f"   💾 Saved {saved_count} new permits")
                
                total_permits += saved_count
            else:
                print(f"   📋 No permits found for {current_date}")
            
        except Exception as e:
            logger.error(f"Error scraping {current_date}: {e}")
            print(f"   ❌ Error scraping {current_date}: {e}")
            continue
        
        # Move to next date
        current_date += timedelta(days=1)
    
    print(f"\n🎉 Scraping completed!")
    print(f"📊 Total permits scraped: {total_permits}")
    print(f"📅 Date range: {start_date} to {end_date}")
    
    return total_permits

def main():
    """Main function to scrape September 22 through today."""
    
    # Check if DATABASE_URL is set
    if not os.getenv('DATABASE_URL'):
        print("❌ DATABASE_URL environment variable is required")
        print("Set it with: $env:DATABASE_URL=\"postgresql://postgres:HZJNGJlWKlxhOFJJOGdNYzFyWmNhTVJJ@roundhouse.proxy.rlwy.net:18685/railway\"")
        return False
    
    # Define date range
    start_date = date(2025, 9, 22)  # September 22, 2025
    end_date = date.today()         # Today
    
    print("🎯 RRC Permit Scraper - Date Range Mode")
    print(f"📅 Start Date: {start_date}")
    print(f"📅 End Date: {end_date}")
    print(f"📊 Total Days: {(end_date - start_date).days + 1}")
    
    # Confirm before starting
    print("\n⚠️  This will scrape RRC permits and add them to your database.")
    print("🔄 Existing permits with the same status_no will be skipped.")
    
    try:
        total_permits = scrape_date_range(start_date, end_date)
        
        if total_permits > 0:
            print(f"\n✅ Successfully scraped {total_permits} permits!")
            print("🎯 Your database now has:")
            print("   - Excel data with correct status_date values")
            print("   - Fresh RRC data with detail_url links")
            print("   - Complete permit information")
        else:
            print("\n⚠️  No new permits were added (they may already exist)")
        
        return True
        
    except Exception as e:
        print(f"\n💥 Scraping failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
