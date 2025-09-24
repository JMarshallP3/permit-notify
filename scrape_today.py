#!/usr/bin/env python3
"""
Scrape today's permits and show results.
"""

import requests
import json
from datetime import datetime

def scrape_today():
    """Scrape today's permits using the local API."""
    
    print("🚀 SCRAPING TODAY'S PERMITS")
    print("=" * 40)
    
    try:
        # Get today's date in MM/DD/YYYY format
        today = datetime.now().strftime("%m/%d/%Y")
        print(f"🔄 Calling local API to scrape permits for {today}...")
        
        # Call the local FastAPI endpoint with required date parameters
        params = {
            "begin": today,
            "end": today
        }
        response = requests.get("http://localhost:8000/w1/search", params=params, timeout=300)
        
        if response.status_code == 200:
            data = response.json()
            
            print(f"✅ API call successful!")
            print(f"📊 Found {len(data.get('items', []))} permits")
            
            # Show database results if available
            if 'database' in data:
                db_info = data['database']
                print(f"💾 Database: {db_info.get('inserted', 0)} inserted, {db_info.get('updated', 0)} updated")
            
            # Show first few permits
            items = data.get('items', [])
            if items:
                print(f"\n📋 Sample permits found:")
                for i, permit in enumerate(items[:3]):
                    print(f"   {i+1}. {permit.get('status_no')} - {permit.get('lease_name', 'N/A')}")
                
                if len(items) > 3:
                    print(f"   ... and {len(items) - 3} more permits")
            
            print(f"\n🎉 Scraping complete! All permits processed with enhanced parsing.")
            return True
            
        else:
            print(f"❌ API call failed: HTTP {response.status_code}")
            print(f"Response: {response.text[:200]}...")
            return False
            
    except requests.exceptions.Timeout:
        print("❌ Request timed out (this can happen with large scraping operations)")
        print("The scraping might still be running in the background.")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    scrape_today()
