#!/usr/bin/env python3
"""Test current scraping to see how many permits are available."""

import requests
from datetime import datetime, timedelta

def test_scrape():
    """Test scraping for today and recent days."""
    
    print("🔍 Testing permit scraping...")
    
    # Test different date ranges
    today = datetime.now()
    dates_to_test = [
        today,
        today - timedelta(days=1),
        today - timedelta(days=2),
        today - timedelta(days=7)
    ]
    
    for test_date in dates_to_test:
        date_str = test_date.strftime("%m/%d/%Y")
        print(f"\n📅 Testing {date_str}...")
        
        try:
            response = requests.get(
                'http://localhost:8000/w1/search',
                params={
                    'begin': date_str,
                    'end': date_str,
                    'pages': 5
                },
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                items = data.get('items', [])
                db_info = data.get('database', {})
                
                print(f"  ✅ Found {len(items)} permits")
                print(f"  📊 Database: {db_info.get('inserted', 0)} new, {db_info.get('updated', 0)} updated")
                
                if len(items) > 0:
                    print(f"  📋 Sample: {items[0].get('operator_name', 'N/A')} - {items[0].get('lease_name', 'N/A')}")
                
            else:
                print(f"  ❌ API returned status {response.status_code}")
                
        except requests.exceptions.ConnectionError:
            print(f"  ❌ Cannot connect to API (is the server running?)")
            break
        except Exception as e:
            print(f"  ❌ Error: {e}")

if __name__ == "__main__":
    test_scrape()
