#!/usr/bin/env python3
"""
Scrape ALL of today's permits with no page limit.
"""

import requests
import json
from datetime import datetime

def scrape_all_today():
    """Scrape ALL permits for today with no page limit."""
    
    print("🚀 SCRAPING ALL TODAY'S PERMITS (NO LIMIT)")
    print("=" * 50)
    
    try:
        # Get today's date in MM/DD/YYYY format
        today = datetime.now().strftime("%m/%d/%Y")
        print(f"📅 Scraping permits for {today}")
        print(f"⏰ This may take a few minutes for all permits...")
        
        # Call the local FastAPI endpoint with NO page limit (pages=None means get all)
        params = {
            "begin": today,
            "end": today,
            # Don't set pages parameter - this means get ALL pages
        }
        
        print(f"🔄 Making API call...")
        response = requests.get("http://localhost:8000/w1/search", params=params, timeout=600)  # 10 minute timeout
        
        if response.status_code == 200:
            data = response.json()
            
            print(f"✅ API call successful!")
            print(f"📊 Results:")
            print(f"   📄 Pages scraped: {data.get('pages', 'N/A')}")
            print(f"   📋 Total permits found: {len(data.get('items', []))}")
            print(f"   🔢 Total count: {data.get('count', 'N/A')}")
            
            # Show database results if available
            if 'database' in data:
                db_info = data['database']
                inserted = db_info.get('inserted', 0)
                updated = db_info.get('updated', 0)
                print(f"   💾 Database: {inserted} new, {updated} updated")
                
                if 'note' in db_info:
                    print(f"   📝 Note: {db_info['note']}")
            
            # Show sample permits with enhanced data
            items = data.get('items', [])
            if items:
                print(f"\n📋 Sample permits (showing first 5):")
                print("   Status   | Lease Name                | District | Well Count")
                print("   ---------|---------------------------|----------|----------")
                
                for i, permit in enumerate(items[:5]):
                    status = permit.get('status_no', 'N/A')[:8]
                    lease = (permit.get('lease_name', 'N/A'))[:25]
                    district = permit.get('district', 'N/A')
                    well_count = permit.get('reservoir_well_count', 'N/A')
                    
                    print(f"   {status:<8} | {lease:<25} | {district:<8} | {well_count}")
                
                if len(items) > 5:
                    print(f"   ... and {len(items) - 5} more permits")
                
                # Check if any have enhanced data
                enhanced_count = sum(1 for p in items if p.get('reservoir_well_count') is not None)
                print(f"\n🎯 Enhanced parsing status:")
                print(f"   📊 {enhanced_count}/{len(items)} permits have reservoir well count data")
                
                if enhanced_count < len(items):
                    print(f"   ⚠️  {len(items) - enhanced_count} permits need enrichment")
                    print(f"   💡 Run enrichment to get enhanced data for remaining permits")
            
            print(f"\n🎉 Scraping complete!")
            print(f"   ✅ All {len(items)} permits from {today} have been processed")
            print(f"   💾 Data stored in Docker database")
            
            return True
            
        else:
            print(f"❌ API call failed: HTTP {response.status_code}")
            try:
                error_data = response.json()
                print(f"Error details: {error_data}")
            except:
                print(f"Response: {response.text[:200]}...")
            return False
            
    except requests.exceptions.Timeout:
        print("❌ Request timed out")
        print("   This can happen with large scraping operations.")
        print("   The scraping might still be running in the background.")
        print("   Check your Docker logs: docker logs permit_notify_app")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = scrape_all_today()
    
    if success:
        print(f"\n🎯 Next steps:")
        print(f"   1. Run enrichment to get enhanced PDF data:")
        print(f"      python -c \"import requests; print(requests.post('http://localhost:8000/enrich/run?n=50').json())\"")
        print(f"   2. Check results in database")
        print(f"   3. Push enhanced data to Railway if needed")
    else:
        print(f"\n❌ Scraping failed. Check Docker containers are running:")
        print(f"   docker-compose ps")
