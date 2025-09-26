#!/usr/bin/env python3
"""
Re-enrich permits that have placeholder field names.
"""

import requests
import json

def fix_placeholder_field_names():
    """Re-enrich permits with placeholder field names."""
    
    # Permits with placeholder field names
    status_numbers = ['906213', '910669', '910670', '910671', '910672', '910673', '910677']
    
    print(f"🔄 Starting re-enrichment for {len(status_numbers)} permits with placeholder field names...")
    print(f"📋 Permits: {', '.join(status_numbers)}")
    
    try:
        response = requests.post(
            'http://localhost:8000/api/v1/permits/reenrich',
            json={
                'status_numbers': status_numbers,
                'reason': 'Fix placeholder field names - replace (exactly as shown in RRC records)'
            },
            timeout=120
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"\n✅ Re-enrichment API call completed!")
            print(f"📊 {data.get('message', 'No message')}")
            print(f"🎯 Successful: {data.get('successful', 0)}")
            print(f"❌ Failed: {data.get('failed', 0)}")
            
            print(f"\n📋 Detailed Results:")
            for result in data.get('results', []):
                status = result.get('status', 'unknown')
                status_no = result.get('status_no', 'unknown')
                
                if status == 'enriched':
                    print(f"   ✅ {status_no}: Successfully re-enriched")
                elif status == 'failed':
                    error = result.get('error', 'Unknown error')
                    print(f"   ❌ {status_no}: {error}")
                else:
                    print(f"   ⚠️  {status_no}: {status}")
            
            # Wait a moment for processing
            print(f"\n⏳ Waiting 5 seconds for enrichment to complete...")
            import time
            time.sleep(5)
            
            print(f"🔄 Now export updated data to see the real field names!")
            
        else:
            print(f"❌ API returned status {response.status_code}")
            print(f"Response: {response.text}")
            
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to API - is the server running?")
        print("💡 Try: python -m uvicorn app.main:app --reload")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    fix_placeholder_field_names()
