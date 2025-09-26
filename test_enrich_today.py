#!/usr/bin/env python3
"""
Test the enrich/today endpoint to re-enrich recent permits.
"""

import requests

def test_enrich_today():
    """Test enrichment of today's permits."""
    
    print("🔄 Using /enrich/today endpoint to re-enrich recent permits...")
    
    try:
        response = requests.post('http://localhost:8000/enrich/today', timeout=60)
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print("✅ Enrichment completed!")
            print(f"📊 Total permits: {data.get('total_permits', 0)}")
            print(f"🎯 Enriched: {data.get('enriched_count', 0)}")
            print(f"📅 Date: {data.get('date', 'unknown')}")
            
            if data.get('enriched_count', 0) > 0:
                print("\n🎉 Success! Some permits were re-enriched.")
                print("💡 Now export the data again to see if field names are fixed:")
                print("   python csv_to_excel.py")
            else:
                print("\n⚠️  No permits were enriched (they may already be up to date)")
                
        elif response.status_code == 404:
            print("❌ Endpoint not found - server may need restart with latest code")
            print("💡 The server is running an older version without the new endpoints")
            
        else:
            print(f"❌ Status {response.status_code}")
            try:
                error_data = response.json()
                print(f"Error: {error_data}")
            except:
                print(f"Response: {response.text}")
                
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to server")
        print("💡 Make sure the FastAPI server is running:")
        print("   python -m uvicorn app.main:app --reload")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_enrich_today()
