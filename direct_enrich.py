#!/usr/bin/env python3
"""
Direct enrichment of permits with NULL field names using EnrichmentWorker.
This bypasses the API and directly updates the database.
"""

import os
import sys
import asyncio

# Set the DATABASE_URL
os.environ['DATABASE_URL'] = 'postgresql://postgres:NqDqZtOjqEHJonvpmBtMkVtsalEkeXxF@ballast.proxy.rlwy.net:57963/railway'

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

async def enrich_problem_permits():
    """Directly enrich permits with NULL field names."""
    
    try:
        from db.session import get_session
        from db.models import Permit
        from services.enrichment.worker import EnrichmentWorker
        
        # Problem permits
        status_numbers = ['906213', '910669', '910670', '910671', '910672', '910673', '910677']
        
        print(f"🔄 Starting direct enrichment for {len(status_numbers)} permits...")
        
        # Initialize enrichment worker
        worker = EnrichmentWorker()
        
        with get_session() as session:
            for status_no in status_numbers:
                try:
                    print(f"\n📋 Processing permit {status_no}...")
                    
                    # Find the permit
                    permit = session.query(Permit).filter(Permit.status_no == status_no).first()
                    if not permit:
                        print(f"   ❌ Permit {status_no} not found")
                        continue
                    
                    print(f"   📍 Current field name: {permit.field_name or 'NULL'}")
                    print(f"   🔗 Detail URL: {permit.detail_url}")
                    
                    if not permit.detail_url:
                        print(f"   ❌ No detail URL available")
                        continue
                    
                    # Enrich the permit directly
                    result = worker._enrich_permit(permit, sleep_ms=1000)  # 1 second delay between requests
                    
                    if result.get('field_name'):
                        print(f"   ✅ Found field name: {result['field_name']}")
                        
                        # Update the permit in database
                        worker._update_permit_in_db(permit, result)
                        print(f"   💾 Updated in database")
                        
                    else:
                        print(f"   ⚠️  No field name extracted")
                        print(f"   📊 Parse status: {result.get('w1_parse_status', 'unknown')}")
                        
                except Exception as e:
                    print(f"   ❌ Error processing {status_no}: {e}")
                    continue
        
        print(f"\n🎉 Direct enrichment completed!")
        print(f"💡 Check your PostgreSQL database to see the updated field names")
        
    except Exception as e:
        print(f"❌ Direct enrichment failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(enrich_problem_permits())
