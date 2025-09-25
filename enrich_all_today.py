#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.enrichment.worker import EnrichmentWorker
from db.session import get_session
from db.models import Permit
from datetime import datetime, date

def enrich_all_today():
    """Enrich all permits from today, regardless of current status."""
    
    print("🔄 ENRICHING ALL TODAY'S PERMITS")
    print("=" * 50)
    
    # Get all permits from today
    with get_session() as session:
        # Convert string date to date object
        target_date = date(2025, 9, 24)  # September 24, 2025
        
        today_permits = session.query(Permit).filter(
            Permit.status_date == target_date,
            Permit.detail_url.isnot(None)
        ).order_by(Permit.status_no).all()
        
        print(f"📊 Found {len(today_permits)} permits from today with detail URLs")
        
        # Reset w1_parse_status to force re-enrichment
        for permit in today_permits:
            permit.w1_parse_status = None
            permit.w1_last_enriched_at = None
        
        session.commit()
        session.expunge_all()
    
    print("🔄 Reset all parsing status to force re-enrichment")
    
    # Create worker with faster rate limit for testing
    worker = EnrichmentWorker(rate_limit=2.0)  # 2 requests per second
    
    # Process all permits in batches
    batch_size = 10
    total_processed = 0
    
    while True:
        pending = worker.get_pending_permits(limit=batch_size)
        if not pending:
            break
            
        print(f"\n🔄 Processing batch of {len(pending)} permits...")
        results = worker.run(limit=batch_size, sleep_ms=0)
        
        total_processed += results['processed']
        
        print(f"   ✅ Successful: {results['successful']}")
        print(f"   ⚠️  Partial: {results['partial']}")  
        print(f"   ❌ Failed: {results['failed']}")
        print(f"   📄 No PDF: {results['no_pdf']}")
        
        if results['errors']:
            print(f"   🚨 Errors: {len(results['errors'])}")
            for error in results['errors'][:3]:  # Show first 3 errors
                print(f"      {error}")
    
    print(f"\n🎉 COMPLETED! Processed {total_processed} permits total")

if __name__ == "__main__":
    enrich_all_today()
