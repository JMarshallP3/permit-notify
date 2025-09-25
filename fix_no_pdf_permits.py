#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.enrichment.worker import EnrichmentWorker
from db.session import get_session
from db.models import Permit
from datetime import date

def fix_no_pdf_permits():
    """Fix permits that were incorrectly marked as having no PDF."""
    
    print("🔧 FIXING 'NO PDF' PERMITS")
    print("=" * 50)
    
    target_date = date(2025, 9, 24)
    
    with get_session() as session:
        # Get permits marked as 'no_pdf' 
        no_pdf_permits = session.query(Permit).filter(
            Permit.status_date == target_date,
            Permit.w1_parse_status == 'no_pdf',
            Permit.detail_url.isnot(None)
        ).order_by(Permit.status_no).all()
        
        print(f"📊 Found {len(no_pdf_permits)} permits marked as 'no_pdf'")
        
        # Reset their status to force re-processing
        for permit in no_pdf_permits:
            print(f"   Resetting {permit.status_no} - {permit.lease_name}")
            permit.w1_parse_status = None
            permit.w1_last_enriched_at = None
            permit.w1_pdf_url = None  # Clear the old static URL
        
        session.commit()
        session.expunge_all()
    
    print("\n🔄 Reset all 'no_pdf' permits for re-processing")
    
    # Create worker with slower rate to avoid session issues
    worker = EnrichmentWorker(rate_limit=0.5)  # 0.5 requests per second (slower)
    
    # Process permits in smaller batches
    batch_size = 5
    total_processed = 0
    
    while True:
        pending = worker.get_pending_permits(limit=batch_size)
        if not pending:
            break
            
        print(f"\n🔄 Processing batch of {len(pending)} permits...")
        for p in pending:
            print(f"   - {p.status_no}: {p.lease_name}")
        
        results = worker.run(limit=batch_size, sleep_ms=500)  # Extra 500ms delay
        
        total_processed += results['processed']
        
        print(f"   ✅ Successful: {results['successful']}")
        print(f"   ⚠️  Partial: {results['partial']}")  
        print(f"   ❌ Failed: {results['failed']}")
        print(f"   📄 No PDF: {results['no_pdf']}")
        
        if results['errors']:
            print(f"   🚨 Errors: {len(results['errors'])}")
            for error in results['errors'][:2]:  # Show first 2 errors
                print(f"      {error}")
    
    print(f"\n🎉 COMPLETED! Re-processed {total_processed} permits")
    
    # Check results
    with get_session() as session:
        still_no_pdf = session.query(Permit).filter(
            Permit.status_date == target_date,
            Permit.w1_parse_status == 'no_pdf'
        ).count()
        
        successful = session.query(Permit).filter(
            Permit.status_date == target_date,
            Permit.w1_parse_status == 'success'
        ).count()
        
        print(f"\n📊 FINAL STATUS:")
        print(f"   • Still marked as 'no_pdf': {still_no_pdf}")
        print(f"   • Successfully parsed: {successful}")

if __name__ == "__main__":
    fix_no_pdf_permits()
