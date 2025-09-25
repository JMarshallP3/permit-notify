#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db.session import get_session
from db.models import Permit
from datetime import date
from services.enrichment.detail_parser import parse_detail_page
from services.enrichment.pdf_parse import extract_text_from_pdf, parse_pdf_fields
import requests
import time

def fix_all_today_permits():
    """Recheck and fix ALL permits from today with fresh parsing."""
    
    print("🔧 FIXING ALL TODAY'S PERMITS WITH FRESH PARSING")
    print("=" * 60)
    
    target_date = date(2025, 9, 24)
    
    with get_session() as session:
        # Get ALL permits from today that have detail URLs
        all_permits = session.query(Permit).filter(
            Permit.status_date == target_date,
            Permit.detail_url.isnot(None)
        ).order_by(Permit.status_no).all()
        
        print(f"📊 Found {len(all_permits)} permits from today with detail URLs")
        
        session_req = requests.Session()
        session_req.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        
        success_count = 0
        no_pdf_count = 0
        error_count = 0
        
        for i, permit in enumerate(all_permits, 1):
            print(f"\n🔄 [{i}/{len(all_permits)}] Processing {permit.status_no} - {permit.lease_name[:40]}...")
            
            try:
                # Get detail page
                detail_response = session_req.get(permit.detail_url, timeout=15)
                if detail_response.status_code != 200:
                    print(f"   ❌ Detail page failed: {detail_response.status_code}")
                    permit.w1_parse_status = 'download_error'
                    error_count += 1
                    continue
                
                # Parse detail page
                detail_data = parse_detail_page(detail_response.text, permit.detail_url)
                pdf_url = detail_data.get('view_w1_pdf_url')
                
                if not pdf_url:
                    print(f"   📄 No PDF URL found")
                    permit.w1_parse_status = 'no_pdf'
                    permit.w1_pdf_url = None
                    no_pdf_count += 1
                    continue
                
                # Wait a bit before PDF request
                time.sleep(0.5)
                
                # Get PDF with proper headers
                pdf_response = session_req.get(
                    pdf_url, 
                    timeout=20,
                    headers={'Referer': permit.detail_url}
                )
                
                if pdf_response.status_code != 200:
                    print(f"   ❌ PDF failed: {pdf_response.status_code}")
                    permit.w1_parse_status = 'download_error'
                    error_count += 1
                    continue
                
                # Extract text
                pdf_text = extract_text_from_pdf(pdf_response.content)
                if not pdf_text:
                    print(f"   ❌ No text extracted from PDF")
                    permit.w1_parse_status = 'parse_error'
                    error_count += 1
                    continue
                
                # Parse PDF fields
                parsed = parse_pdf_fields(pdf_text)
                
                # Show what was parsed (abbreviated)
                print(f"   ✅ Parsed: sec={parsed.get('section')}, blk={parsed.get('block')}, survey={parsed.get('survey')[:10] if parsed.get('survey') else None}")
                print(f"       wells={parsed.get('reservoir_well_count')}, conf={parsed.get('confidence', 0):.2f}")
                
                # Update permit with parsed data
                permit.reservoir_well_count = parsed.get('reservoir_well_count')
                permit.section = parsed.get('section')
                permit.block = parsed.get('block')
                permit.survey = parsed.get('survey')
                permit.abstract_no = parsed.get('abstract_no')
                permit.acres = parsed.get('acres')
                if not permit.field_name:  # Only update if not already set
                    permit.field_name = parsed.get('field_name')
                permit.w1_text_snippet = parsed.get('snippet')
                permit.w1_pdf_url = pdf_url
                permit.w1_parse_confidence = parsed.get('confidence', 0.0)
                permit.w1_parse_status = 'success'
                
                success_count += 1
                
                # Commit every 10 permits to avoid losing progress
                if i % 10 == 0:
                    session.commit()
                    print(f"   💾 Progress saved ({i} permits processed)")
                
                # Small delay between permits to be respectful
                time.sleep(1)
                
            except Exception as e:
                print(f"   ❌ Error: {e}")
                permit.w1_parse_status = 'parse_error'
                error_count += 1
        
        # Final commit
        session.commit()
        
        print(f"\n🎉 COMPLETED!")
        print(f"   • Successfully processed: {success_count}")
        print(f"   • No PDF available: {no_pdf_count}")
        print(f"   • Errors: {error_count}")
        print(f"   • Total permits: {len(all_permits)}")
        print(f"   • All changes committed to database")

if __name__ == "__main__":
    fix_all_today_permits()
