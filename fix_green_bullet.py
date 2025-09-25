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

def fix_green_bullet_permits():
    """Manually fix GREEN BULLET permits with detailed debugging."""
    
    print("🎯 FIXING GREEN BULLET PERMITS")
    print("=" * 50)
    
    target_date = date(2025, 9, 24)
    
    with get_session() as session:
        # Get GREEN BULLET permits
        green_bullets = session.query(Permit).filter(
            Permit.status_date == target_date,
            Permit.lease_name.ilike('%GREEN BULLET%')
        ).order_by(Permit.status_no).all()
        
        print(f"📊 Found {len(green_bullets)} GREEN BULLET permits")
        
        session_req = requests.Session()
        session_req.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        
        success_count = 0
        
        for permit in green_bullets:
            print(f"\n🔄 Processing {permit.status_no} - {permit.lease_name}")
            
            try:
                # Get detail page
                detail_response = session_req.get(permit.detail_url, timeout=15)
                if detail_response.status_code != 200:
                    print(f"   ❌ Detail page failed: {detail_response.status_code}")
                    continue
                
                print(f"   ✅ Detail page loaded")
                
                # Parse detail page
                detail_data = parse_detail_page(detail_response.text, permit.detail_url)
                pdf_url = detail_data.get('view_w1_pdf_url')
                
                if not pdf_url:
                    print(f"   ❌ No PDF URL found")
                    permit.w1_parse_status = 'no_pdf'
                    continue
                
                print(f"   ✅ PDF URL found: {pdf_url[:80]}...")
                
                # Wait a bit before PDF request
                time.sleep(1)
                
                # Get PDF with proper headers
                pdf_response = session_req.get(
                    pdf_url, 
                    timeout=20,
                    headers={'Referer': permit.detail_url}
                )
                
                if pdf_response.status_code != 200:
                    print(f"   ❌ PDF failed: {pdf_response.status_code}")
                    permit.w1_parse_status = 'download_error'
                    continue
                
                print(f"   ✅ PDF downloaded ({len(pdf_response.content)} bytes)")
                
                # Extract text
                pdf_text = extract_text_from_pdf(pdf_response.content)
                if not pdf_text:
                    print(f"   ❌ No text extracted from PDF")
                    permit.w1_parse_status = 'parse_error'
                    continue
                
                print(f"   ✅ PDF text extracted ({len(pdf_text)} chars)")
                
                # Parse PDF fields
                parsed = parse_pdf_fields(pdf_text)
                
                print(f"   📊 Parsed: sec={parsed.get('section')}, blk={parsed.get('block')}, survey={parsed.get('survey')}")
                print(f"       abstract={parsed.get('abstract_no')}, acres={parsed.get('acres')}")
                print(f"       field={parsed.get('field_name')}, wells={parsed.get('reservoir_well_count')}")
                print(f"       confidence={parsed.get('confidence')}")
                
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
                print(f"   ✅ SUCCESS! Updated permit {permit.status_no}")
                
                # Small delay between permits
                time.sleep(2)
                
            except Exception as e:
                print(f"   ❌ Error: {e}")
                permit.w1_parse_status = 'parse_error'
        
        # Commit all changes
        session.commit()
        
        print(f"\n🎉 COMPLETED!")
        print(f"   • Successfully processed: {success_count}/{len(green_bullets)}")
        print(f"   • All changes committed to database")

if __name__ == "__main__":
    fix_green_bullet_permits()
