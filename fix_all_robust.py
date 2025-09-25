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

def fix_all_robust():
    """Recheck and fix ALL permits with more robust session handling."""
    
    print("🔧 ROBUST FIX FOR ALL TODAY'S PERMITS")
    print("=" * 60)
    
    target_date = date(2025, 9, 24)
    
    with get_session() as session:
        # Get permits that have existing parsing data (might be wrong)
        permits_with_data = session.query(Permit).filter(
            Permit.status_date == target_date,
            Permit.detail_url.isnot(None),
            Permit.section.isnot(None)  # Has some parsing data
        ).order_by(Permit.status_no).all()
        
        print(f"📊 Found {len(permits_with_data)} permits with existing parsing data")
        
        success_count = 0
        error_count = 0
        
        for i, permit in enumerate(permits_with_data, 1):
            print(f"\n🔄 [{i}/{len(permits_with_data)}] {permit.status_no} - {permit.lease_name[:30]}...")
            print(f"   Current: sec={permit.section}, blk={permit.block}, survey={permit.survey}")
            
            # Create fresh session for each permit
            session_req = requests.Session()
            session_req.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            })
            
            try:
                # Wait before request
                time.sleep(2)
                
                # Get detail page
                detail_response = session_req.get(permit.detail_url, timeout=20)
                if detail_response.status_code != 200:
                    print(f"   ❌ Detail page failed: {detail_response.status_code}")
                    error_count += 1
                    continue
                
                # Parse detail page
                detail_data = parse_detail_page(detail_response.text, permit.detail_url)
                pdf_url = detail_data.get('view_w1_pdf_url')
                
                if not pdf_url:
                    print(f"   📄 No PDF URL found - skipping")
                    continue
                
                print(f"   ✅ PDF URL found")
                
                # Wait before PDF request
                time.sleep(1)
                
                # Get PDF
                pdf_response = session_req.get(
                    pdf_url, 
                    timeout=25,
                    headers={'Referer': permit.detail_url}
                )
                
                if pdf_response.status_code != 200:
                    print(f"   ❌ PDF failed: {pdf_response.status_code}")
                    error_count += 1
                    continue
                
                # Extract and parse PDF text
                pdf_text = extract_text_from_pdf(pdf_response.content)
                if not pdf_text:
                    print(f"   ❌ No PDF text")
                    error_count += 1
                    continue
                
                # Parse PDF fields
                parsed = parse_pdf_fields(pdf_text)
                
                # Check if we got better data than what's stored
                current_confidence = permit.w1_parse_confidence or 0.0
                new_confidence = parsed.get('confidence', 0.0)
                
                print(f"   📊 New: sec={parsed.get('section')}, blk={parsed.get('block')}, survey={parsed.get('survey')}")
                print(f"       wells={parsed.get('reservoir_well_count')}, conf={new_confidence:.2f} (was {current_confidence:.2f})")
                
                # Update if we have better confidence or if current data looks generic
                should_update = (
                    new_confidence > current_confidence or
                    permit.section == permit.block == permit.survey or  # Generic data (like all "15")
                    permit.section in ['15', '16', '17', '47'] or  # Common generic values
                    new_confidence > 0.5  # Good confidence
                )
                
                if should_update:
                    print(f"   ✅ UPDATING (better data)")
                    
                    permit.reservoir_well_count = parsed.get('reservoir_well_count')
                    permit.section = parsed.get('section')
                    permit.block = parsed.get('block')
                    permit.survey = parsed.get('survey')
                    permit.abstract_no = parsed.get('abstract_no')
                    permit.acres = parsed.get('acres')
                    if not permit.field_name or permit.field_name == '(exactly as shown in RRC records)':
                        permit.field_name = parsed.get('field_name')
                    permit.w1_text_snippet = parsed.get('snippet')
                    permit.w1_pdf_url = pdf_url
                    permit.w1_parse_confidence = new_confidence
                    permit.w1_parse_status = 'success'
                    
                    success_count += 1
                else:
                    print(f"   ⏭️  SKIPPING (current data is better)")
                
                # Commit every 5 permits
                if i % 5 == 0:
                    session.commit()
                    print(f"   💾 Progress saved")
                
            except Exception as e:
                print(f"   ❌ Error: {e}")
                error_count += 1
            
            finally:
                session_req.close()  # Clean up session
        
        # Final commit
        session.commit()
        
        print(f"\n🎉 COMPLETED!")
        print(f"   • Successfully updated: {success_count}")
        print(f"   • Errors: {error_count}")
        print(f"   • Total checked: {len(permits_with_data)}")

if __name__ == "__main__":
    fix_all_robust()
