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

def debug_midnight_bourbon():
    """Debug the MIDNIGHT BOURBON B permit parsing."""
    
    print("🔍 DEBUGGING MIDNIGHT BOURBON B PERMIT")
    print("=" * 50)
    
    target_date = date(2025, 9, 24)
    
    with get_session() as session:
        midnight_bourbon = session.query(Permit).filter(
            Permit.status_date == target_date,
            Permit.lease_name.ilike('%MIDNIGHT BOURBON B%')
        ).first()
        
        if not midnight_bourbon:
            print("❌ Permit not found")
            return
        
        print(f"🔄 Processing {midnight_bourbon.status_no} - {midnight_bourbon.lease_name}")
        
        session_req = requests.Session()
        session_req.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        try:
            # Get detail page
            detail_response = session_req.get(midnight_bourbon.detail_url, timeout=15)
            if detail_response.status_code != 200:
                print(f"   ❌ Detail page failed: {detail_response.status_code}")
                return
            
            print(f"   ✅ Detail page loaded")
            
            # Parse detail page
            detail_data = parse_detail_page(detail_response.text, midnight_bourbon.detail_url)
            pdf_url = detail_data.get('view_w1_pdf_url')
            
            if not pdf_url:
                print(f"   ❌ No PDF URL found")
                return
            
            print(f"   ✅ PDF URL found: {pdf_url}")
            
            # Get PDF
            time.sleep(1)
            pdf_response = session_req.get(
                pdf_url, 
                timeout=20,
                headers={'Referer': midnight_bourbon.detail_url}
            )
            
            if pdf_response.status_code != 200:
                print(f"   ❌ PDF failed: {pdf_response.status_code}")
                return
            
            print(f"   ✅ PDF downloaded ({len(pdf_response.content)} bytes)")
            
            # Extract text
            pdf_text = extract_text_from_pdf(pdf_response.content)
            if not pdf_text:
                print(f"   ❌ No text extracted from PDF")
                return
            
            print(f"   ✅ PDF text extracted ({len(pdf_text)} chars)")
            
            # Show first 1000 characters of PDF text
            print(f"\n📄 PDF TEXT PREVIEW:")
            print("=" * 60)
            print(pdf_text[:1000])
            print("=" * 60)
            
            # Parse PDF fields
            parsed = parse_pdf_fields(pdf_text)
            
            print(f"\n📊 PARSED RESULTS:")
            print(f"   Section: {parsed.get('section')}")
            print(f"   Block: {parsed.get('block')}")
            print(f"   Survey: {parsed.get('survey')}")
            print(f"   Abstract No: {parsed.get('abstract_no')}")
            print(f"   Acres: {parsed.get('acres')}")
            print(f"   Field Name: {parsed.get('field_name')}")
            print(f"   Reservoir Well Count: {parsed.get('reservoir_well_count')}")
            print(f"   Confidence: {parsed.get('confidence')}")
            
            # Update the permit with correct data
            print(f"\n🔧 UPDATING PERMIT WITH CORRECT DATA...")
            midnight_bourbon.reservoir_well_count = parsed.get('reservoir_well_count')
            midnight_bourbon.section = parsed.get('section')
            midnight_bourbon.block = parsed.get('block')
            midnight_bourbon.survey = parsed.get('survey')
            midnight_bourbon.abstract_no = parsed.get('abstract_no')
            midnight_bourbon.acres = parsed.get('acres')
            if not midnight_bourbon.field_name:
                midnight_bourbon.field_name = parsed.get('field_name')
            midnight_bourbon.w1_text_snippet = parsed.get('snippet')
            midnight_bourbon.w1_pdf_url = pdf_url
            midnight_bourbon.w1_parse_confidence = parsed.get('confidence', 0.0)
            midnight_bourbon.w1_parse_status = 'success'
            
            session.commit()
            print(f"   ✅ SUCCESS! Updated permit {midnight_bourbon.status_no}")
            
        except Exception as e:
            print(f"   ❌ Error: {e}")

if __name__ == "__main__":
    debug_midnight_bourbon()
