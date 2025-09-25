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

def debug_parsing():
    """Debug why some permits aren't parsing correctly."""
    
    print("🔍 DEBUGGING PARSING ISSUES")
    print("=" * 50)
    
    target_date = date(2025, 9, 24)
    
    with get_session() as session:
        # Get GREEN BULLET permits
        green_bullets = session.query(Permit).filter(
            Permit.status_date == target_date,
            Permit.lease_name.ilike('%GREEN BULLET%')
        ).order_by(Permit.status_no).all()
        
        print(f"📊 Found {len(green_bullets)} GREEN BULLET permits")
        
        for permit in green_bullets[:3]:  # Check first 3
            print(f"\n🔍 DEBUGGING PERMIT {permit.status_no}")
            print(f"   Lease: {permit.lease_name}")
            print(f"   Detail URL: {permit.detail_url}")
            print(f"   PDF URL: {permit.w1_pdf_url}")
            print(f"   Parse Status: {permit.w1_parse_status}")
            print(f"   Current Values: sec={permit.section}, blk={permit.block}, survey={permit.survey}")
            
            # Check if detail URL works
            if permit.detail_url:
                try:
                    session_req = requests.Session()
                    session_req.headers.update({
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                    })
                    
                    detail_response = session_req.get(permit.detail_url, timeout=10)
                    if detail_response.status_code == 200:
                        print(f"   ✅ Detail URL accessible")
                        
                        # Parse detail page
                        detail_data = parse_detail_page(detail_response.text, permit.detail_url)
                        pdf_url = detail_data.get('view_w1_pdf_url')
                        print(f"   PDF URL from parsing: {pdf_url}")
                        
                        if pdf_url:
                            print(f"   ✅ PDF URL found in detail page")
                            
                            # Try to get PDF
                            pdf_response = session_req.get(pdf_url, timeout=15)
                            if pdf_response.status_code == 200:
                                print(f"   ✅ PDF accessible ({len(pdf_response.content)} bytes)")
                                
                                # Extract and parse PDF text
                                pdf_text = extract_text_from_pdf(pdf_response.content)
                                if pdf_text:
                                    print(f"   ✅ PDF text extracted ({len(pdf_text)} chars)")
                                    
                                    # Show first 500 chars of PDF
                                    print(f"   📄 PDF Preview:")
                                    print(f"      {pdf_text[:500]}...")
                                    
                                    # Parse PDF fields
                                    parsed = parse_pdf_fields(pdf_text)
                                    print(f"   📊 Parsed Results:")
                                    print(f"      Section: {parsed.get('section')}")
                                    print(f"      Block: {parsed.get('block')}")
                                    print(f"      Survey: {parsed.get('survey')}")
                                    print(f"      Abstract: {parsed.get('abstract_no')}")
                                    print(f"      Acres: {parsed.get('acres')}")
                                    print(f"      Field: {parsed.get('field_name')}")
                                    print(f"      Wells: {parsed.get('reservoir_well_count')}")
                                    print(f"      Confidence: {parsed.get('confidence')}")
                                else:
                                    print(f"   ❌ No text extracted from PDF")
                            else:
                                print(f"   ❌ PDF not accessible: {pdf_response.status_code}")
                        else:
                            print(f"   ❌ No PDF URL found in detail page")
                    else:
                        print(f"   ❌ Detail URL not accessible: {detail_response.status_code}")
                        
                except Exception as e:
                    print(f"   ❌ Error accessing URLs: {e}")
            else:
                print(f"   ❌ No detail URL")
        
        # Now check some permits that were marked as "No PDF URL found"
        print(f"\n🔍 CHECKING PERMITS MARKED AS 'NO PDF'")
        no_pdf_permits = session.query(Permit).filter(
            Permit.status_date == target_date,
            Permit.w1_parse_status == 'no_pdf'
        ).limit(3).all()
        
        for permit in no_pdf_permits:
            print(f"\n🔍 PERMIT {permit.status_no} (marked as no PDF)")
            print(f"   Lease: {permit.lease_name}")
            print(f"   Detail URL: {permit.detail_url}")
            print(f"   Stored PDF URL: {permit.w1_pdf_url}")
            
            if permit.detail_url:
                try:
                    session_req = requests.Session()
                    session_req.headers.update({
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                    })
                    
                    detail_response = session_req.get(permit.detail_url, timeout=10)
                    if detail_response.status_code == 200:
                        detail_data = parse_detail_page(detail_response.text, permit.detail_url)
                        pdf_url = detail_data.get('view_w1_pdf_url')
                        print(f"   PDF URL found NOW: {pdf_url}")
                        
                        if pdf_url:
                            print(f"   ✅ PDF URL ACTUALLY EXISTS!")
                        else:
                            print(f"   ❌ Still no PDF URL found")
                            # Show some HTML content to debug
                            html_snippet = detail_response.text[:1000]
                            if 'pdf' in html_snippet.lower():
                                print(f"   📄 HTML contains 'pdf':")
                                lines = html_snippet.split('\n')
                                for line in lines:
                                    if 'pdf' in line.lower():
                                        print(f"      {line.strip()}")
                except Exception as e:
                    print(f"   ❌ Error: {e}")

if __name__ == "__main__":
    debug_parsing()
