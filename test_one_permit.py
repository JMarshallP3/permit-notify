#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db.session import get_session
from db.models import Permit
from datetime import date
from services.enrichment.detail_parser import parse_detail_page
import requests

def test_one_permit():
    """Test one specific permit to see what's happening."""
    
    print("🔍 TESTING ONE SPECIFIC PERMIT")
    print("=" * 50)
    
    target_date = date(2025, 9, 24)
    
    with get_session() as session:
        # Get one GREEN BULLET permit that we know worked before
        permit = session.query(Permit).filter(
            Permit.status_date == target_date,
            Permit.status_no == '910678'  # GREEN BULLET 28-0-15-22 A
        ).first()
        
        if not permit:
            print("❌ Permit 910678 not found")
            return
        
        print(f"🔄 Testing permit {permit.status_no} - {permit.lease_name}")
        print(f"   Detail URL: {permit.detail_url}")
        
        session_req = requests.Session()
        session_req.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        
        try:
            # Get detail page
            detail_response = session_req.get(permit.detail_url, timeout=15)
            print(f"   Detail response status: {detail_response.status_code}")
            print(f"   Detail response length: {len(detail_response.text)} chars")
            
            if detail_response.status_code == 200:
                # Show first 1000 chars of HTML
                print(f"\n📄 HTML PREVIEW:")
                print("=" * 60)
                print(detail_response.text[:1000])
                print("=" * 60)
                
                # Parse detail page
                detail_data = parse_detail_page(detail_response.text, permit.detail_url)
                print(f"\n📊 PARSED DETAIL DATA:")
                for key, value in detail_data.items():
                    print(f"   {key}: {value}")
                
                pdf_url = detail_data.get('view_w1_pdf_url')
                print(f"\n🔍 PDF URL: {pdf_url}")
                
                if pdf_url:
                    print("✅ PDF URL found!")
                else:
                    print("❌ No PDF URL found")
                    
                    # Check if HTML contains any PDF-related content
                    html_lower = detail_response.text.lower()
                    if 'pdf' in html_lower:
                        print("   But HTML contains 'pdf' text")
                        # Find lines with 'pdf'
                        lines = detail_response.text.split('\n')
                        pdf_lines = [line.strip() for line in lines if 'pdf' in line.lower()]
                        for line in pdf_lines[:5]:  # Show first 5 PDF-related lines
                            print(f"      {line}")
                    
                    if 'w1' in html_lower:
                        print("   HTML contains 'w1' text")
                        w1_lines = [line.strip() for line in lines if 'w1' in line.lower()]
                        for line in w1_lines[:5]:  # Show first 5 W1-related lines
                            print(f"      {line}")
            
        except Exception as e:
            print(f"   ❌ Error: {e}")

if __name__ == "__main__":
    test_one_permit()
