#!/usr/bin/env python3
"""
Simple reparse script for problematic permits.
"""

import sys
import os
import logging
import requests
from datetime import datetime

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.enrichment.detail_parser import parse_detail_page
from services.enrichment.pdf_parse import extract_text_from_pdf, parse_pdf_fields
from db.session import get_session
from db.models import Permit

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def reparse_permit(status_no: str):
    """Reparse a specific permit by status number."""
    
    # Get permit from database
    with get_session() as session:
        permit = session.query(Permit).filter(Permit.status_no == status_no).first()
        if not permit:
            print(f"❌ Permit {status_no} not found in database")
            return False
        
        # Extract data we need before the session closes
        lease_name = permit.lease_name
        detail_url = permit.detail_url
        current_section = permit.section
        current_block = permit.block
        current_survey = permit.survey
        current_abstract = permit.abstract_no
        current_acres = permit.acres
        current_field = permit.field_name
        current_well_count = permit.reservoir_well_count
        
        print(f"🔍 Found permit: {status_no} - {lease_name}")
        print(f"📄 Detail URL: {detail_url}")
        
        # Show current parsing status
        print(f"\n📊 Current parsing status:")
        print(f"   Section: {current_section}")
        print(f"   Block: {current_block}")
        print(f"   Survey: {current_survey}")
        print(f"   Abstract: {current_abstract}")
        print(f"   Acres: {current_acres}")
        print(f"   Field: {current_field}")
        print(f"   Reservoir Well Count: {current_well_count}")
    
    print(f"\n🔄 Starting reparse process...")
    
    try:
        # Parse detail page with fresh session
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        print("📄 Parsing detail page...")
        # First get the HTML content
        response = session.get(detail_url, timeout=30)
        if response.status_code != 200:
            print(f"❌ Failed to fetch detail page: {response.status_code}")
            return False
        
        detail_data = parse_detail_page(response.text, detail_url)
        
        if not detail_data:
            print("❌ Failed to parse detail page")
            return False
        
        print(f"✅ Detail page parsed: {len(detail_data)} fields")
        
        # Try to get PDF data
        pdf_data = {}
        if detail_data.get('pdf_url'):
            try:
                print("📑 Downloading and parsing PDF...")
                pdf_response = session.get(detail_data['pdf_url'], timeout=30)
                if pdf_response.status_code == 200:
                    pdf_text = extract_text_from_pdf(pdf_response.content)
                    if pdf_text:
                        pdf_data = parse_pdf_fields(pdf_text)
                        print(f"✅ PDF parsed: {len(pdf_data)} fields")
                    else:
                        print("⚠️  PDF text extraction failed")
                else:
                    print(f"⚠️  PDF download failed: {pdf_response.status_code}")
            except Exception as e:
                print(f"⚠️  PDF parsing error: {e}")
        else:
            print("ℹ️  No PDF URL found")
        
        # Combine data
        combined_data = {**detail_data, **pdf_data}
        
        print(f"\n📊 Parsed data:")
        for field, value in combined_data.items():
            if value is not None:
                print(f"   {field}: {value}")
        
        # Update database
        with get_session() as session:
            permit = session.query(Permit).filter(Permit.status_no == status_no).first()
            if permit:
                # Update fields
                updated_fields = []
                for field, value in combined_data.items():
                    if hasattr(permit, field) and value is not None:
                        old_value = getattr(permit, field)
                        if old_value != value:
                            setattr(permit, field, value)
                            updated_fields.append(f"{field}: {old_value} → {value}")
                
                # Set parsing metadata
                permit.w1_parse_status = 'success'
                permit.w1_parse_confidence = len([v for v in combined_data.values() if v is not None]) / 10.0
                permit.updated_at = datetime.now()
                
                session.commit()
                
                print(f"\n✅ Updated database:")
                for field_update in updated_fields:
                    print(f"   {field_update}")
                
                print(f"   Parse confidence: {permit.w1_parse_confidence:.2f}")
                
                return True
            else:
                print("❌ Could not find permit to update")
                return False
                
    except Exception as e:
        print(f"❌ Parsing error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python simple_reparse.py <status_no>")
        print("\nExample permits that need reparsing:")
        print("  910711 - STATE MAYFLY UNIT")
        print("  910712 - CLAY PASTURE -B- STATE UNIT") 
        print("  910713 - STATE MAYFLY UNIT")
        print("  910714 - STATE MAYFLY UNIT")
        print("  910715 - UL GOLD A")
        sys.exit(1)
    
    status_no = sys.argv[1]
    
    try:
        success = reparse_permit(status_no)
        if success:
            print(f"\n🎉 Successfully reparsed permit {status_no}")
        else:
            print(f"\n❌ Failed to reparse permit {status_no}")
    except Exception as e:
        print(f"\n💥 Error reparsing permit {status_no}: {e}")
        import traceback
        traceback.print_exc()
