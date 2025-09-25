#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db.session import get_session
from db.models import Permit
from datetime import date

def check_midnight_bourbon():
    """Check the MIDNIGHT BOURBON B permit parsing."""
    
    print("🔍 CHECKING MIDNIGHT BOURBON B PERMIT")
    print("=" * 50)
    
    target_date = date(2025, 9, 24)
    
    with get_session() as session:
        # Find MIDNIGHT BOURBON B permit
        midnight_bourbon = session.query(Permit).filter(
            Permit.status_date == target_date,
            Permit.lease_name.ilike('%MIDNIGHT BOURBON B%')
        ).first()
        
        if not midnight_bourbon:
            print("❌ MIDNIGHT BOURBON B permit not found")
            return
        
        print(f"📊 Found permit: {midnight_bourbon.status_no}")
        print(f"   Lease Name: {midnight_bourbon.lease_name}")
        print(f"   Detail URL: {midnight_bourbon.detail_url}")
        print(f"   PDF URL: {midnight_bourbon.w1_pdf_url}")
        print(f"   Parse Status: {midnight_bourbon.w1_parse_status}")
        print()
        
        print("📋 CURRENT PARSED VALUES:")
        print(f"   Section: {midnight_bourbon.section}")
        print(f"   Block: {midnight_bourbon.block}")
        print(f"   Survey: {midnight_bourbon.survey}")
        print(f"   Abstract No: {midnight_bourbon.abstract_no}")
        print(f"   Acres: {midnight_bourbon.acres}")
        print(f"   Field Name: {midnight_bourbon.field_name}")
        print(f"   Reservoir Well Count: {midnight_bourbon.reservoir_well_count}")
        print(f"   Parse Confidence: {midnight_bourbon.w1_parse_confidence}")
        
        if midnight_bourbon.w1_text_snippet:
            print(f"\n📄 PDF Text Snippet:")
            print(f"   {midnight_bourbon.w1_text_snippet[:500]}...")

if __name__ == "__main__":
    check_midnight_bourbon()
