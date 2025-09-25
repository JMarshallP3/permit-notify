#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db.session import get_session
from db.models import Permit
from datetime import date

def check_results():
    """Check the parsing results in Railway database."""
    
    print("🔍 CHECKING RAILWAY DATABASE RESULTS")
    print("=" * 50)
    
    target_date = date(2025, 9, 24)  # September 24, 2025
    
    with get_session() as session:
        # Get all permits from today
        all_permits = session.query(Permit).filter(
            Permit.status_date == target_date
        ).order_by(Permit.status_no).all()
        
        print(f"📊 Total permits for today: {len(all_permits)}")
        
        # Count parsing statistics
        with_parsing = [p for p in all_permits if p.reservoir_well_count is not None or p.section is not None]
        with_pdf = [p for p in all_permits if p.w1_pdf_url is not None]
        successful = [p for p in all_permits if p.w1_parse_status == 'success']
        
        print(f"   • With PDF URLs: {len(with_pdf)}")
        print(f"   • Successfully parsed: {len(successful)}")
        print(f"   • With enhanced data: {len(with_parsing)}")
        
        # Show GREEN BULLET permits specifically
        green_bullets = [p for p in all_permits if p.lease_name and 'GREEN BULLET' in p.lease_name.upper()]
        
        print(f"\n🎯 GREEN BULLET permits: {len(green_bullets)}")
        if green_bullets:
            print("\nStatus | Lease Name | Sec | Blk | Survey | Abstract | Acres | Field Name | Wells")
            print("-" * 90)
            for p in green_bullets:
                print(f"{p.status_no} | {(p.lease_name or '')[:20]:20} | {p.section or 'NULL':3} | {p.block or 'NULL':3} | {(p.survey or '')[:6]:6} | {(p.abstract_no or '')[:8]:8} | {p.acres or 'NULL':6} | {(p.field_name or '')[:15]:15} | {p.reservoir_well_count or 'NULL'}")
        
        # Show some other successfully parsed permits
        other_success = [p for p in successful if not (p.lease_name and 'GREEN BULLET' in p.lease_name.upper())][:5]
        if other_success:
            print(f"\n📋 Other successfully parsed permits (showing first 5):")
            print("\nStatus | Lease Name | Sec | Blk | Survey | Abstract | Acres | Field Name | Wells")
            print("-" * 90)
            for p in other_success:
                print(f"{p.status_no} | {(p.lease_name or '')[:20]:20} | {p.section or 'NULL':3} | {p.block or 'NULL':3} | {(p.survey or '')[:6]:6} | {(p.abstract_no or '')[:8]:8} | {p.acres or 'NULL':6} | {(p.field_name or '')[:15]:15} | {p.reservoir_well_count or 'NULL'}")

if __name__ == "__main__":
    check_results()
