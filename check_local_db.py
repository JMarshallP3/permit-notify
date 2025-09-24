#!/usr/bin/env python3

import psycopg
import os
from datetime import datetime

# Connect to local Docker database
DATABASE_URL = "postgresql://permit_app:permit_db_password_123@localhost:5432/permit_notify"

def check_local_database():
    print("🔍 CHECKING LOCAL DOCKER DATABASE")
    print("=" * 50)
    
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                # Check total permits for today
                cur.execute("SELECT COUNT(*) FROM permits WHERE status_date = '09/24/2025'")
                total = cur.fetchone()[0]
                print(f"📊 Total permits for 09/24/2025: {total}")
                
                # Check GREEN BULLET permits
                cur.execute("""
                    SELECT status_no, lease_name, section, block, survey, abstract_no, acres, field_name, reservoir_well_count 
                    FROM permits 
                    WHERE lease_name LIKE 'GREEN BULLET%' AND status_date = '09/24/2025'
                    ORDER BY status_no
                """)
                green_bullets = cur.fetchall()
                
                print(f"\n🎯 GREEN BULLET permits found: {len(green_bullets)}")
                if green_bullets:
                    print("\nStatus | Lease Name | Sec | Blk | Survey | Abstract | Acres | Field Name | Wells")
                    print("-" * 85)
                    for row in green_bullets:
                        status_no, lease_name, section, block, survey, abstract_no, acres, field_name, reservoir_well_count = row
                        print(f"{status_no} | {lease_name[:20]:20} | {section or 'NULL':3} | {block or 'NULL':3} | {survey or 'NULL':6} | {abstract_no or 'NULL':8} | {acres or 'NULL':6} | {field_name[:15] if field_name else 'NULL':15} | {reservoir_well_count or 'NULL'}")
                
                # Check parsing status
                cur.execute("""
                    SELECT COUNT(*) as total,
                           COUNT(section) as has_section,
                           COUNT(reservoir_well_count) as has_wells,
                           COUNT(field_name) as has_field
                    FROM permits 
                    WHERE status_date = '09/24/2025'
                """)
                stats = cur.fetchone()
                print(f"\n📈 PARSING STATISTICS:")
                print(f"   • Total permits: {stats[0]}")
                print(f"   • With section data: {stats[1]}")
                print(f"   • With well count: {stats[2]}")
                print(f"   • With field name: {stats[3]}")
                
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    check_local_database()
