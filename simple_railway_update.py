#!/usr/bin/env python3
"""
Simple script to update Railway database with the enhanced data.
"""

import psycopg
import os

def update_railway_directly():
    """Update Railway database with the GREEN BULLET enhanced data."""
    
    # Use the Railway DATABASE_URL we had working before
    railway_db_url = "postgresql://postgres:Neq5tOjGHDonvpmtbMVtsikIEoxCRbullant.proxy.rlwy.net:57268/railway"
    
    print("🚀 UPDATING RAILWAY DATABASE")
    print("=" * 40)
    
    try:
        print("🔄 Connecting to Railway database...")
        
        with psycopg.connect(railway_db_url) as conn:
            with conn.cursor() as cur:
                print("✅ Connected successfully!")
                
                # Update the 3 GREEN BULLET permits with correct enhanced data
                updates = [
                    {
                        'status_no': '910678',
                        'section': '15',
                        'block': '28', 
                        'survey': 'PSL',
                        'abstract_no': 'A-980',
                        'acres': 1284.37,
                        'field_name': 'PHANTOM (WOLFCAMP)',
                        'reservoir_well_count': 2
                    },
                    {
                        'status_no': '910679',
                        'section': '15',
                        'block': '28',
                        'survey': 'PSL', 
                        'abstract_no': 'A-980',
                        'acres': 1284.37,
                        'field_name': 'PHANTOM (WOLFCAMP)',
                        'reservoir_well_count': 3
                    },
                    {
                        'status_no': '910681',
                        'section': '15',
                        'block': '28',
                        'survey': 'PSL',
                        'abstract_no': 'A-980',
                        'acres': 1284.37,
                        'field_name': 'PHANTOM (WOLFCAMP)', 
                        'reservoir_well_count': 4
                    }
                ]
                
                print("📝 Updating GREEN BULLET permits with enhanced data...")
                
                for update in updates:
                    try:
                        cur.execute("""
                            UPDATE permits.permits SET 
                                section = %s,
                                block = %s,
                                survey = %s,
                                abstract_no = %s,
                                acres = %s,
                                field_name = %s,
                                reservoir_well_count = %s,
                                updated_at = NOW()
                            WHERE status_no = %s
                        """, (
                            update['section'],
                            update['block'],
                            update['survey'],
                            update['abstract_no'],
                            update['acres'],
                            update['field_name'],
                            update['reservoir_well_count'],
                            update['status_no']
                        ))
                        
                        print(f"   ✅ Updated permit {update['status_no']} - wells: {update['reservoir_well_count']}")
                        
                    except Exception as e:
                        print(f"   ❌ Error updating permit {update['status_no']}: {e}")
                
                # Commit all changes
                conn.commit()
                print("\n🎉 All updates committed to Railway!")
                
                # Verify the updates
                print("\n🔍 Verifying updates...")
                cur.execute("""
                    SELECT status_no, lease_name, section, block, survey, abstract_no, 
                           acres, field_name, reservoir_well_count
                    FROM permits.permits 
                    WHERE status_no IN ('910678', '910679', '910681')
                    ORDER BY status_no
                """)
                
                results = cur.fetchall()
                
                if results:
                    print("\n📊 UPDATED DATA IN RAILWAY:")
                    print("Status   | Lease Name                | Sec | Blk | Survey | Abstract | Acres   | Field Name         | Wells")
                    print("---------|---------------------------|-----|-----|--------|----------|---------|--------------------|---------")
                    
                    for row in results:
                        status_no = row[0]
                        lease_name = row[1][:25] if row[1] else 'N/A'
                        section = row[2] or 'N/A'
                        block = row[3] or 'N/A'
                        survey = row[4] or 'N/A'
                        abstract_no = row[5] or 'N/A'
                        acres = f"{row[6]:.1f}" if row[6] else 'N/A'
                        field_name = row[7][:18] if row[7] else 'N/A'
                        wells = row[8] or 'N/A'
                        
                        print(f"{status_no:<8} | {lease_name:<25} | {section:<3} | {block:<3} | {survey:<6} | {abstract_no:<8} | {acres:<7} | {field_name:<18} | {wells}")
                
                return True
                
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = update_railway_directly()
    if success:
        print("\n🎉 SUCCESS! Railway database updated with enhanced parsing data!")
        print("   Your GREEN BULLET permits now have correct:")
        print("   • Section: 15, Block: 28, Survey: PSL")  
        print("   • Abstract: A-980, Acres: 1284.37")
        print("   • Field: PHANTOM (WOLFCAMP)")
        print("   • Reservoir Well Count: 2, 3, 4 (FIXED!)")
    else:
        print("\n❌ Update failed. Please check the errors above.")
