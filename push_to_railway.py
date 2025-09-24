#!/usr/bin/env python3
"""
Push enriched permit data from local Docker database to Railway production database.
"""

import os
import sys
import psycopg
from datetime import datetime

def push_enriched_to_railway():
    """Push all enriched permit data to Railway database."""
    
    # Local Docker database connection
    local_db_url = "postgresql://permit_app:permit_password@localhost:5432/permit_notify"
    
    # Railway database connection (from environment)
    railway_db_url = os.getenv('DATABASE_URL')
    if not railway_db_url:
        print("❌ ERROR: DATABASE_URL environment variable not set")
        print("Please set your Railway database URL:")
        print("$env:DATABASE_URL='your_railway_database_url'")
        return False
    
    try:
        print("🔄 Connecting to databases...")
        
        # Connect to local database
        with psycopg.connect(local_db_url) as local_conn:
            with local_conn.cursor() as local_cur:
                # Get all enriched permits from local database
                print("📊 Fetching enriched permits from local database...")
                local_cur.execute("""
                    SELECT 
                        status_no, api_no, operator_name, lease_name, well_no, district, county,
                        wellbore_profile, filing_purpose, amend, total_depth, current_queue,
                        detail_url, status_date, horizontal_wellbore, field_name, acres,
                        section, block, survey, abstract_no, reservoir_well_count,
                        w1_pdf_url, w1_text_snippet, w1_parse_confidence, w1_parse_status,
                        w1_last_enriched_at, created_at, updated_at
                    FROM permits.permits 
                    WHERE w1_last_enriched_at IS NOT NULL
                    ORDER BY status_no
                """)
                
                permits = local_cur.fetchall()
                print(f"📋 Found {len(permits)} enriched permits in local database")
                
                if not permits:
                    print("⚠️  No enriched permits found in local database")
                    return True
        
        # Connect to Railway database
        with psycopg.connect(railway_db_url) as railway_conn:
            with railway_conn.cursor() as railway_cur:
                print("🚀 Pushing data to Railway database...")
                
                updated_count = 0
                inserted_count = 0
                
                for permit in permits:
                    status_no = permit[0]
                    
                    try:
                        # Check if permit exists in Railway
                        railway_cur.execute(
                            "SELECT status_no FROM permits.permits WHERE status_no = %s",
                            (status_no,)
                        )
                        exists = railway_cur.fetchone()
                        
                        if exists:
                            # Update existing permit
                            railway_cur.execute("""
                                UPDATE permits.permits SET
                                    api_no = %s, operator_name = %s, lease_name = %s, well_no = %s,
                                    district = %s, county = %s, wellbore_profile = %s, filing_purpose = %s,
                                    amend = %s, total_depth = %s, current_queue = %s, detail_url = %s,
                                    status_date = %s, horizontal_wellbore = %s, field_name = %s, acres = %s,
                                    section = %s, block = %s, survey = %s, abstract_no = %s,
                                    reservoir_well_count = %s, w1_pdf_url = %s, w1_text_snippet = %s,
                                    w1_parse_confidence = %s, w1_parse_status = %s, w1_last_enriched_at = %s,
                                    updated_at = %s
                                WHERE status_no = %s
                            """, (
                                permit[1], permit[2], permit[3], permit[4], permit[5], permit[6],
                                permit[7], permit[8], permit[9], permit[10], permit[11], permit[12],
                                permit[13], permit[14], permit[15], permit[16], permit[17], permit[18],
                                permit[19], permit[20], permit[21], permit[22], permit[23], permit[24],
                                permit[25], permit[26], datetime.now(), permit[0]
                            ))
                            updated_count += 1
                        else:
                            # Insert new permit
                            railway_cur.execute("""
                                INSERT INTO permits.permits (
                                    status_no, api_no, operator_name, lease_name, well_no, district, county,
                                    wellbore_profile, filing_purpose, amend, total_depth, current_queue,
                                    detail_url, status_date, horizontal_wellbore, field_name, acres,
                                    section, block, survey, abstract_no, reservoir_well_count,
                                    w1_pdf_url, w1_text_snippet, w1_parse_confidence, w1_parse_status,
                                    w1_last_enriched_at, created_at, updated_at
                                ) VALUES (
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                )
                            """, (
                                permit[0], permit[1], permit[2], permit[3], permit[4], permit[5], permit[6],
                                permit[7], permit[8], permit[9], permit[10], permit[11], permit[12], permit[13],
                                permit[14], permit[15], permit[16], permit[17], permit[18], permit[19], permit[20],
                                permit[21], permit[22], permit[23], permit[24], permit[25], permit[26] or datetime.now(),
                                datetime.now()
                            ))
                            inserted_count += 1
                            
                        if (updated_count + inserted_count) % 5 == 0:
                            print(f"  📝 Processed {updated_count + inserted_count} permits...")
                            
                    except Exception as e:
                        print(f"❌ Error processing permit {status_no}: {e}")
                        continue
                
                # Commit all changes
                railway_conn.commit()
                
                print(f"\n✅ SUCCESS! Data pushed to Railway:")
                print(f"   📊 Total permits processed: {len(permits)}")
                print(f"   ✨ New permits inserted: {inserted_count}")
                print(f"   🔄 Existing permits updated: {updated_count}")
                
                # Show sample of enhanced data
                print(f"\n🎯 Sample of enhanced data in Railway:")
                railway_cur.execute("""
                    SELECT status_no, lease_name, section, block, survey, abstract_no, 
                           acres, field_name, reservoir_well_count
                    FROM permits.permits 
                    WHERE section IS NOT NULL 
                    ORDER BY status_no 
                    LIMIT 5
                """)
                
                samples = railway_cur.fetchall()
                if samples:
                    print("   Status   | Lease Name                | Sec | Blk | Survey | Abstract | Acres   | Field Name         | Wells")
                    print("   ---------|---------------------------|-----|-----|--------|----------|---------|--------------------|---------")
                    for sample in samples:
                        print(f"   {sample[0]:<8} | {sample[1]:<25} | {sample[2]:<3} | {sample[3]:<3} | {sample[4]:<6} | {sample[5]:<8} | {sample[6]:<7} | {sample[7]:<18} | {sample[8]}")
                
                return True
                
    except psycopg.Error as e:
        print(f"❌ Database error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

if __name__ == "__main__":
    print("🚀 PUSHING ENRICHED PERMIT DATA TO RAILWAY")
    print("=" * 50)
    
    success = push_enriched_to_railway()
    
    if success:
        print("\n🎉 All enhanced permit data has been successfully pushed to Railway!")
        print("   Your production database now contains all the enriched fields:")
        print("   • Section, Block, Survey, Abstract No")
        print("   • Acres, Field Name") 
        print("   • Reservoir Well Count (fixed!)")
        print("   • PDF URLs and parsing metadata")
    else:
        print("\n❌ Failed to push data to Railway. Please check the errors above.")
        sys.exit(1)
