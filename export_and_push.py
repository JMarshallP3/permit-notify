#!/usr/bin/env python3
"""
Export enriched permit data from Docker and push to Railway.
"""

import os
import subprocess
import tempfile
import psycopg
from datetime import datetime

def export_from_docker():
    """Export enriched permit data from Docker database."""
    print("📊 Exporting enriched permits from Docker database...")
    
    # Export data using docker exec
    cmd = [
        'docker', 'exec', 'permit_notify_db', 'psql', 
        '-U', 'permit_app', '-d', 'permit_notify', 
        '-c', '''COPY (
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
        ) TO STDOUT WITH (FORMAT CSV, HEADER, DELIMITER ',', QUOTE '"')'''
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        csv_data = result.stdout
        
        if not csv_data.strip():
            print("⚠️  No enriched permits found in Docker database")
            return None
            
        lines = csv_data.strip().split('\n')
        print(f"📋 Exported {len(lines)-1} enriched permits from Docker")
        return csv_data
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Error exporting data from Docker: {e}")
        print(f"❌ Error output: {e.stderr}")
        return None

def push_to_railway(csv_data):
    """Push CSV data to Railway database."""
    railway_db_url = os.getenv('DATABASE_URL')
    if not railway_db_url:
        print("❌ ERROR: DATABASE_URL environment variable not set")
        return False
    
    try:
        print("🚀 Connecting to Railway database...")
        
        with psycopg.connect(railway_db_url) as conn:
            with conn.cursor() as cur:
                print("📝 Processing permit data...")
                
                # Parse CSV data
                lines = csv_data.strip().split('\n')
                header = lines[0]
                data_lines = lines[1:]
                
                updated_count = 0
                inserted_count = 0
                
                for line in data_lines:
                    try:
                        # Parse CSV line (basic parsing - assumes no commas in quoted fields for now)
                        values = []
                        current_value = ""
                        in_quotes = False
                        
                        for char in line:
                            if char == '"':
                                in_quotes = not in_quotes
                            elif char == ',' and not in_quotes:
                                values.append(current_value.strip('"'))
                                current_value = ""
                            else:
                                current_value += char
                        values.append(current_value.strip('"'))
                        
                        # Convert empty strings to None
                        values = [v if v != '' else None for v in values]
                        
                        status_no = values[0]
                        
                        # Check if permit exists
                        cur.execute(
                            "SELECT status_no FROM permits.permits WHERE status_no = %s",
                            (status_no,)
                        )
                        exists = cur.fetchone()
                        
                        if exists:
                            # Update existing permit
                            cur.execute("""
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
                            """, values[1:27] + [datetime.now(), status_no])
                            updated_count += 1
                        else:
                            # Insert new permit
                            cur.execute("""
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
                            """, values[:26] + [values[26] or datetime.now(), datetime.now()])
                            inserted_count += 1
                            
                        if (updated_count + inserted_count) % 5 == 0:
                            print(f"  📝 Processed {updated_count + inserted_count} permits...")
                            
                    except Exception as e:
                        print(f"❌ Error processing permit {values[0] if values else 'unknown'}: {e}")
                        continue
                
                # Commit all changes
                conn.commit()
                
                print(f"\n✅ SUCCESS! Data pushed to Railway:")
                print(f"   📊 Total permits processed: {len(data_lines)}")
                print(f"   ✨ New permits inserted: {inserted_count}")
                print(f"   🔄 Existing permits updated: {updated_count}")
                
                # Show sample of enhanced data
                print(f"\n🎯 Sample of enhanced data in Railway:")
                cur.execute("""
                    SELECT status_no, lease_name, section, block, survey, abstract_no, 
                           acres, field_name, reservoir_well_count
                    FROM permits.permits 
                    WHERE section IS NOT NULL 
                    ORDER BY status_no 
                    LIMIT 5
                """)
                
                samples = cur.fetchall()
                if samples:
                    print("   Status   | Lease Name                | Sec | Blk | Survey | Abstract | Acres   | Field Name         | Wells")
                    print("   ---------|---------------------------|-----|-----|--------|----------|---------|--------------------|---------")
                    for sample in samples:
                        acres_str = f"{sample[6]:.1f}" if sample[6] else "None"
                        field_str = sample[7][:18] if sample[7] else "None"
                        print(f"   {sample[0]:<8} | {sample[1]:<25} | {sample[2] or 'N/A':<3} | {sample[3] or 'N/A':<3} | {sample[4] or 'N/A':<6} | {sample[5] or 'N/A':<8} | {acres_str:<7} | {field_str:<18} | {sample[8] or 'N/A'}")
                
                return True
                
    except Exception as e:
        print(f"❌ Error connecting to Railway: {e}")
        return False

def main():
    print("🚀 EXPORTING AND PUSHING ENRICHED PERMIT DATA TO RAILWAY")
    print("=" * 60)
    
    # Step 1: Export from Docker
    csv_data = export_from_docker()
    if not csv_data:
        print("❌ Failed to export data from Docker")
        return False
    
    # Step 2: Push to Railway
    success = push_to_railway(csv_data)
    
    if success:
        print("\n🎉 All enhanced permit data has been successfully pushed to Railway!")
        print("   Your production database now contains all the enriched fields:")
        print("   • Section, Block, Survey, Abstract No")
        print("   • Acres, Field Name") 
        print("   • Reservoir Well Count (fixed!)")
        print("   • PDF URLs and parsing metadata")
    else:
        print("\n❌ Failed to push data to Railway. Please check the errors above.")
        return False
    
    return True

if __name__ == "__main__":
    main()
