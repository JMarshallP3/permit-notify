"""
Import Excel data to replace existing database permits.
This will clear the current permits and import fresh data with correct status_date values.
"""

import pandas as pd
import os
import sys
from datetime import datetime, date
from typing import Dict, Any, List, Optional
import logging

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db.session import get_session
from db.models import Permit
from sqlalchemy import text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_date(date_str: str) -> Optional[date]:
    """Parse date string in various formats."""
    if not date_str or pd.isna(date_str) or str(date_str).strip() == '':
        return None
    
    try:
        date_str = str(date_str).strip()
        
        # Handle YYYY-MM-DD HH:MM:SS format (from Excel datetime)
        if ' ' in date_str and ':' in date_str:
            date_part = date_str.split(' ')[0]  # Take just the date part
            return datetime.strptime(date_part, '%Y-%m-%d').date()
        
        # Handle YYYY-MM-DD format
        if len(date_str) == 10 and date_str.count('-') == 2:
            parts = date_str.split('-')
            if len(parts[0]) == 4:  # YYYY-MM-DD
                return datetime.strptime(date_str, '%Y-%m-%d').date()
            else:  # MM-DD-YYYY
                month, day, year = parts
                return date(int(year), int(month), int(day))
        
        # Handle MM/DD/YYYY format
        if '/' in date_str:
            return datetime.strptime(date_str, '%m/%d/%Y').date()
        
        # Try pandas to_datetime as fallback
        return pd.to_datetime(date_str).date()
        
    except Exception as e:
        logger.warning(f"Could not parse date '{date_str}': {e}")
        return None

def parse_boolean(value: Any) -> Optional[bool]:
    """Parse boolean value."""
    if pd.isna(value) or str(value).strip() == '':
        return None
    
    value_str = str(value).lower().strip()
    if value_str in ['true', 'yes', '1', 't', 'y']:
        return True
    elif value_str in ['false', 'no', '0', 'f', 'n']:
        return False
    return None

def parse_numeric(value: Any) -> Optional[float]:
    """Parse numeric value."""
    if pd.isna(value) or str(value).strip() == '':
        return None
    
    try:
        return float(str(value).strip())
    except:
        return None

def parse_integer(value: Any) -> Optional[int]:
    """Parse integer value."""
    if pd.isna(value) or str(value).strip() == '':
        return None
    
    try:
        return int(float(str(value).strip()))
    except:
        return None

def clean_string(value: Any) -> Optional[str]:
    """Clean string value."""
    if pd.isna(value) or str(value).strip() == '':
        return None
    return str(value).strip()

def import_excel_permits(excel_file: str = "permit_import_template_v2.xlsx"):
    """Import permits from Excel file, replacing existing data."""
    
    if not os.path.exists(excel_file):
        print(f"❌ Excel file not found: {excel_file}")
        return False
    
    print(f"📊 Reading Excel file: {excel_file}")
    
    try:
        # Read Excel file
        df = pd.read_excel(excel_file, sheet_name='Permits')
        print(f"📋 Found {len(df)} rows in Excel file")
        
        # Filter out empty rows (rows where status_no is empty)
        df_filtered = df[df['status_no'].notna() & (df['status_no'] != '')]
        print(f"📋 Found {len(df_filtered)} permits with status_no")
        
        if len(df_filtered) == 0:
            print("❌ No valid permits found in Excel file")
            return False
        
        # Convert DataFrame to permit dictionaries
        permits_data = []
        for _, row in df_filtered.iterrows():
            permit_dict = {
                'status_no': clean_string(row.get('status_no')),
                'api_no': clean_string(row.get('api_no')),
                'operator_name': clean_string(row.get('operator_name')),
                'lease_name': clean_string(row.get('lease_name')),
                'well_no': clean_string(row.get('well_no')),
                'district': parse_integer(row.get('district')),
                'county': clean_string(row.get('county')),
                'wellbore_profile': clean_string(row.get('wellbore_profile')),
                'filing_purpose': clean_string(row.get('filing_purpose')),
                'amend': parse_boolean(row.get('amend')),
                'total_depth': parse_numeric(row.get('total_depth')),
                'current_queue': clean_string(row.get('current_queue')),
                'status_date': parse_date(row.get('status_date')),  # This is the key fix!
                'horizontal_wellbore': clean_string(row.get('horizontal_wellbore')),
                'field_name': clean_string(row.get('field_name')),
                'acres': parse_numeric(row.get('acres')),
                'section': clean_string(row.get('section')),
                'block': clean_string(row.get('block')),
                'survey': clean_string(row.get('survey')),
                'abstract_no': clean_string(row.get('abstract_no')),
                'reservoir_well_count': parse_integer(row.get('reservoir_well_count')),
                'w1_pdf_url': clean_string(row.get('w1_pdf_url')),
                'w1_text_snippet': clean_string(row.get('w1_text_snippet')),
                'w1_parse_confidence': parse_numeric(row.get('w1_parse_confidence')),
                'w1_parse_status': clean_string(row.get('w1_parse_status')),
                # Metadata
                'created_at': datetime.now(),
                'updated_at': datetime.now(),
                'org_id': 'default_org'
            }
            
            # Validate required fields
            if not all([permit_dict.get('status_no'), permit_dict.get('operator_name'), 
                       permit_dict.get('lease_name'), permit_dict.get('county')]):
                print(f"⚠️  Skipping permit with missing required fields: {permit_dict.get('status_no', 'unknown')}")
                continue
            
            permits_data.append(permit_dict)
            
            # Store reservoir learning data if provided
            reservoir_name = clean_string(row.get('reservoir_name'))
            if reservoir_name and permit_dict.get('field_name'):
                # TODO: Store this mapping for the learning system
                logger.info(f"Learning mapping: '{permit_dict['field_name']}' → '{reservoir_name}'")
        
        if not permits_data:
            print("❌ No valid permits to import")
            return False
        
        print(f"✅ Processed {len(permits_data)} valid permits")
        
        # Data integrity checks before import
        print("\n🔍 Running data integrity checks...")
        
        # Check 1: Unique status_no values
        status_nos = [p['status_no'] for p in permits_data if p['status_no']]
        if len(status_nos) != len(set(status_nos)):
            print("❌ INTEGRITY ERROR: Duplicate status_no values found!")
            duplicates = [x for x in status_nos if status_nos.count(x) > 1]
            print(f"   Duplicates: {set(duplicates)}")
            return False
        
        # Check 2: Required fields validation
        missing_required = []
        for i, permit in enumerate(permits_data):
            if not all([permit.get('status_no'), permit.get('operator_name'), 
                       permit.get('lease_name'), permit.get('county')]):
                missing_required.append(f"Row {i+2}: {permit.get('status_no', 'NO_STATUS')}")
        
        if missing_required:
            print("❌ INTEGRITY ERROR: Missing required fields!")
            for missing in missing_required[:5]:  # Show first 5
                print(f"   {missing}")
            if len(missing_required) > 5:
                print(f"   ... and {len(missing_required) - 5} more")
            return False
        
        # Check 3: Date validation
        invalid_dates = []
        for i, permit in enumerate(permits_data):
            if permit.get('status_date') is None and permit.get('status_no'):
                invalid_dates.append(f"Row {i+2}: {permit['status_no']} - missing status_date")
        
        if invalid_dates:
            print("⚠️  WARNING: Some permits missing status_date:")
            for invalid in invalid_dates[:3]:  # Show first 3
                print(f"   {invalid}")
            if len(invalid_dates) > 3:
                print(f"   ... and {len(invalid_dates) - 3} more")
            
            response = input("Continue anyway? (y/N): ")
            if response.lower() != 'y':
                return False
        
        # Check 4: Data distribution validation
        counties = [p['county'] for p in permits_data if p['county']]
        operators = [p['operator_name'] for p in permits_data if p['operator_name']]
        
        print(f"✅ Data validation passed:")
        print(f"   - {len(permits_data)} permits to import")
        print(f"   - {len(set(status_nos))} unique status numbers")
        print(f"   - {len(set(counties))} unique counties")
        print(f"   - {len(set(operators))} unique operators")
        
        # Show date range if we have valid dates
        valid_dates = [p['status_date'] for p in permits_data if p['status_date']]
        if valid_dates:
            print(f"   - Date range: {min(valid_dates)} to {max(valid_dates)}")
        else:
            print(f"   - No valid dates found in data")
        
        # Import to database with transaction safety
        with get_session() as session:
            try:
                print("🗑️  Clearing existing permits...")
                
                # Clear existing permits
                deleted_count = session.execute(text("DELETE FROM permits")).rowcount
                print(f"🗑️  Deleted {deleted_count} existing permits")
                
                print("💾 Importing new permits...")
                
                # Insert new permits
                inserted_count = 0
                failed_permits = []
                
                for permit_data in permits_data:
                    try:
                        permit = Permit(**permit_data)
                        session.add(permit)
                        inserted_count += 1
                        
                        if inserted_count % 10 == 0:
                            print(f"   Processed {inserted_count}/{len(permits_data)} permits...")
                            
                    except Exception as e:
                        logger.error(f"Error preparing permit {permit_data.get('status_no')}: {e}")
                        failed_permits.append(permit_data.get('status_no'))
                        continue
                
                # Final validation before commit
                if failed_permits:
                    print(f"⚠️  {len(failed_permits)} permits failed to prepare:")
                    for failed in failed_permits[:3]:
                        print(f"   {failed}")
                    if len(failed_permits) > 3:
                        print(f"   ... and {len(failed_permits) - 3} more")
                
                if inserted_count == 0:
                    print("❌ No permits were successfully prepared for import")
                    session.rollback()
                    return False
                
                print(f"💾 Committing {inserted_count} permits to database...")
                
                # Commit all changes atomically
                session.commit()
                print(f"✅ Successfully imported {inserted_count} permits")
                
                # Post-import validation
                print("\n🔍 Post-import validation...")
                actual_count = session.execute(text("SELECT COUNT(*) FROM permits")).scalar()
                print(f"✅ Database contains {actual_count} permits")
                
                if actual_count != inserted_count:
                    print(f"⚠️  WARNING: Expected {inserted_count}, but database has {actual_count}")
                
            except Exception as e:
                print(f"❌ Import failed, rolling back changes: {e}")
                session.rollback()
                raise
            
            # Show sample of imported data
            print("\n📊 Sample of imported permits:")
            sample_permits = session.execute(text("""
                SELECT status_no, operator_name, lease_name, county, status_date, created_at 
                FROM permits 
                ORDER BY status_date DESC 
                LIMIT 5
            """)).fetchall()
            
            for permit in sample_permits:
                print(f"   {permit.status_no} | {permit.operator_name[:30]:<30} | {permit.status_date} | Created: {permit.created_at}")
        
        return True
        
    except Exception as e:
        print(f"💥 Import failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🚀 Starting Excel import to database...")
    print("⚠️  WARNING: This will DELETE all existing permits and replace with Excel data!")
    
    # Check if DATABASE_URL is set
    if not os.getenv('DATABASE_URL'):
        print("❌ DATABASE_URL environment variable is required")
        print("Set it with: $env:DATABASE_URL=\"postgresql://postgres:HZJNGJlWKlxhOFJJOGdNYzFyWmNhTVJJ@roundhouse.proxy.rlwy.net:18685/railway\"")
        sys.exit(1)
    
    success = import_excel_permits()
    
    if success:
        print("\n🎉 Import completed successfully!")
        print("✅ Database now contains your Excel data with correct status_date values")
        print("🔄 Ready for RRC scraping to fill in missing details")
    else:
        print("\n❌ Import failed")
        sys.exit(1)
