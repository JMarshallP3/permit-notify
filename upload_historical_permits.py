#!/usr/bin/env python3
"""
Simple script to upload historical permits from the Excel template.
"""

import sys
import os
import pandas as pd
from datetime import datetime

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db.repo import upsert_permits

def upload_from_excel(excel_file: str = "historical_permits_template.xlsx"):
    """Upload historical permits from Excel file."""
    
    if not os.path.exists(excel_file):
        print(f"❌ Excel file not found: {excel_file}")
        print("💡 Run: python create_excel_template.py to create the template")
        return False
    
    try:
        # Read from 'Your Data' sheet
        print(f"📁 Reading data from {excel_file}...")
        df = pd.read_excel(excel_file, sheet_name='Your Data')
        
        if len(df) == 0:
            print("❌ No data found in 'Your Data' sheet")
            print("💡 Make sure you've filled in the 'Your Data' sheet with your permit data")
            return False
        
        print(f"📊 Found {len(df)} permits to import")
        
        # Convert DataFrame to list of dictionaries
        permits_data = []
        for _, row in df.iterrows():
            permit_dict = {}
            
            # Process each column
            for col, value in row.items():
                if pd.isna(value) or value == '' or str(value).strip() == '':
                    continue
                
                # Handle date fields
                if col == 'status_date':
                    if isinstance(value, str):
                        try:
                            # Try parsing common date formats
                            if '/' in value:
                                permit_dict[col] = datetime.strptime(value, '%m/%d/%Y').date()
                            elif '-' in value:
                                permit_dict[col] = datetime.strptime(value, '%Y-%m-%d').date()
                        except ValueError:
                            print(f"⚠️  Invalid date format: {value}")
                            continue
                    else:
                        permit_dict[col] = value
                
                # Handle boolean fields
                elif col == 'horizontal_wellbore':
                    if isinstance(value, bool):
                        permit_dict[col] = value
                    else:
                        str_val = str(value).lower()
                        permit_dict[col] = str_val in ['true', '1', 'yes', 'horizontal']
                
                # Handle numeric fields
                elif col in ['acres', 'swr_total_depth', 'reservoir_well_count']:
                    try:
                        permit_dict[col] = float(value)
                    except (ValueError, TypeError):
                        continue
                
                # Handle string fields
                else:
                    permit_dict[col] = str(value).strip()
            
            # Check required fields
            if not all(permit_dict.get(field) for field in ['status_no', 'lease_name', 'county']):
                print(f"⚠️  Skipping row with missing required fields: {permit_dict.get('status_no', 'unknown')}")
                continue
            
            # Set metadata
            permit_dict['created_at'] = datetime.now()
            permit_dict['updated_at'] = datetime.now()
            permit_dict['w1_parse_status'] = 'imported'
            permit_dict['w1_parse_confidence'] = 0.8
            
            permits_data.append(permit_dict)
        
        if not permits_data:
            print("❌ No valid permits found to import")
            return False
        
        print(f"✅ Processed {len(permits_data)} valid permits")
        
        # Import to database
        print("🚀 Importing to database...")
        result = upsert_permits(permits_data)
        
        print(f"\n🎉 Import completed!")
        print(f"   ✅ Inserted: {result['inserted']} permits")
        print(f"   🔄 Updated: {result['updated']} permits")
        print(f"   ❌ Errors: {result['errors']} permits")
        
        total_processed = result['inserted'] + result['updated']
        if total_processed > 0:
            print(f"\n📈 Successfully processed {total_processed} historical permits!")
            print("🎯 Check your PermitTracker dashboard to see the historical data")
        
        return True
        
    except Exception as e:
        print(f"💥 Import failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main function."""
    
    excel_file = "historical_permits_template.xlsx"
    
    # Check if a different file was specified
    if len(sys.argv) > 1:
        excel_file = sys.argv[1]
    
    print(f"📋 Historical Permit Importer")
    print(f"📁 Excel file: {excel_file}")
    print(f"" + "="*50)
    
    success = upload_from_excel(excel_file)
    
    if success:
        print(f"\n🎊 Import successful! Your historical permits are now in PermitTracker.")
    else:
        print(f"\n❌ Import failed. Please check the errors above and try again.")

if __name__ == "__main__":
    main()
