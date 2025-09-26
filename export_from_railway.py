#!/usr/bin/env python3
"""
Export permits from Railway database to Excel.
This connects directly to your Railway PostgreSQL database.
"""

import os
import sys
import pandas as pd
from datetime import datetime

# Railway database URL - replace with your actual Railway DATABASE_URL
RAILWAY_DATABASE_URL = "postgresql+psycopg://postgres:PASSWORD@HOST:PORT/railway"

def export_from_railway():
    """Export permits from Railway database."""
    
    # You can get your Railway DATABASE_URL from:
    # 1. Railway dashboard -> Your project -> Variables tab
    # 2. Or from railway CLI: railway variables
    
    database_url = input("Enter your Railway DATABASE_URL (or press Enter to skip): ").strip()
    
    if not database_url:
        print("💡 To get your Railway DATABASE_URL:")
        print("   1. Go to Railway dashboard")
        print("   2. Open your permit-notify project")
        print("   3. Go to Variables tab")
        print("   4. Copy the DATABASE_URL value")
        print("   5. Run this script again and paste it")
        return
    
    try:
        # Temporarily set the DATABASE_URL
        os.environ['DATABASE_URL'] = database_url
        
        # Now import after setting the environment variable
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        from db.session import get_session
        from db.models import Permit
        
        print("🔄 Connecting to Railway database...")
        
        with get_session() as session:
            # Get all permits
            permits = session.query(Permit).order_by(Permit.status_date.desc()).all()
            
            if not permits:
                print("⚠️ No permits found in Railway database")
                return
            
            print(f"📋 Found {len(permits)} permits in Railway database")
            
            # Convert to Excel data
            data = []
            for p in permits:
                data.append({
                    'ID': p.id,
                    'Status Date': p.status_date,
                    'Status Number': p.status_no,
                    'API Number': p.api_no,
                    'Operator Name': p.operator_name,
                    'Lease Name': p.lease_name,
                    'Well Number': p.well_no,
                    'County': p.county,
                    'District': p.district,
                    'Wellbore Profile': p.wellbore_profile,
                    'Filing Purpose': p.filing_purpose,
                    'Amendment': p.amend,
                    'Total Depth': p.total_depth,
                    'Current Queue': p.current_queue,
                    
                    # Enrichment data
                    'Field Name': p.field_name,
                    'Horizontal Wellbore': p.horizontal_wellbore,
                    'Acres': p.acres,
                    'Section': p.section,
                    'Block': p.block,
                    'Survey': p.survey,
                    'Abstract Number': p.abstract_no,
                    
                    # Parse status
                    'Parse Status': p.w1_parse_status,
                    'Parse Confidence': p.w1_parse_confidence,
                    'Last Enriched': p.w1_last_enriched_at,
                    
                    # URLs
                    'Detail URL': p.detail_url,
                    'PDF URL': p.w1_pdf_url,
                    
                    # Metadata
                    'Created': p.created_at,
                    'Updated': p.updated_at
                })
            
            # Create Excel file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"railway_permits_{timestamp}.xlsx"
            
            df = pd.DataFrame(data)
            
            # Create Excel with multiple sheets for analysis
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # Main data
                df.to_excel(writer, sheet_name='All Permits', index=False)
                
                # Summary by county
                county_summary = df.groupby('County').agg({
                    'ID': 'count',
                    'Field Name': lambda x: x.notna().sum()
                }).rename(columns={'ID': 'Total', 'Field Name': 'With Field Names'})
                county_summary.to_excel(writer, sheet_name='By County')
                
                # Data quality issues
                issues = []
                missing_field = df[df['Field Name'].isna() | (df['Field Name'] == '')]
                if len(missing_field) > 0:
                    issues.append(f"Missing Field Names: {len(missing_field)} permits")
                
                parse_errors = df[df['Parse Status'].isin(['parse_error', 'download_error'])]
                if len(parse_errors) > 0:
                    issues.append(f"Parse Errors: {len(parse_errors)} permits")
                
                if issues:
                    issues_df = pd.DataFrame({'Issues Found': issues})
                    issues_df.to_excel(writer, sheet_name='Issues', index=False)
            
            print(f"✅ Exported to: {filename}")
            print(f"📊 {len(permits)} permits exported from Railway")
            print(f"📁 Open {filename} in Excel to review issues")
            
    except Exception as e:
        print(f"❌ Export failed: {e}")
        print("💡 Make sure your DATABASE_URL is correct")

if __name__ == "__main__":
    export_from_railway()
