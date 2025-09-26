#!/usr/bin/env python3
"""
Quick export of permit database to Excel.
Simple version that just exports all permits to a basic Excel file.
"""

import os
import sys
import pandas as pd
from datetime import datetime

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from db.session import get_session
    from db.models import Permit
    
    print("🔄 Connecting to database...")
    
    with get_session() as session:
        # Get all permits
        permits = session.query(Permit).order_by(Permit.status_date.desc()).all()
        
        if not permits:
            print("⚠️ No permits found in database")
            print("💡 Make sure you have:")
            print("   1. Set up DATABASE_URL in .env file")
            print("   2. Run the scraper to get some permits")
            sys.exit(1)
        
        print(f"📋 Found {len(permits)} permits")
        
        # Convert to simple list
        data = []
        for p in permits:
            data.append({
                'Status Date': p.status_date,
                'Status Number': p.status_no,
                'API Number': p.api_no,
                'Operator Name': p.operator_name,
                'Lease Name': p.lease_name,
                'County': p.county,
                'District': p.district,
                'Wellbore Profile': p.wellbore_profile,
                'Field Name': p.field_name,
                'Acres': p.acres,
                'Section': p.section,
                'Block': p.block,
                'Survey': p.survey,
                'Parse Status': p.w1_parse_status,
                'Detail URL': p.detail_url,
                'Created': p.created_at,
                'Updated': p.updated_at
            })
        
        # Create Excel file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"permits_{timestamp}.xlsx"
        
        df = pd.DataFrame(data)
        df.to_excel(filename, index=False)
        
        print(f"✅ Exported to: {filename}")
        print(f"📊 {len(permits)} permits exported")
        print(f"📁 Open {filename} in Excel to review")

except ImportError as e:
    print(f"❌ Missing dependency: {e}")
    print("💡 Install required packages:")
    print("   pip install pandas openpyxl")
    
except Exception as e:
    print(f"❌ Export failed: {e}")
    print("💡 Common issues:")
    print("   1. DATABASE_URL not set in .env file")
    print("   2. PostgreSQL not running")
    print("   3. No permits in database yet")
