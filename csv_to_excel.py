#!/usr/bin/env python3
"""
Convert the existing enriched_permits.csv to Excel for analysis.
"""

import pandas as pd
from datetime import datetime

try:
    print("🔄 Reading enriched_permits.csv...")
    
    # Read the CSV file - try UTF-16 first (common for Windows exports)
    encodings = ['utf-16', 'utf-16-le', 'utf-8', 'latin-1', 'cp1252']
    df = None
    
    for encoding in encodings:
        try:
            df = pd.read_csv('enriched_permits.csv', 
                           encoding=encoding,
                           on_bad_lines='skip',
                           engine='python')
            print(f"✅ Successfully read CSV with {encoding} encoding")
            break
        except (UnicodeDecodeError, pd.errors.ParserError) as e:
            print(f"❌ Failed with {encoding}: {str(e)[:50]}...")
            continue
    
    if df is None:
        raise Exception("Could not read CSV with any encoding")
    
    print(f"📋 Found {len(df)} permits in CSV file")
    print(f"📊 Columns: {list(df.columns)}")
    
    # Create Excel file with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    excel_file = f"permits_analysis_{timestamp}.xlsx"
    
    # Create Excel with multiple sheets for analysis
    with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
        # Main data sheet
        df.to_excel(writer, sheet_name='All Permits', index=False)
        
        # Summary sheet
        summary_data = {
            'Metric': [
                'Total Permits',
                'Unique Operators',
                'Unique Counties',
                'Date Range (Earliest)',
                'Date Range (Latest)',
                'Records with Field Names',
                'Records with Acres',
                'Records with Section Info'
            ],
            'Value': [
                len(df),
                df['operator_name'].nunique() if 'operator_name' in df.columns else 'N/A',
                df['county'].nunique() if 'county' in df.columns else 'N/A',
                df['status_date'].min() if 'status_date' in df.columns else 'N/A',
                df['status_date'].max() if 'status_date' in df.columns else 'N/A',
                df['field_name'].notna().sum() if 'field_name' in df.columns else 'N/A',
                df['acres'].notna().sum() if 'acres' in df.columns else 'N/A',
                df['section'].notna().sum() if 'section' in df.columns else 'N/A'
            ]
        }
        
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        # County breakdown (if county column exists)
        if 'county' in df.columns:
            county_summary = df.groupby('county').size().reset_index(name='Count').sort_values('Count', ascending=False)
            county_summary.to_excel(writer, sheet_name='By County', index=False)
        
        # Operator breakdown (if operator column exists)
        if 'operator_name' in df.columns:
            operator_summary = df.groupby('operator_name').size().reset_index(name='Count').sort_values('Count', ascending=False).head(20)
            operator_summary.to_excel(writer, sheet_name='Top Operators', index=False)
        
        # Data quality issues
        issues = []
        
        # Check for missing critical fields
        for col in ['operator_name', 'county', 'lease_name', 'field_name']:
            if col in df.columns:
                missing_count = df[col].isna().sum() + (df[col] == '').sum()
                if missing_count > 0:
                    issues.append(f"Missing {col}: {missing_count} records")
        
        # Check for parsing issues
        if 'w1_parse_status' in df.columns:
            error_statuses = ['parse_error', 'download_error', 'no_pdf']
            for status in error_statuses:
                error_count = (df['w1_parse_status'] == status).sum()
                if error_count > 0:
                    issues.append(f"{status}: {error_count} records")
        
        if issues:
            issues_df = pd.DataFrame({'Data Quality Issues': issues})
            issues_df.to_excel(writer, sheet_name='Issues Found', index=False)
    
    print(f"✅ Successfully converted to Excel: {excel_file}")
    print(f"📁 Open {excel_file} in Excel to review permit data")
    print(f"📊 File contains multiple sheets for detailed analysis")
    
    # Show a preview of the data
    print(f"\n📋 Data Preview:")
    print(f"Columns: {', '.join(df.columns[:10])}{'...' if len(df.columns) > 10 else ''}")
    if len(df) > 0:
        print(f"Sample record:")
        for col in df.columns[:5]:
            print(f"  {col}: {df[col].iloc[0]}")
    
except FileNotFoundError:
    print("❌ enriched_permits.csv not found")
    print("💡 Try running one of the other export scripts first")
    
except Exception as e:
    print(f"❌ Conversion failed: {e}")
    print("💡 Make sure pandas and openpyxl are installed:")
    print("   pip install pandas openpyxl")
