#!/usr/bin/env python3
"""
Export permit database to Excel for analysis and review.
Usage: python export_permits_to_excel.py
"""

import os
import sys
import pandas as pd
from datetime import datetime
import logging

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db.session import get_session
from db.models import Permit

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def export_permits_to_excel(output_file: str = None, limit: int = None):
    """
    Export all permits from the database to an Excel file.
    
    Args:
        output_file: Path to output Excel file (optional)
        limit: Maximum number of permits to export (optional, None for all)
    
    Returns:
        str: Path to the created Excel file
    """
    
    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"permits_export_{timestamp}.xlsx"
    
    try:
        logger.info("🔄 Connecting to database...")
        
        with get_session() as session:
            # Build query
            query = session.query(Permit).order_by(Permit.status_date.desc(), Permit.id.desc())
            
            if limit:
                query = query.limit(limit)
                logger.info(f"📊 Exporting up to {limit} permits...")
            else:
                logger.info("📊 Exporting all permits...")
            
            # Get all permits
            permits = query.all()
            
            if not permits:
                logger.warning("⚠️ No permits found in database")
                return None
            
            logger.info(f"📋 Found {len(permits)} permits to export")
            
            # Convert to list of dictionaries
            permit_data = []
            for permit in permits:
                permit_dict = {
                    'ID': permit.id,
                    'Status Date': permit.status_date,
                    'Status Number': permit.status_no,
                    'API Number': permit.api_no,
                    'Operator Name': permit.operator_name,
                    'Lease Name': permit.lease_name,
                    'Well Number': permit.well_no,
                    'District': permit.district,
                    'County': permit.county,
                    'Wellbore Profile': permit.wellbore_profile,
                    'Filing Purpose': permit.filing_purpose,
                    'Amendment': permit.amend,
                    'Total Depth': permit.total_depth,
                    'Current Queue': permit.current_queue,
                    'Stacked Lateral Parent': permit.stacked_lateral_parent_well_dp,
                    
                    # Enrichment fields
                    'Field Name': permit.field_name,
                    'Horizontal Wellbore': permit.horizontal_wellbore,
                    'Acres': permit.acres,
                    'Section': permit.section,
                    'Block': permit.block,
                    'Survey': permit.survey,
                    'Abstract Number': permit.abstract_no,
                    'Detail URL': permit.detail_url,
                    
                    # PDF/Parsing fields
                    'Reservoir Well Count': permit.reservoir_well_count,
                    'W1 PDF URL': permit.w1_pdf_url,
                    'W1 Parse Status': permit.w1_parse_status,
                    'W1 Parse Confidence': permit.w1_parse_confidence,
                    'W1 Text Snippet': permit.w1_text_snippet,
                    'W1 Last Enriched': permit.w1_last_enriched_at,
                    
                    # Metadata
                    'Created At': permit.created_at,
                    'Updated At': permit.updated_at
                }
                permit_data.append(permit_dict)
            
            # Create DataFrame
            df = pd.DataFrame(permit_data)
            
            # Create Excel writer with multiple sheets
            logger.info(f"💾 Writing to Excel file: {output_file}")
            
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # Main permits sheet
                df.to_excel(writer, sheet_name='All Permits', index=False)
                
                # Summary sheet
                summary_data = {
                    'Metric': [
                        'Total Permits',
                        'Unique Operators',
                        'Unique Counties',
                        'Permits with Field Names',
                        'Permits with Enrichment',
                        'Permits with PDF Data',
                        'Horizontal Wells',
                        'Vertical Wells',
                        'Date Range (Earliest)',
                        'Date Range (Latest)'
                    ],
                    'Value': [
                        len(df),
                        df['Operator Name'].nunique(),
                        df['County'].nunique(),
                        df['Field Name'].notna().sum(),
                        df['W1 Parse Status'].notna().sum(),
                        df['W1 PDF URL'].notna().sum(),
                        (df['Wellbore Profile'] == 'H').sum(),
                        (df['Wellbore Profile'] == 'V').sum(),
                        df['Status Date'].min(),
                        df['Status Date'].max()
                    ]
                }
                
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
                # County breakdown
                county_summary = df.groupby('County').agg({
                    'ID': 'count',
                    'Field Name': lambda x: x.notna().sum(),
                    'Wellbore Profile': lambda x: (x == 'H').sum()
                }).rename(columns={
                    'ID': 'Total Permits',
                    'Field Name': 'With Field Names',
                    'Wellbore Profile': 'Horizontal Wells'
                }).reset_index()
                
                county_summary.to_excel(writer, sheet_name='By County', index=False)
                
                # Operator breakdown (top 20)
                operator_summary = df.groupby('Operator Name').agg({
                    'ID': 'count',
                    'Field Name': lambda x: x.notna().sum()
                }).rename(columns={
                    'ID': 'Total Permits',
                    'Field Name': 'With Field Names'
                }).sort_values('Total Permits', ascending=False).head(20).reset_index()
                
                operator_summary.to_excel(writer, sheet_name='Top Operators', index=False)
                
                # Data quality issues
                issues = []
                
                # Missing critical data
                missing_operator = df[df['Operator Name'].isna() | (df['Operator Name'] == '')]
                if len(missing_operator) > 0:
                    issues.extend([f"Missing Operator Name: {len(missing_operator)} permits"])
                
                missing_county = df[df['County'].isna() | (df['County'] == '')]
                if len(missing_county) > 0:
                    issues.extend([f"Missing County: {len(missing_county)} permits"])
                
                missing_lease = df[df['Lease Name'].isna() | (df['Lease Name'] == '')]
                if len(missing_lease) > 0:
                    issues.extend([f"Missing Lease Name: {len(missing_lease)} permits"])
                
                # Enrichment issues
                no_field_name = df[df['Field Name'].isna() | (df['Field Name'] == '')]
                if len(no_field_name) > 0:
                    issues.extend([f"Missing Field Name: {len(no_field_name)} permits"])
                
                parse_errors = df[df['W1 Parse Status'].isin(['parse_error', 'download_error', 'no_pdf'])]
                if len(parse_errors) > 0:
                    issues.extend([f"Parse/Download Errors: {len(parse_errors)} permits"])
                
                if issues:
                    issues_df = pd.DataFrame({'Data Quality Issues': issues})
                    issues_df.to_excel(writer, sheet_name='Data Issues', index=False)
            
            logger.info(f"✅ Successfully exported {len(permits)} permits to {output_file}")
            logger.info(f"📊 Excel file contains {len(writer.sheets)} sheets with detailed analysis")
            
            return output_file
            
    except Exception as e:
        logger.error(f"❌ Export failed: {e}")
        raise

def main():
    """Main function to run the export."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Export permit database to Excel')
    parser.add_argument('--output', '-o', help='Output Excel file path')
    parser.add_argument('--limit', '-l', type=int, help='Maximum number of permits to export')
    
    args = parser.parse_args()
    
    try:
        output_file = export_permits_to_excel(args.output, args.limit)
        if output_file:
            print(f"\n🎉 Export completed successfully!")
            print(f"📁 File saved as: {output_file}")
            print(f"📊 Open in Excel to review permit data and identify issues")
        else:
            print("⚠️ No permits found to export")
            
    except Exception as e:
        print(f"❌ Export failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
