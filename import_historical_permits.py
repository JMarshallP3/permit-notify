#!/usr/bin/env python3
"""
Import historical permits from external data sources into the PermitTracker database.
Supports multiple formats: CSV, JSON, Excel, and direct API imports.
"""

import sys
import os
import logging
import pandas as pd
import json
from datetime import datetime, date
from typing import List, Dict, Any, Optional
import argparse

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db.session import get_session
from db.models import Permit
from db.repo import upsert_permits

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HistoricalPermitImporter:
    """Import historical permits from various data sources."""
    
    def __init__(self):
        self.supported_formats = ['csv', 'json', 'xlsx', 'xls']
        self.required_fields = ['status_no', 'lease_name', 'county']
        self.field_mapping = self._get_field_mapping()
    
    def _get_field_mapping(self) -> Dict[str, str]:
        """Map common field names to our database schema."""
        return {
            # Common variations of field names
            'permit_number': 'status_no',
            'permit_no': 'status_no',
            'status_number': 'status_no',
            'rrc_permit_number': 'status_no',
            'permit_id': 'status_no',
            
            'operator': 'operator_name',
            'operator_company': 'operator_name',
            'company_name': 'operator_name',
            
            'well_name': 'lease_name',
            'lease': 'lease_name',
            'well_lease_name': 'lease_name',
            
            'well_number': 'well_no',
            'api_number': 'api_no',
            'api': 'api_no',
            
            'filing_date': 'status_date',
            'permit_date': 'status_date',
            'submission_date': 'status_date',
            'filed_date': 'status_date',
            
            'purpose': 'filing_purpose',
            'permit_type': 'filing_purpose',
            'permit_purpose': 'filing_purpose',
            
            'field': 'field_name',
            'reservoir': 'field_name',
            'formation': 'field_name',
            
            'location': 'survey',
            'legal_description': 'survey',
            
            # Location fields
            'sec': 'section',
            'section_number': 'section',
            'blk': 'block',
            'block_number': 'block',
            'abstract': 'abstract_no',
            'abstract_number': 'abstract_no',
            
            # Other common fields
            'total_depth': 'swr_total_depth',
            'td': 'swr_total_depth',
            'horizontal': 'horizontal_wellbore',
            'wellbore_type': 'horizontal_wellbore',
            'queue': 'current_queue',
            'status': 'current_queue',
        }
    
    def import_from_file(self, file_path: str, format_type: str = None) -> Dict[str, int]:
        """Import permits from a file."""
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Auto-detect format if not provided
        if not format_type:
            format_type = file_path.split('.')[-1].lower()
        
        if format_type not in self.supported_formats:
            raise ValueError(f"Unsupported format: {format_type}. Supported: {self.supported_formats}")
        
        print(f"📁 Importing permits from {file_path} (format: {format_type})")
        
        # Load data based on format
        if format_type == 'csv':
            df = pd.read_csv(file_path)
        elif format_type == 'json':
            with open(file_path, 'r') as f:
                data = json.load(f)
            df = pd.DataFrame(data)
        elif format_type in ['xlsx', 'xls']:
            df = pd.read_excel(file_path)
        
        print(f"📊 Loaded {len(df)} records from file")
        
        # Process and import
        return self._process_dataframe(df)
    
    def import_from_dataframe(self, df: pd.DataFrame) -> Dict[str, int]:
        """Import permits from a pandas DataFrame."""
        print(f"📊 Processing {len(df)} records from DataFrame")
        return self._process_dataframe(df)
    
    def _process_dataframe(self, df: pd.DataFrame) -> Dict[str, int]:
        """Process a DataFrame and import permits."""
        
        # Show column names for mapping
        print(f"📋 Available columns: {list(df.columns)}")
        
        # Normalize column names and apply mapping
        df_normalized = self._normalize_dataframe(df)
        
        # Validate required fields
        missing_required = [field for field in self.required_fields if field not in df_normalized.columns]
        if missing_required:
            raise ValueError(f"Missing required fields: {missing_required}")
        
        # Convert to list of dictionaries
        permits_data = []
        for _, row in df_normalized.iterrows():
            permit_dict = self._row_to_permit_dict(row)
            if permit_dict:
                permits_data.append(permit_dict)
        
        print(f"✅ Processed {len(permits_data)} valid permits")
        
        # Import to database
        if permits_data:
            return upsert_permits(permits_data)
        else:
            return {"inserted": 0, "updated": 0, "errors": 0}
    
    def _normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize column names and apply field mapping."""
        
        # Create a copy to avoid modifying original
        df_norm = df.copy()
        
        # Normalize column names (lowercase, replace spaces/special chars)
        df_norm.columns = [
            col.lower().replace(' ', '_').replace('-', '_').replace('.', '_')
            for col in df_norm.columns
        ]
        
        # Apply field mapping
        column_mapping = {}
        for col in df_norm.columns:
            if col in self.field_mapping:
                column_mapping[col] = self.field_mapping[col]
        
        if column_mapping:
            df_norm = df_norm.rename(columns=column_mapping)
            print(f"🔄 Applied column mapping: {column_mapping}")
        
        return df_norm
    
    def _row_to_permit_dict(self, row: pd.Series) -> Optional[Dict[str, Any]]:
        """Convert a DataFrame row to a permit dictionary."""
        
        try:
            permit_dict = {}
            
            # Process each field
            for col, value in row.items():
                if pd.isna(value) or value == '' or value == 'NULL':
                    continue
                
                # Convert dates
                if col in ['status_date', 'created_at', 'updated_at']:
                    permit_dict[col] = self._parse_date(value)
                
                # Convert boolean fields
                elif col in ['horizontal_wellbore', 'amend']:
                    permit_dict[col] = self._parse_boolean(value)
                
                # Convert numeric fields
                elif col in ['acres', 'swr_total_depth', 'reservoir_well_count']:
                    permit_dict[col] = self._parse_numeric(value)
                
                # String fields - clean up
                else:
                    permit_dict[col] = str(value).strip() if value else None
            
            # Validate required fields
            if not all(permit_dict.get(field) for field in self.required_fields):
                return None
            
            # Set default values
            permit_dict['created_at'] = permit_dict.get('created_at', datetime.now())
            permit_dict['updated_at'] = datetime.now()
            permit_dict['w1_parse_status'] = permit_dict.get('w1_parse_status', 'imported')
            permit_dict['w1_parse_confidence'] = permit_dict.get('w1_parse_confidence', 0.8)
            
            return permit_dict
            
        except Exception as e:
            logger.warning(f"Error processing row: {e}")
            return None
    
    def _parse_date(self, value) -> Optional[date]:
        """Parse various date formats."""
        if pd.isna(value):
            return None
        
        if isinstance(value, (date, datetime)):
            return value.date() if isinstance(value, datetime) else value
        
        # Try common date formats
        date_formats = [
            '%Y-%m-%d',
            '%m/%d/%Y',
            '%m-%d-%Y',
            '%d/%m/%Y',
            '%Y/%m/%d',
            '%m/%d/%y',
            '%Y-%m-%d %H:%M:%S'
        ]
        
        for fmt in date_formats:
            try:
                parsed = datetime.strptime(str(value), fmt)
                return parsed.date()
            except ValueError:
                continue
        
        logger.warning(f"Could not parse date: {value}")
        return None
    
    def _parse_boolean(self, value) -> Optional[bool]:
        """Parse boolean values."""
        if pd.isna(value):
            return None
        
        if isinstance(value, bool):
            return value
        
        str_val = str(value).lower()
        if str_val in ['true', '1', 'yes', 'y', 'horizontal']:
            return True
        elif str_val in ['false', '0', 'no', 'n', 'vertical']:
            return False
        
        return None
    
    def _parse_numeric(self, value) -> Optional[float]:
        """Parse numeric values."""
        if pd.isna(value):
            return None
        
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def create_sample_csv(self, output_path: str = "sample_historical_permits.csv"):
        """Create a sample CSV file showing the expected format."""
        
        sample_data = [
            {
                'status_no': '900001',
                'operator_name': 'SAMPLE OIL COMPANY (123456)',
                'lease_name': 'SAMPLE LEASE A',
                'well_no': '1',
                'county': 'KARNES',
                'status_date': '2024-01-15',
                'filing_purpose': 'New Drill',
                'current_queue': 'Pending Approval',
                'field_name': 'EAGLE FORD',
                'section': '10',
                'block': '5',
                'survey': 'H&TC RR CO',
                'abstract_no': 'A-123',
                'acres': '1250.50',
                'horizontal_wellbore': 'True',
                'api_no': '427-12345',
                'swr_total_depth': '15000'
            },
            {
                'status_no': '900002',
                'operator_name': 'ANOTHER OPERATOR LLC (789012)',
                'lease_name': 'TEST UNIT B',
                'well_no': '2H',
                'county': 'WEBB',
                'status_date': '2024-01-20',
                'filing_purpose': 'Amendment',
                'current_queue': 'Approved',
                'field_name': 'WOLFCAMP',
                'section': '25',
                'block': '12',
                'survey': 'TWNG RR CO',
                'abstract_no': 'A-456',
                'acres': '2100.75',
                'horizontal_wellbore': 'True',
                'api_no': '479-67890',
                'swr_total_depth': '18000'
            }
        ]
        
        df = pd.DataFrame(sample_data)
        df.to_csv(output_path, index=False)
        
        print(f"📝 Created sample CSV file: {output_path}")
        print(f"📋 Required columns: {', '.join(self.required_fields)}")
        print(f"📋 Optional columns: section, block, survey, abstract_no, acres, field_name, etc.")
        
        return output_path

def main():
    """Main function for command-line usage."""
    
    parser = argparse.ArgumentParser(description='Import historical permits into PermitTracker')
    parser.add_argument('file_path', nargs='?', help='Path to the data file to import')
    parser.add_argument('--format', choices=['csv', 'json', 'xlsx', 'xls'], 
                       help='File format (auto-detected if not specified)')
    parser.add_argument('--sample', action='store_true', 
                       help='Create a sample CSV file showing expected format')
    parser.add_argument('--preview', action='store_true',
                       help='Preview the data without importing')
    
    args = parser.parse_args()
    
    importer = HistoricalPermitImporter()
    
    if args.sample:
        sample_file = importer.create_sample_csv()
        print(f"\n✅ Sample file created: {sample_file}")
        print("📖 Edit this file with your historical permit data, then run:")
        print(f"   python import_historical_permits.py {sample_file}")
        return
    
    if not args.file_path:
        print("❌ No file specified. Use --sample to create a template or provide a file path.")
        parser.print_help()
        return
    
    try:
        if args.preview:
            # Just show what would be imported
            print("🔍 Preview mode - no data will be imported")
            if args.file_path.endswith('.csv'):
                df = pd.read_csv(args.file_path)
                print(f"📊 Found {len(df)} records")
                print(f"📋 Columns: {list(df.columns)}")
                print("\n📝 First 5 rows:")
                print(df.head())
        else:
            # Actually import the data
            result = importer.import_from_file(args.file_path, args.format)
            
            print(f"\n🎉 Import completed!")
            print(f"   ✅ Inserted: {result['inserted']} permits")
            print(f"   🔄 Updated: {result['updated']} permits")
            print(f"   ❌ Errors: {result['errors']} permits")
            
            total_processed = result['inserted'] + result['updated']
            if total_processed > 0:
                print(f"\n📈 Successfully processed {total_processed} historical permits!")
                print("🎯 Check your PermitTracker dashboard to see the historical data")
    
    except Exception as e:
        print(f"💥 Import failed: {e}")
        logger.exception("Import error")

if __name__ == "__main__":
    main()
