#!/usr/bin/env python3
"""
Quick script to push enhanced data to Railway without hanging commands.
"""

import os
import subprocess
import json

def export_data_simple():
    """Export just the key enhanced fields in a simple format."""
    print("🔄 Exporting enhanced data from Docker...")
    
    # Simple query to get just the enhanced fields
    cmd = [
        'docker', 'exec', 'permit_notify_db', 'psql', 
        '-U', 'permit_app', '-d', 'permit_notify', 
        '-t', '-c',  # -t removes headers and formatting
        """
        SELECT 
            status_no || ',' || 
            COALESCE(section, '') || ',' ||
            COALESCE(block, '') || ',' ||
            COALESCE(survey, '') || ',' ||
            COALESCE(abstract_no, '') || ',' ||
            COALESCE(acres::text, '') || ',' ||
            COALESCE(field_name, '') || ',' ||
            COALESCE(reservoir_well_count::text, '')
        FROM permits.permits 
        WHERE w1_last_enriched_at IS NOT NULL
        ORDER BY status_no;
        """
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            lines = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
            print(f"✅ Exported {len(lines)} enhanced permits")
            return lines
        else:
            print(f"❌ Error: {result.stderr}")
            return None
    except subprocess.TimeoutExpired:
        print("❌ Command timed out")
        return None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def show_data(data_lines):
    """Show the enhanced data that would be pushed."""
    print("\n🎯 ENHANCED DATA TO PUSH TO RAILWAY:")
    print("=" * 60)
    print("Status   | Sec | Blk | Survey | Abstract | Acres   | Field Name         | Wells")
    print("---------|-----|-----|--------|----------|---------|--------------------|---------")
    
    for line in data_lines:
        parts = line.split(',')
        if len(parts) >= 8:
            status = parts[0][:8]
            section = parts[1][:3] or 'N/A'
            block = parts[2][:3] or 'N/A'  
            survey = parts[3][:6] or 'N/A'
            abstract = parts[4][:8] or 'N/A'
            acres = parts[5][:7] or 'N/A'
            field = parts[6][:18] or 'N/A'
            wells = parts[7] or 'N/A'
            
            print(f"{status:<8} | {section:<3} | {block:<3} | {survey:<6} | {abstract:<8} | {acres:<7} | {field:<18} | {wells}")

def create_railway_script(data_lines):
    """Create a simple SQL script for Railway."""
    sql_lines = []
    sql_lines.append("-- Update permits with enhanced data")
    
    for line in data_lines:
        parts = line.split(',')
        if len(parts) >= 8:
            status_no = parts[0].strip()
            section = parts[1].strip() if parts[1].strip() else 'NULL'
            block = parts[2].strip() if parts[2].strip() else 'NULL'
            survey = parts[3].strip() if parts[3].strip() else 'NULL'
            abstract_no = parts[4].strip() if parts[4].strip() else 'NULL'
            acres = parts[5].strip() if parts[5].strip() else 'NULL'
            field_name = parts[6].strip() if parts[6].strip() else 'NULL'
            reservoir_count = parts[7].strip() if parts[7].strip() else 'NULL'
            
            # Create UPDATE statement
            sql_lines.append(f"""
UPDATE permits.permits SET 
    section = {f"'{section}'" if section != 'NULL' else 'NULL'},
    block = {f"'{block}'" if block != 'NULL' else 'NULL'},
    survey = {f"'{survey}'" if survey != 'NULL' else 'NULL'},
    abstract_no = {f"'{abstract_no}'" if abstract_no != 'NULL' else 'NULL'},
    acres = {acres if acres != 'NULL' else 'NULL'},
    field_name = {f"'{field_name}'" if field_name != 'NULL' else 'NULL'},
    reservoir_well_count = {reservoir_count if reservoir_count != 'NULL' else 'NULL'},
    updated_at = NOW()
WHERE status_no = '{status_no}';""")
    
    sql_lines.append("\n-- Show results")
    sql_lines.append("SELECT COUNT(*) as updated_permits FROM permits.permits WHERE section IS NOT NULL;")
    sql_lines.append("SELECT status_no, section, block, survey, abstract_no, acres, field_name, reservoir_well_count FROM permits.permits WHERE status_no LIKE '9106%' ORDER BY status_no LIMIT 5;")
    
    with open('railway_update.sql', 'w') as f:
        f.write('\n'.join(sql_lines))
    
    print(f"\n📝 Created railway_update.sql with {len(data_lines)} updates")
    return 'railway_update.sql'

def main():
    print("🚀 QUICK RAILWAY PUSH")
    print("=" * 30)
    
    # Step 1: Export data
    data_lines = export_data_simple()
    if not data_lines:
        print("❌ Failed to export data")
        return
    
    # Step 2: Show what we're pushing
    show_data(data_lines)
    
    # Step 3: Create Railway SQL script
    sql_file = create_railway_script(data_lines)
    
    print(f"\n✅ Ready to push to Railway!")
    print(f"📋 Next steps:")
    print(f"   1. Run: railway shell")
    print(f"   2. In Railway shell, run: \\i {sql_file}")
    print(f"   3. Or copy/paste the SQL commands from {sql_file}")
    
    print(f"\n🎯 This will update {len(data_lines)} permits with enhanced data!")

if __name__ == "__main__":
    main()
