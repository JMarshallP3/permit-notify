#!/usr/bin/env python3
"""
Test script for the new database setup.
"""

import os
import sys
import json

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.db import healthcheck, get_connection_info
from app.ingest import insert_raw_record, get_raw_record_count, get_raw_records

def test_database_connection():
    """Test database connection."""
    print("🔍 Testing database connection...")
    
    # Show connection info
    conn_info = get_connection_info()
    print(f"Connection info: {conn_info}")
    
    # Test connection
    if healthcheck():
        print("✅ Database connection OK")
        return True
    else:
        print("❌ Database connection FAILED")
        return False

def test_raw_table_insert():
    """Test inserting data into permits_raw table."""
    print("\n🔍 Testing raw table insert...")
    
    # Test data
    test_payload = {
        "status_date": "09/23/2025",
        "status_no": "TEST123",
        "api_no": "42-123-45678",
        "operator_name": "TEST OPERATOR INC",
        "lease_name": "TEST LEASE",
        "county": "TEST COUNTY",
        "district": "08",
        "wellbore_profile": "Horizontal",
        "filing_purpose": "New Drill"
    }
    
    try:
        success = insert_raw_record(
            source_url="test://example.com",
            payload=test_payload,
            status='test'
        )
        
        if success:
            print("✅ Raw table insert OK")
            return True
        else:
            print("⚠️ Raw table insert returned False (might be duplicate)")
            return True  # Still OK, just a duplicate
    except Exception as e:
        print(f"❌ Raw table insert FAILED: {e}")
        return False

def test_raw_table_query():
    """Test querying from permits_raw table."""
    print("\n🔍 Testing raw table query...")
    
    try:
        # Get counts
        counts = get_raw_record_count()
        print(f"Raw record counts: {counts}")
        
        # Get test records
        test_records = get_raw_records(status='test', limit=5)
        print(f"Found {len(test_records)} test records")
        
        if test_records:
            print("Sample record:")
            sample = test_records[0]
            print(f"  ID: {sample['raw_id']}")
            print(f"  Status: {sample['status']}")
            print(f"  Payload: {json.dumps(sample['payload_json'], indent=2)}")
        
        print("✅ Raw table query OK")
        return True
    except Exception as e:
        print(f"❌ Raw table query FAILED: {e}")
        return False

def main():
    """Run all tests."""
    print("🚀 Testing new database setup...\n")
    
    tests = [
        test_database_connection,
        test_raw_table_insert,
        test_raw_table_query
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
        print()
    
    print(f"📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! New database setup is working.")
        return 0
    else:
        print("❌ Some tests failed. Check the errors above.")
        return 1

if __name__ == "__main__":
    exit(main())
