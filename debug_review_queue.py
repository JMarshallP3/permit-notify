#!/usr/bin/env python3
"""
Debug script to check what's in the review queue and permit data
"""

import json
from db.session import get_session
from db.models import Permit

def debug_review_queue():
    print("=== DEBUGGING REVIEW QUEUE ===")
    
    # Check what permits are in the database
    with get_session() as session:
        permits = session.query(Permit).filter(
            Permit.status_no.in_(['910670', '910767'])  # The problematic ones
        ).all()
        
        print(f"\nFound {len(permits)} permits in database:")
        for permit in permits:
            print(f"  Status: {permit.status_no}, ID: {permit.id}, Field: {permit.field_name}, Lease: {permit.lease_name}")
    
    # Check what's in localStorage (simulated)
    print("\n=== WHAT SHOULD BE IN REVIEW QUEUE ===")
    print("The review queue should contain permits with:")
    print("- permit.id (database ID)")
    print("- permit.status_no")
    print("- permit.lease_name")
    print("- permit.field_name")
    print("- permit.detail_url")
    
    print("\n=== POTENTIAL ISSUES ===")
    print("1. Review queue permits might not have 'id' field")
    print("2. Status numbers might not match exactly")
    print("3. Permits might not be in the main permits list")

if __name__ == "__main__":
    debug_review_queue()
