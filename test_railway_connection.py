#!/usr/bin/env python3
"""
Test Railway database connection and count permits.
"""

import os
import sys

# Set the DATABASE_URL directly for testing
os.environ['DATABASE_URL'] = 'postgresql://postgres:NqDqZtOjqEHJonvpmBtMkVtsalEkeXxF@ballast.proxy.rlwy.net:57963/railway'

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    print("🔄 Testing Railway database connection...")
    
    from db.session import get_session
    from db.models import Permit
    
    with get_session() as session:
        # Total count
        total_count = session.query(Permit).count()
        print(f"✅ Connected successfully!")
        print(f"📊 Total permits in Railway database: {total_count}")
        
        if total_count > 0:
            # Check the specific permits with placeholder field names
            problem_permits = session.query(Permit).filter(
                Permit.status_no.in_(['906213', '910669', '910670', '910671', '910672', '910673', '910677'])
            ).all()
            
            print(f"\n🔍 Checking problem permits:")
            for permit in problem_permits:
                field_name = permit.field_name or 'NULL'
                print(f"   {permit.status_no}: {field_name}")
            
            # Show some recent permits
            recent = session.query(Permit).order_by(Permit.created_at.desc()).limit(3).all()
            print(f"\n📋 Latest 3 permits:")
            for permit in recent:
                print(f"   {permit.status_no}: {permit.operator_name} - {permit.field_name or 'No field name'}")
                
        else:
            print("⚠️  Database is empty")
            
        print(f"\n🎯 Ready to run re-enrichment!")
        print(f"   Next step: Restart FastAPI server with latest code")
        
except Exception as e:
    print(f"❌ Connection failed: {e}")
    print("💡 Check if the DATABASE_URL is correct")
