#!/usr/bin/env python3
"""Count permits in database and show recent activity."""

import os
import sys
from datetime import datetime, timedelta

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    # Set a temporary DATABASE_URL for testing
    if not os.getenv('DATABASE_URL'):
        # Try to use a local database URL for testing
        os.environ['DATABASE_URL'] = 'postgresql+psycopg://postgres:password@localhost:5432/permitdb'
    
    from db.session import get_session
    from db.models import Permit
    
    print("🔄 Connecting to database...")
    
    with get_session() as session:
        # Total count
        total_count = session.query(Permit).count()
        print(f"📊 Total permits in database: {total_count}")
        
        if total_count == 0:
            print("❌ No permits found in database!")
            print("💡 This could mean:")
            print("   1. Database is empty (need to run scraper)")
            print("   2. DATABASE_URL not configured")
            print("   3. Database connection issues")
            sys.exit(1)
        
        # Recent activity
        recent_dates = []
        for days_ago in range(7):
            date = datetime.now().date() - timedelta(days=days_ago)
            count = session.query(Permit).filter(Permit.status_date == date).count()
            if count > 0:
                recent_dates.append((date, count))
        
        print(f"\n📅 Recent permit activity:")
        for date, count in recent_dates:
            print(f"   {date}: {count} permits")
        
        # Show latest permits
        latest = session.query(Permit).order_by(Permit.created_at.desc()).limit(5).all()
        print(f"\n📋 Latest 5 permits:")
        for permit in latest:
            print(f"   {permit.status_no}: {permit.operator_name} - {permit.lease_name}")
            print(f"      Status Date: {permit.status_date}, Created: {permit.created_at}")
        
        # Check enrichment status
        enriched_count = session.query(Permit).filter(Permit.field_name.isnot(None)).count()
        print(f"\n🔍 Enrichment status:")
        print(f"   {enriched_count} permits have field names ({enriched_count/total_count*100:.1f}%)")
        
        parse_status_counts = session.query(Permit.w1_parse_status, session.query(Permit).filter(Permit.w1_parse_status == Permit.w1_parse_status).count()).group_by(Permit.w1_parse_status).all()
        print(f"   Parse status breakdown:")
        for status, count in parse_status_counts:
            print(f"      {status or 'None'}: {count}")

except ImportError as e:
    print(f"❌ Missing dependency: {e}")
    print("💡 Install required packages: pip install psycopg sqlalchemy")
    
except Exception as e:
    print(f"❌ Database connection failed: {e}")
    print("💡 Common issues:")
    print("   1. PostgreSQL not running")
    print("   2. DATABASE_URL not set correctly")
    print("   3. Database doesn't exist")
    print("   4. Wrong credentials")
