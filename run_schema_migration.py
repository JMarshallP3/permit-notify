#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from alembic.config import Config
from alembic import command
import logging

def run_migration():
    """Run the schema migration on Railway database."""
    
    print("🔧 RUNNING SCHEMA MIGRATION")
    print("=" * 50)
    
    # Check if DATABASE_URL is set
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("❌ DATABASE_URL environment variable not set!")
        print("   Please set it first:")
        print('   $env:DATABASE_URL="postgresql://postgres:YourPassword@ballast.proxy.rlwy.net:57963/railway"')
        return False
    
    print(f"✅ DATABASE_URL configured")
    print(f"   Target: {database_url.split('@')[1] if '@' in database_url else 'unknown'}")
    
    try:
        # Configure Alembic
        alembic_cfg = Config("alembic.ini")
        
        # Set the database URL
        alembic_cfg.set_main_option("sqlalchemy.url", database_url)
        
        # Enable logging
        logging.basicConfig(level=logging.INFO)
        
        print("\n🔄 Running migration to head...")
        
        # Run migration
        command.upgrade(alembic_cfg, "head")
        
        print("✅ Migration completed successfully!")
        
        # Show current revision
        print("\n📊 Current database revision:")
        command.current(alembic_cfg, verbose=True)
        
        return True
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        return False

def verify_changes():
    """Verify the schema changes were applied."""
    
    print("\n🔍 VERIFYING SCHEMA CHANGES")
    print("=" * 50)
    
    try:
        from db.session import get_session
        from db.models import Permit
        import sqlalchemy as sa
        
        with get_session() as session:
            # Check table structure
            inspector = sa.inspect(session.bind)
            columns = inspector.get_columns('permits')
            
            print("📋 Current table structure:")
            for col in columns:
                print(f"   {col['name']:25} {str(col['type']):20} {'NULL' if col['nullable'] else 'NOT NULL'}")
            
            # Check if removed columns are gone
            column_names = [col['name'] for col in columns]
            
            removed_columns = ['submission_date', 'w1_field_name', 'w1_well_count']
            for col in removed_columns:
                if col not in column_names:
                    print(f"✅ Column '{col}' successfully removed")
                else:
                    print(f"❌ Column '{col}' still exists")
            
            # Check column order (created_at should be last)
            if column_names[-1] == 'created_at':
                print("✅ created_at is now the last column")
            else:
                print(f"❌ created_at is not last (last column: {column_names[-1]})")
            
            # Test operator name/number splitting on a sample record
            sample = session.query(Permit).first()
            if sample:
                print(f"\n📊 Sample record:")
                print(f"   Operator Name: {sample.operator_name}")
                print(f"   Operator Number: {sample.operator_number}")
                print(f"   Status Date: {sample.status_date}")
        
        return True
        
    except Exception as e:
        print(f"❌ Verification failed: {e}")
        return False

if __name__ == "__main__":
    success = run_migration()
    if success:
        verify_changes()
    else:
        print("\n❌ Migration failed. Please check the errors above.")
