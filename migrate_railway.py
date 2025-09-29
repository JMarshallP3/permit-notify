#!/usr/bin/env python3
"""
Quick script to run the database migration on Railway.
Run this locally with the Railway DATABASE_URL to apply the new schema.
"""

import os
import sys
import subprocess

def main():
    print("🔧 Railway Database Migration")
    print("=" * 40)
    
    # Set the Railway DATABASE_URL
    database_url = "postgresql://postgres:NqDqZtOjqEHJonvpmBtMkVtsalEkeXxF@ballast.proxy.rlwy.net:57963/railway"
    os.environ['DATABASE_URL'] = database_url
    
    print(f"✅ Connecting to Railway database...")
    print(f"   Target: ballast.proxy.rlwy.net:57963/railway")
    
    try:
        # Run alembic upgrade head
        print("\n🔄 Running migration...")
        result = subprocess.run(
            ['alembic', 'upgrade', 'head'],
            capture_output=True,
            text=True,
            check=True
        )
        
        print("✅ Migration completed successfully!")
        
        if result.stdout:
            print("\nOutput:")
            print(result.stdout)
            
        print("\n🎉 Railway database is now ready for real-time sync!")
        return True
            
    except subprocess.CalledProcessError as e:
        print(f"❌ Migration failed: {e}")
        if e.stdout:
            print("Output:", e.stdout)
        if e.stderr:
            print("Error:", e.stderr)
        return False
    except FileNotFoundError:
        print("❌ Alembic not found. Install it with:")
        print("   pip install alembic")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
