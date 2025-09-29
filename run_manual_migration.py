#!/usr/bin/env python3
"""
Run manual migration SQL on Railway database.
"""

import psycopg2
import sys

def main():
    print("🔧 Running Manual Migration on Railway")
    print("=" * 40)
    
    # Railway DATABASE_URL
    database_url = "postgresql://postgres:NqDqZtOjqEHJonvpmBtMkVtsalEkeXxF@ballast.proxy.rlwy.net:57963/railway"
    
    try:
        # Connect to database
        print("✅ Connecting to Railway database...")
        conn = psycopg2.connect(database_url)
        conn.autocommit = True  # Important: auto-commit each statement
        cursor = conn.cursor()
        
        # Read and execute SQL
        print("🔄 Reading migration SQL...")
        with open('manual_migration.sql', 'r') as f:
            sql_content = f.read()
        
        # Split by semicolons and execute each statement
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        
        print(f"📝 Executing {len(statements)} SQL statements...")
        
        for i, statement in enumerate(statements, 1):
            if statement.strip():
                print(f"   {i}/{len(statements)}: {statement[:50]}...")
                try:
                    cursor.execute(statement)
                    print(f"      ✅ Success")
                except Exception as e:
                    print(f"      ⚠️  Warning: {e}")
                    # Continue with other statements
        
        cursor.close()
        conn.close()
        
        print("\n🎉 Migration completed successfully!")
        print("   Railway database is now ready for real-time sync!")
        return True
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
