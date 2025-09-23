# scripts/run_migrations.py
import os
import sys
from sqlalchemy import text

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db import engine, healthcheck

def run_sql_file(file_path: str):
    """Run a SQL file."""
    if not os.path.exists(file_path):
        print(f"SQL file not found: {file_path}")
        return False
    
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            sql_content = f.read()
        
        print(f"Running SQL file: {file_path}")
        with engine.begin() as conn:
            # Execute the entire SQL file as one statement
            print(f"  Executing SQL file...")
            conn.execute(text(sql_content))
        
        print(f"Successfully executed {file_path}")
        return True
        
    except Exception as e:
        print(f"Error running SQL file {file_path}: {e}")
        return False

def run_migrations():
    """Run all migration files in order."""
    migration_dir = "db/migrations"
    
    if not os.path.exists(migration_dir):
        print(f"Migration directory not found: {migration_dir}")
        return False
    
    # Get all SQL files and sort them
    migration_files = [f for f in os.listdir(migration_dir) if f.endswith('.sql')]
    migration_files.sort()
    
    if not migration_files:
        print("No migration files found")
        return True
    
    print(f"Found {len(migration_files)} migration files")
    
    success = True
    for migration_file in migration_files:
        file_path = os.path.join(migration_dir, migration_file)
        if not run_sql_file(file_path):
            success = False
            break
    
    return success

if __name__ == "__main__":
    print("Starting database migrations...")
    
    # Test database connection first
    if not healthcheck():
        print("Database healthcheck failed. Exiting.")
        sys.exit(1)
    
    print("Database connection OK")
    
    # Run migrations
    if run_migrations():
        print("All migrations completed successfully")
        sys.exit(0)
    else:
        print("Migration failed")
        sys.exit(1)
