"""
Database migration script for permit notification system.
"""

import os
import sys
import subprocess
import logging

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)

def run_migrations():
    """
    Run Alembic migrations to upgrade database to latest version.
    """
    # Check if DATABASE_URL is set
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        logger.error("DATABASE_URL environment variable is required")
        print("Error: DATABASE_URL environment variable is required")
        print("Example: export DATABASE_URL='postgresql://user:password@localhost:5432/dbname'")
        sys.exit(1)
    
    logger.info(f"Running migrations with DATABASE_URL: {database_url[:20]}...")
    print(f"Running database migrations...")
    
    try:
        # Run alembic upgrade head
        result = subprocess.run(
            ['alembic', 'upgrade', 'head'],
            capture_output=True,
            text=True,
            check=True
        )
        
        logger.info("Migrations completed successfully")
        print("✅ Database migrations completed successfully")
        
        if result.stdout:
            print("Output:", result.stdout)
            
    except subprocess.CalledProcessError as e:
        logger.error(f"Migration failed: {e}")
        print(f"❌ Migration failed: {e}")
        if e.stdout:
            print("Output:", e.stdout)
        if e.stderr:
            print("Error:", e.stderr)
        sys.exit(1)
    except FileNotFoundError:
        logger.error("Alembic not found. Make sure it's installed: pip install alembic")
        print("❌ Alembic not found. Make sure it's installed:")
        print("   pip install alembic")
        sys.exit(1)

if __name__ == "__main__":
    # Set up basic logging
    logging.basicConfig(level=logging.INFO)
    
    run_migrations()
