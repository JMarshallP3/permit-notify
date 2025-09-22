"""
Railway migration runner script.
This script should be run in the Railway environment to apply database migrations.
"""

import os
import sys
import subprocess
import logging

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logger = logging.getLogger(__name__)

def run_migration():
    """Run the database migration in Railway environment."""
    
    # Check if DATABASE_URL is set
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        logger.error("DATABASE_URL environment variable is required")
        print("Error: DATABASE_URL environment variable is required")
        return False
    
    logger.info(f"Running migration with DATABASE_URL: {database_url[:20]}...")
    print(f"Running database migration...")
    
    try:
        # Run alembic upgrade head
        result = subprocess.run(
            ['alembic', 'upgrade', 'head'],
            capture_output=True,
            text=True,
            check=True
        )
        
        logger.info("Migration completed successfully")
        print("✅ Database migration completed successfully")
        
        if result.stdout:
            print("Output:", result.stdout)
            
        return True
            
    except subprocess.CalledProcessError as e:
        logger.error(f"Migration failed: {e}")
        print(f"❌ Migration failed: {e}")
        if e.stdout:
            print("Output:", e.stdout)
        if e.stderr:
            print("Error:", e.stderr)
        return False
    except FileNotFoundError:
        logger.error("Alembic not found. Make sure it's installed: pip install alembic")
        print("❌ Alembic not found. Make sure it's installed:")
        print("   pip install alembic")
        return False

if __name__ == "__main__":
    # Set up basic logging
    logging.basicConfig(level=logging.INFO)
    
    success = run_migration()
    sys.exit(0 if success else 1)
