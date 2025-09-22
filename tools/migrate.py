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
        return {"status": "error", "message": "DATABASE_URL environment variable is required"}
    
    logger.info(f"Running migrations with DATABASE_URL: {database_url[:20]}...")
    
    try:
        # Run alembic upgrade head
        result = subprocess.run(
            ['alembic', 'upgrade', 'head'],
            capture_output=True,
            text=True,
            check=True
        )
        
        logger.info("Migrations completed successfully")
        return {"status": "success", "output": result.stdout}
            
    except subprocess.CalledProcessError as e:
        logger.error(f"Migration failed: {e}")
        return {"status": "error", "output": e.stderr}
    except FileNotFoundError:
        logger.error("Alembic not found. Make sure it's installed: pip install alembic")
        return {"status": "error", "message": "Alembic not found"}

if __name__ == "__main__":
    # Set up basic logging
    logging.basicConfig(level=logging.INFO)
    
    run_migrations()
