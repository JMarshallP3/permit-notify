#!/usr/bin/env python3
"""
Simple migration runner with better error handling and logging.
"""

import os
import sys
import logging
from alembic.config import Config
from alembic import command

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_migrations():
    """Run database migrations with proper error handling."""
    try:
        # Check if DATABASE_URL is set
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            logger.error("DATABASE_URL environment variable is not set")
            return False
        
        logger.info(f"Running migrations with DATABASE_URL: {database_url[:20]}...")
        
        # Create Alembic config
        alembic_cfg = Config("alembic.ini")
        
        # Override the database URL
        alembic_cfg.set_main_option("sqlalchemy.url", database_url)
        
        # Run the migration
        logger.info("Starting migration upgrade...")
        command.upgrade(alembic_cfg, "head")
        logger.info("✅ Migration completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Migration failed: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return False

if __name__ == "__main__":
    success = run_migrations()
    sys.exit(0 if success else 1)
