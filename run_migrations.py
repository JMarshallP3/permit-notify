#!/usr/bin/env python3
"""
Run database migrations manually
"""

import os
import sys
from alembic.config import Config
from alembic import command

def run_migrations():
    """Run all pending migrations"""
    
    # Check if DATABASE_URL is set
    if not os.getenv('DATABASE_URL'):
        print("ERROR: DATABASE_URL environment variable not set")
        print("This script should be run in the production environment")
        return False
    
    try:
        # Create Alembic configuration
        alembic_cfg = Config("alembic.ini")
        
        # Run migrations
        print("Running database migrations...")
        command.upgrade(alembic_cfg, "head")
        print("SUCCESS: Migrations completed successfully!")
        return True
        
    except Exception as e:
        print(f"ERROR: Migration failed: {e}")
        return False

if __name__ == "__main__":
    success = run_migrations()
    sys.exit(0 if success else 1)
