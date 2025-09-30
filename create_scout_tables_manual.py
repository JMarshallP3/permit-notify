#!/usr/bin/env python3
"""
Manual Scout v2.2 Database Table Creation
Creates Scout tables directly using SQLAlchemy when Alembic migrations fail
"""

import os
import sys
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Add the app directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db.session import Base, get_engine
from db.scout_models import Signal, ScoutInsight, ScoutInsightUserState

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_scout_enums(engine):
    """Create the required enums for Scout v2.2"""
    
    enum_sql = [
        # SourceType enum
        """
        DO $$ BEGIN
            CREATE TYPE sourcetype AS ENUM (
                'forum', 'news', 'pr', 'filing', 'gov_bulletin', 'blog', 'social', 'other'
            );
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
        """,
        
        # Timeframe enum  
        """
        DO $$ BEGIN
            CREATE TYPE timeframe AS ENUM (
                'past', 'now', 'next_90d'
            );
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
        """,
        
        # ConfidenceLevel enum (might already exist)
        """
        DO $$ BEGIN
            CREATE TYPE confidencelevel AS ENUM (
                'low', 'medium', 'high'
            );
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
        """,
        
        # InsightUserState enum (might already exist)
        """
        DO $$ BEGIN
            CREATE TYPE insightuserstate AS ENUM (
                'default', 'kept', 'dismissed', 'archived'
            );
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
        """
    ]
    
    with engine.connect() as conn:
        for sql in enum_sql:
            try:
                conn.execute(text(sql))
                conn.commit()
                logger.info("✅ Created enum successfully")
            except Exception as e:
                logger.info(f"⚠️ Enum creation (expected if exists): {e}")

def create_scout_tables():
    """Create Scout v2.2 tables manually"""
    
    try:
        # Get database URL
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            logger.error("❌ DATABASE_URL environment variable not set")
            return False
        
        logger.info("🔧 Creating Scout v2.2 database tables...")
        
        # Create engine
        engine = create_engine(database_url)
        
        # Create enums first
        logger.info("📝 Creating required enums...")
        create_scout_enums(engine)
        
        # Create tables
        logger.info("🏗️ Creating Scout tables...")
        Base.metadata.create_all(engine, tables=[
            Signal.__table__,
            ScoutInsight.__table__, 
            ScoutInsightUserState.__table__
        ])
        
        # Verify tables exist
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name IN ('signals', 'scout_insights', 'scout_insight_user_state')
            """))
            tables = [row[0] for row in result]
            
            logger.info(f"✅ Created tables: {tables}")
            
            if len(tables) == 3:
                logger.info("🎉 SUCCESS: All Scout v2.2 tables created successfully!")
                return True
            else:
                logger.error(f"❌ FAILED: Only {len(tables)}/3 tables created")
                return False
                
    except Exception as e:
        logger.error(f"❌ ERROR creating Scout tables: {e}")
        return False

def add_org_id_to_field_corrections():
    """Add org_id column to field_corrections table if it doesn't exist"""
    
    try:
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            return False
            
        engine = create_engine(database_url)
        
        with engine.connect() as conn:
            # Check if org_id column exists
            result = conn.execute(text("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'field_corrections' AND column_name = 'org_id'
            """))
            
            if not result.fetchone():
                logger.info("📝 Adding org_id column to field_corrections...")
                conn.execute(text("""
                    ALTER TABLE field_corrections 
                    ADD COLUMN org_id VARCHAR(50) NOT NULL DEFAULT 'default_org'
                """))
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_field_corrections_org_id 
                    ON field_corrections(org_id)
                """))
                conn.commit()
                logger.info("✅ Added org_id column to field_corrections")
            else:
                logger.info("✅ org_id column already exists in field_corrections")
                
        return True
        
    except Exception as e:
        logger.error(f"❌ ERROR adding org_id to field_corrections: {e}")
        return False

if __name__ == "__main__":
    logger.info("🚀 Starting Scout v2.2 manual database setup...")
    
    # Step 1: Add org_id to field_corrections
    logger.info("\n📋 Step 1: Updating field_corrections table...")
    add_org_id_to_field_corrections()
    
    # Step 2: Create Scout tables
    logger.info("\n📋 Step 2: Creating Scout v2.2 tables...")
    success = create_scout_tables()
    
    if success:
        logger.info("\n🎉 SUCCESS: Scout v2.2 database setup complete!")
        logger.info("✅ You can now use real Scout insights instead of demo mode")
    else:
        logger.error("\n❌ FAILED: Scout v2.2 database setup failed")
        logger.error("⚠️ Scout will continue to use demo mode")
    
    sys.exit(0 if success else 1)
