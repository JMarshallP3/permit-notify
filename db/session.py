"""
Database session configuration and management.
"""

import os
import logging
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

# Get DATABASE_URL from environment
DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError(
        "DATABASE_URL environment variable is required. "
        "Example: postgresql://user:password@localhost:5432/permitdb"
    )

# Convert postgresql:// to postgresql+psycopg:// for psycopg v3 compatibility
if DATABASE_URL.startswith('postgresql://'):
    DATABASE_URL = DATABASE_URL.replace('postgresql://', 'postgresql+psycopg://', 1)

# Create engine with connection pooling
# Use psycopg dialect for psycopg v3 compatibility
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,  # Verify connections before use
    pool_recycle=300,    # Recycle connections every 5 minutes
    echo=False           # Set to True for SQL query logging
)

# Create session factory
SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False
)

# Create declarative base
Base = declarative_base()

@contextmanager
def get_session():
    """
    Context manager for database sessions.
    Automatically handles commit/rollback and session cleanup.
    """
    session = SessionLocal()
    try:
        logger.debug("Database session started")
        yield session
        session.commit()
        logger.debug("Database session committed")
    except Exception as e:
        logger.error(f"Database session error: {e}")
        session.rollback()
        raise
    finally:
        session.close()
        logger.debug("Database session closed")
