#!/usr/bin/env python3
"""Check current database schema."""

from app.db import engine
from sqlalchemy import text

def check_schema():
    try:
        with engine.connect() as conn:
            # Check tables
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """))
            tables = [row[0] for row in result]
            print("Tables:", tables)
            
            # Check permits table columns
            if 'permits' in tables:
                result = conn.execute(text("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns 
                    WHERE table_name = 'permits' 
                    ORDER BY ordinal_position
                """))
                print("\nPermits table columns:")
                for row in result:
                    print(f"  {row[0]}: {row[1]} (nullable: {row[2]})")
            
            # Check alembic version
            try:
                result = conn.execute(text("SELECT version_num FROM alembic_version"))
                version = result.fetchone()
                print(f"\nAlembic version: {version[0] if version else 'No version found'}")
            except Exception as e:
                print(f"\nAlembic version: Error - {e}")
                
    except Exception as e:
        print(f"Error checking schema: {e}")

if __name__ == "__main__":
    check_schema()
