#!/usr/bin/env python3
"""Create the field_corrections table on Railway database."""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db.session import get_session, engine
from db.field_corrections import FieldCorrection
from sqlalchemy import text

def create_field_corrections_table():
    """Create the field_corrections table directly."""
    
    print("🔧 Creating field_corrections table...")
    
    try:
        # Create the table using SQLAlchemy
        FieldCorrection.metadata.create_all(engine)
        
        print("✅ Successfully created field_corrections table!")
        
        # Test the table
        with get_session() as session:
            count = session.execute(text("SELECT COUNT(*) FROM field_corrections")).scalar()
            print(f"📊 Table is ready. Current corrections: {count}")
            
    except Exception as e:
        print(f"❌ Error creating table: {e}")
        
        # Try manual SQL creation as fallback
        print("🔄 Trying manual SQL creation...")
        
        try:
            with get_session() as session:
                session.execute(text("""
                    CREATE TABLE IF NOT EXISTS field_corrections (
                        id SERIAL PRIMARY KEY,
                        permit_id INTEGER NOT NULL,
                        status_no VARCHAR(50) NOT NULL,
                        lease_name VARCHAR(255),
                        operator_name VARCHAR(255),
                        wrong_field_name VARCHAR(255) NOT NULL,
                        correct_field_name VARCHAR(255) NOT NULL,
                        detail_url TEXT,
                        html_context TEXT,
                        corrected_by VARCHAR(100) DEFAULT 'user',
                        corrected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        applied_to_permit BOOLEAN DEFAULT FALSE,
                        extraction_pattern TEXT,
                        correction_pattern TEXT
                    )
                """))
                
                # Create indexes
                session.execute(text("CREATE INDEX IF NOT EXISTS idx_field_corrections_permit_id ON field_corrections (permit_id)"))
                session.execute(text("CREATE INDEX IF NOT EXISTS idx_field_corrections_status_no ON field_corrections (status_no)"))
                session.execute(text("CREATE INDEX IF NOT EXISTS idx_field_corrections_wrong_field ON field_corrections (wrong_field_name)"))
                session.execute(text("CREATE INDEX IF NOT EXISTS idx_field_corrections_corrected_at ON field_corrections (corrected_at)"))
                
                session.commit()
                
                print("✅ Successfully created field_corrections table with manual SQL!")
                
        except Exception as sql_error:
            print(f"❌ Manual SQL creation also failed: {sql_error}")
            print("⚠️  The learning system will work without the table, but corrections won't be saved permanently.")

if __name__ == "__main__":
    create_field_corrections_table()
