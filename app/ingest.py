# app/ingest.py
import json
import hashlib
from typing import Dict, Any, Optional
from sqlalchemy import text
from app.db import engine

def generate_fingerprint(payload: Dict[str, Any]) -> str:
    """Generate a fingerprint for the payload to detect duplicates."""
    # Create a stable string representation of the payload
    # Sort keys to ensure consistent ordering
    payload_str = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.md5(payload_str.encode()).hexdigest()

def insert_raw_record(
    source_url: str, 
    payload: Dict[str, Any], 
    fingerprint: Optional[str] = None,
    status: str = 'new'
) -> bool:
    """
    Insert a raw record into permits_raw table.
    
    Args:
        source_url: URL where the data was scraped from
        payload: Raw data dictionary
        fingerprint: Optional fingerprint for deduplication
        status: Status of the record ('new', 'processed', 'error')
    
    Returns:
        bool: True if inserted, False if duplicate
    """
    if fingerprint is None:
        fingerprint = generate_fingerprint(payload)
    
    sql = text("""
        INSERT INTO permits.permits_raw (source_url, payload_json, fingerprint, status)
        VALUES (:source_url, CAST(:payload_json AS JSONB), :fingerprint, :status)
        ON CONFLICT (fingerprint) DO NOTHING
        RETURNING raw_id
    """)
    
    try:
        with engine.begin() as conn:
            result = conn.execute(sql, {
                "source_url": source_url,
                "payload_json": json.dumps(payload),
                "fingerprint": fingerprint,
                "status": status
            })
            
            # Check if a row was inserted
            row = result.fetchone()
            if row:
                print(f"Inserted raw record with ID: {row[0]}")
                return True
            else:
                print(f"Duplicate record detected (fingerprint: {fingerprint})")
                return False
                
    except Exception as e:
        print(f"Error inserting raw record: {e}")
        # Try to insert with error status
        try:
            error_sql = text("""
                INSERT INTO permits.permits_raw (source_url, payload_json, fingerprint, status, error_msg)
                VALUES (:source_url, CAST(:payload_json AS JSONB), :fingerprint, 'error', :error_msg)
                ON CONFLICT (fingerprint) DO NOTHING
            """)
            with engine.begin() as conn:
                conn.execute(error_sql, {
                    "source_url": source_url,
                    "payload_json": json.dumps(payload),
                    "fingerprint": fingerprint,
                    "error_msg": str(e)
                })
        except Exception as e2:
            print(f"Failed to insert error record: {e2}")
        return False

def get_raw_records(status: str = 'new', limit: int = 100) -> list:
    """Get raw records by status."""
    sql = text("""
        SELECT raw_id, scraped_at, source_url, payload_json, fingerprint, status, error_msg
        FROM permits.permits_raw
        WHERE status = :status
        ORDER BY scraped_at DESC
        LIMIT :limit
    """)
    
    try:
        with engine.connect() as conn:
            result = conn.execute(sql, {"status": status, "limit": limit})
            return [dict(row._mapping) for row in result]
    except Exception as e:
        print(f"Error getting raw records: {e}")
        return []

def update_raw_record_status(raw_id: str, status: str, error_msg: Optional[str] = None):
    """Update the status of a raw record."""
    sql = text("""
        UPDATE permits.permits_raw
        SET status = :status, error_msg = :error_msg
        WHERE raw_id = :raw_id
    """)
    
    try:
        with engine.begin() as conn:
            conn.execute(sql, {
                "raw_id": raw_id,
                "status": status,
                "error_msg": error_msg
            })
    except Exception as e:
        print(f"Error updating raw record status: {e}")

def get_raw_record_count() -> Dict[str, int]:
    """Get count of raw records by status."""
    sql = text("""
        SELECT status, COUNT(*) as count
        FROM permits.permits_raw
        GROUP BY status
    """)
    
    try:
        with engine.connect() as conn:
            result = conn.execute(sql)
            return {row.status: row.count for row in result}
    except Exception as e:
        print(f"Error getting raw record count: {e}")
        return {}
