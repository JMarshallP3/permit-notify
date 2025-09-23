# app/db.py
import os
import json
from urllib.parse import urlparse
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

# Enforce sslmode=require if missing
if "sslmode=" not in DATABASE_URL:
    sep = "&" if "?" in DATABASE_URL else "?"
    DATABASE_URL = f"{DATABASE_URL}{sep}sslmode=require"

engine: Engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=3,
    max_overflow=2,
)

def healthcheck():
    """Test database connection."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            return True
    except Exception as e:
        print(f"Database healthcheck failed: {e}")
        return False

def get_connection_info():
    """Get database connection info for debugging."""
    try:
        parsed = urlparse(DATABASE_URL)
        return {
            "host": parsed.hostname,
            "port": parsed.port,
            "database": parsed.path.lstrip('/'),
            "user": parsed.username,
            "ssl_required": "sslmode=require" in DATABASE_URL
        }
    except Exception as e:
        return {"error": str(e)}
