# scripts/test_db.py
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db import healthcheck, get_connection_info

if __name__ == "__main__":
    print("Testing database connection...")
    
    # Show connection info
    conn_info = get_connection_info()
    print(f"Connection info: {conn_info}")
    
    # Test connection
    ok = healthcheck()
    if ok:
        print("✅ Database connection OK")
        sys.exit(0)
    else:
        print("❌ Database connection FAILED")
        sys.exit(1)
