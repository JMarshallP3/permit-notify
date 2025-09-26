#!/usr/bin/env python3
"""
Set up Railway database connection for local development.
"""

import os

def setup_railway_env():
    """Guide user through setting up Railway database connection."""
    
    print("🚀 Setting up Railway Database Connection")
    print("=" * 50)
    
    print("\n📋 You provided an internal Railway URL:")
    print("postgresql://postgres:NqDqZtOjqEHJonvpmBtMkVtsalEkeXxF@postgres.railway.internal:5432/railway")
    
    print("\n⚠️  This internal URL only works within Railway's network.")
    print("For local development, you need the PUBLIC/EXTERNAL URL.")
    
    print("\n🔧 To get the external URL:")
    print("1. Go to https://railway.app/dashboard")
    print("2. Open your 'permit-notify' project")
    print("3. Click on the 'PostgreSQL' service")
    print("4. Go to the 'Connect' tab")
    print("5. Look for 'Public Network' section")
    print("6. Copy the connection string that looks like:")
    print("   postgresql://postgres:NqDqZtOjqEHJonvpmBtMkVtsalEkeXxF@HOST.railway.app:PORT/railway")
    
    print("\n💡 The external URL will have:")
    print("   - Same password: NqDqZtOjqEHJonvpmBtMkVtsalEkeXxF")
    print("   - External host: something like 'roundhouse.proxy.rlwy.net' or similar")
    print("   - Public port: usually 5432 or different")
    
    external_url = input("\n📝 Paste the external DATABASE_URL here: ").strip()
    
    if external_url:
        # Create .env file
        env_content = f"""# Railway Database Connection
DATABASE_URL={external_url}
RAILWAY_ENVIRONMENT=development

# Scraper configuration  
SCRAPE_BEGIN_DATE=09/26/2025
SCRAPE_END_DATE=09/26/2025
SCRAPE_MAX_PAGES=10
"""
        
        try:
            with open('.env', 'w') as f:
                f.write(env_content)
            
            print("\n✅ Created .env file successfully!")
            print("📁 File location: .env")
            print("\n🧪 Testing connection...")
            
            # Test the connection
            os.environ['DATABASE_URL'] = external_url
            
            try:
                import sys
                sys.path.append(os.path.dirname(os.path.abspath(__file__)))
                from db.session import get_session
                from db.models import Permit
                
                with get_session() as session:
                    count = session.query(Permit).count()
                    print(f"🎉 SUCCESS! Found {count} permits in Railway database")
                    
                    if count > 0:
                        latest = session.query(Permit).order_by(Permit.created_at.desc()).first()
                        print(f"📋 Latest permit: {latest.status_no} - {latest.operator_name}")
                        
                        print(f"\n🚀 Ready to export! Run:")
                        print(f"   python export_permits_to_excel.py")
                        print(f"   python quick_export.py")
                    else:
                        print("⚠️  Database is empty - may need to run scraper first")
                        
            except Exception as e:
                print(f"❌ Connection test failed: {e}")
                print("💡 Double-check the DATABASE_URL is correct")
                
        except Exception as e:
            print(f"❌ Failed to create .env file: {e}")
    else:
        print("\n❌ No URL provided. Please get the external URL from Railway dashboard.")

if __name__ == "__main__":
    setup_railway_env()
