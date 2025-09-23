#!/usr/bin/env python3
"""
Deploy and run the permit scraper in Railway environment.
"""

import os
import sys
import subprocess
import time

def run_command(command, description):
    """Run a command and return the result."""
    print(f"\n🚀 {description}")
    print(f"Running: {command}")
    
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ {description} completed successfully")
            if result.stdout:
                print(f"Output: {result.stdout}")
        else:
            print(f"❌ {description} failed")
            if result.stderr:
                print(f"Error: {result.stderr}")
        return result.returncode == 0
    except Exception as e:
        print(f"❌ {description} failed with exception: {e}")
        return False

def main():
    print("🚀 Railway Deployment Script")
    print("=" * 50)
    
    # Check if Railway CLI is installed
    if not run_command("railway --version", "Checking Railway CLI"):
        print("\n❌ Railway CLI not found. Please install it first:")
        print("   npm install -g @railway/cli")
        return False
    
    # Login to Railway (if not already logged in)
    print("\n🔐 Checking Railway authentication...")
    if not run_command("railway whoami", "Checking Railway login status"):
        print("\n🔐 Please login to Railway:")
        print("   railway login")
        return False
    
    # Link to project (if not already linked)
    print("\n🔗 Linking to Railway project...")
    if not run_command("railway link", "Linking to Railway project"):
        print("\n❌ Failed to link to Railway project")
        return False
    
    # Deploy the application
    print("\n🚀 Deploying to Railway...")
    if not run_command("railway up", "Deploying application"):
        print("\n❌ Deployment failed")
        return False
    
    # Wait for deployment to complete
    print("\n⏳ Waiting for deployment to complete...")
    time.sleep(30)
    
    # Run database migration
    print("\n🗄️ Running database migration...")
    if not run_command("railway run python railway_migrate.py", "Running database migration"):
        print("\n⚠️ Migration failed, but continuing...")
    
    # Test the scraper
    print("\n🧪 Testing the scraper...")
    if not run_command("railway run python save_permits_to_db.py", "Testing scraper"):
        print("\n⚠️ Scraper test failed, but deployment completed")
    
    print("\n🎉 Deployment completed!")
    print("\n📋 Next steps:")
    print("1. Check your Railway dashboard for the app URL")
    print("2. Test the API endpoints:")
    print("   - GET /health - Health check")
    print("   - GET /w1/search?begin=09/23/2025&end=09/23/2025 - Test scraper")
    print("   - GET /api/v1/permits - View stored permits")
    print("3. Monitor logs: railway logs")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
