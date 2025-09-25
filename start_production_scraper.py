#!/usr/bin/env python3

import os
import sys
import subprocess

def start_production_scraper():
    """Start the production scraper with optimal settings."""
    
    print("🚀 Starting Production Permit Scraper")
    print("=" * 50)
    print()
    print("⚙️  Configuration:")
    print("   📅 Schedule: Monday-Friday, 7:00 AM - 6:00 PM")
    print("   ⏱️  Interval: 5 minutes")
    print("   🎯 Target: Today's permits only")
    print("   💾 Database: Railway PostgreSQL")
    print("   📊 AI Data: Collecting trends for analysis")
    print()
    print("🔄 Starting scraper...")
    print("   Press Ctrl+C to stop")
    print("   Logs saved to: scraper.log")
    print("   Stats saved to: scrape_stats.jsonl")
    print()
    
    # Set environment variables
    env = os.environ.copy()
    env['DATABASE_URL'] = "postgresql://postgres:NqDqZtOjqEHJonvpmBtMkVtsalEkeXxF@ballast.proxy.rlwy.net:57963/railway"
    
    # Start the scraper with production settings
    try:
        subprocess.run([
            sys.executable, "automated_scraper.py",
            "--interval", "5",        # 5-minute intervals
            "--start-hour", "7",      # 7:00 AM
            "--end-hour", "18"        # 6:00 PM
        ], env=env)
    except KeyboardInterrupt:
        print("\n")
        print("🛑 Production scraper stopped")
        print("📊 Check 'python scraper_control.py status' for final stats")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("💡 Try running: python scraper_control.py help")

if __name__ == "__main__":
    start_production_scraper()
