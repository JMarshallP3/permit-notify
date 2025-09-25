#!/usr/bin/env python3

import sys
import os
import subprocess
import json
import time
from pathlib import Path

def install_dependencies():
    """Install required dependencies."""
    print("📦 Installing dependencies...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "schedule>=1.2.0"])
        print("✅ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install dependencies: {e}")
        return False

def start_scraper(interval=10, start_hour=7, end_hour=18):
    """Start the automated scraper."""
    print(f"🚀 Starting automated scraper...")
    print(f"   - Interval: {interval} minutes")
    print(f"   - Hours: {start_hour}:00 - {end_hour}:00")
    print(f"   - Press Ctrl+C to stop")
    print()
    
    try:
        # Set DATABASE_URL for the scraper
        env = os.environ.copy()
        env['DATABASE_URL'] = "postgresql://postgres:NqDqZtOjqEHJonvpmBtMkVtsalEkeXxF@ballast.proxy.rlwy.net:57963/railway"
        
        subprocess.run([
            sys.executable, "automated_scraper.py",
            "--interval", str(interval),
            "--start-hour", str(start_hour),
            "--end-hour", str(end_hour)
        ], env=env)
        
    except KeyboardInterrupt:
        print("\n🛑 Scraper stopped by user")
    except Exception as e:
        print(f"❌ Error starting scraper: {e}")

def show_status():
    """Show scraper status."""
    print("📊 Scraper Status:")
    print("=" * 50)
    
    # Check if scraper log exists
    log_file = Path("scraper.log")
    if log_file.exists():
        print(f"📝 Log file: {log_file.absolute()}")
        
        # Show last few log entries
        try:
            with open(log_file, 'r') as f:
                lines = f.readlines()
                if lines:
                    print("\n📋 Recent activity (last 5 entries):")
                    for line in lines[-5:]:
                        print(f"   {line.strip()}")
                else:
                    print("   No log entries yet")
        except Exception as e:
            print(f"   Error reading log: {e}")
    else:
        print("📝 No log file found (scraper hasn't run yet)")
    
    # Check if stats file exists
    stats_file = Path("scrape_stats.jsonl")
    if stats_file.exists():
        print(f"\n📈 Stats file: {stats_file.absolute()}")
        
        try:
            with open(stats_file, 'r') as f:
                lines = f.readlines()
                if lines:
                    # Parse last entry
                    last_stats = json.loads(lines[-1])
                    print(f"   Last scrape: {last_stats['timestamp']}")
                    print(f"   Permits found: {last_stats['permits_found']}")
                    print(f"   New permits: {last_stats['permits_inserted']}")
                    print(f"   Updated permits: {last_stats['permits_updated']}")
                    
                    # Count total scrapes
                    total_scrapes = len(lines)
                    successful_scrapes = sum(1 for line in lines if json.loads(line).get('success', False))
                    
                    print(f"\n📊 Overall stats:")
                    print(f"   Total scrapes: {total_scrapes}")
                    print(f"   Successful: {successful_scrapes}")
                    print(f"   Success rate: {successful_scrapes/total_scrapes*100:.1f}%")
                else:
                    print("   No stats recorded yet")
        except Exception as e:
            print(f"   Error reading stats: {e}")
    else:
        print("📈 No stats file found")

def show_help():
    """Show help information."""
    print("🤖 Automated Permit Scraper Control")
    print("=" * 50)
    print()
    print("Commands:")
    print("  start [interval] [start_hour] [end_hour]  - Start the scraper")
    print("  status                                    - Show scraper status")
    print("  install                                   - Install dependencies")
    print("  help                                      - Show this help")
    print()
    print("Examples:")
    print("  python scraper_control.py start          - Start with defaults (10 min, 7AM-6PM)")
    print("  python scraper_control.py start 5        - Start with 5-minute intervals")
    print("  python scraper_control.py start 5 8 17   - 5 min intervals, 8AM-5PM")
    print("  python scraper_control.py status         - Check current status")
    print()
    print("Default settings:")
    print("  - Interval: 10 minutes")
    print("  - Hours: 7:00 AM - 6:00 PM")
    print("  - Days: Monday - Friday")
    print()
    print("The scraper will:")
    print("  ✅ Only run during business hours (weekdays)")
    print("  ✅ Save all activity to scraper.log")
    print("  ✅ Store statistics for AI analysis")
    print("  ✅ Handle errors gracefully")
    print("  ✅ Update your Railway database")

def main():
    """Main control function."""
    if len(sys.argv) < 2:
        show_help()
        return
    
    command = sys.argv[1].lower()
    
    if command == "start":
        # Parse optional arguments
        interval = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        start_hour = int(sys.argv[3]) if len(sys.argv) > 3 else 7
        end_hour = int(sys.argv[4]) if len(sys.argv) > 4 else 18
        
        # Validate arguments
        if not (5 <= interval <= 60):
            print("❌ Interval must be between 5 and 60 minutes")
            return
        
        if not (0 <= start_hour < 24 and 0 <= end_hour < 24):
            print("❌ Hours must be between 0 and 23")
            return
        
        if start_hour >= end_hour:
            print("❌ Start hour must be before end hour")
            return
        
        start_scraper(interval, start_hour, end_hour)
    
    elif command == "status":
        show_status()
    
    elif command == "install":
        install_dependencies()
    
    elif command == "help":
        show_help()
    
    else:
        print(f"❌ Unknown command: {command}")
        print("Use 'python scraper_control.py help' for available commands")

if __name__ == "__main__":
    main()
