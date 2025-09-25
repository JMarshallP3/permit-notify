#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import asyncio
import schedule
import time
import logging
import requests
from datetime import datetime, timedelta
from typing import Optional
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PermitScraperScheduler:
    """
    Automated permit scraper that runs during business hours (7 AM - 6 PM, weekdays)
    with configurable intervals (5-10 minutes).
    """
    
    def __init__(self, 
                 api_url: str = "http://localhost:8000/w1/search",
                 interval_minutes: int = 10,
                 start_hour: int = 7,
                 end_hour: int = 18):
        """
        Initialize the scraper scheduler.
        
        Args:
            api_url: URL of your FastAPI scraping endpoint
            interval_minutes: How often to scrape (5-10 minutes)
            start_hour: Start time (24-hour format, default 7 AM)
            end_hour: End time (24-hour format, default 6 PM)
        """
        self.api_url = api_url
        self.interval_minutes = interval_minutes
        self.start_hour = start_hour
        self.end_hour = end_hour
        self.is_running = False
        self.last_scrape_time = None
        self.scrape_count = 0
        self.error_count = 0
        
        logger.info(f"Initialized scraper scheduler:")
        logger.info(f"  - Interval: {interval_minutes} minutes")
        logger.info(f"  - Hours: {start_hour}:00 - {end_hour}:00")
        logger.info(f"  - API URL: {api_url}")
    
    def is_business_hours(self) -> bool:
        """Check if current time is within business hours (weekdays, 7 AM - 6 PM)."""
        now = datetime.now()
        
        # Check if it's a weekday (Monday = 0, Sunday = 6)
        if now.weekday() >= 5:  # Saturday or Sunday
            return False
        
        # Check if it's within business hours
        current_hour = now.hour
        if current_hour < self.start_hour or current_hour >= self.end_hour:
            return False
        
        return True
    
    def get_today_date_string(self) -> str:
        """Get today's date in MM/DD/YYYY format for the API."""
        return datetime.now().strftime("%m/%d/%Y")
    
    def scrape_permits(self) -> bool:
        """
        Perform the actual scraping by calling the API.
        
        Returns:
            bool: True if successful, False if failed
        """
        try:
            today = self.get_today_date_string()
            
            logger.info(f"Starting scrape #{self.scrape_count + 1} for {today}")
            
            # Call your existing API endpoint
            params = {
                "begin": today,
                "end": today,
                "max_pages": 5  # Limit to prevent long runs
            }
            
            response = requests.get(
                self.api_url,
                params=params,
                timeout=300  # 5 minute timeout
            )
            
            if response.status_code == 200:
                data = response.json()
                permit_count = len(data.get("items", []))
                database_info = data.get("database", {})
                
                logger.info(f"Scrape successful:")
                logger.info(f"  - Found: {permit_count} permits")
                logger.info(f"  - Inserted: {database_info.get('inserted', 0)} new")
                logger.info(f"  - Updated: {database_info.get('updated', 0)} existing")
                
                self.last_scrape_time = datetime.now()
                self.scrape_count += 1
                
                # Store scrape statistics for AI analysis later
                self.save_scrape_stats({
                    "timestamp": self.last_scrape_time.isoformat(),
                    "permits_found": permit_count,
                    "permits_inserted": database_info.get('inserted', 0),
                    "permits_updated": database_info.get('updated', 0),
                    "success": True
                })
                
                return True
            else:
                logger.error(f"API returned status {response.status_code}: {response.text}")
                self.error_count += 1
                return False
                
        except requests.exceptions.Timeout:
            logger.error("Scrape timed out (5 minutes)")
            self.error_count += 1
            return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error during scrape: {e}")
            self.error_count += 1
            return False
        except Exception as e:
            logger.error(f"Unexpected error during scrape: {e}")
            self.error_count += 1
            return False
    
    def save_scrape_stats(self, stats: dict):
        """Save scraping statistics for AI analysis."""
        try:
            stats_file = "scrape_stats.jsonl"
            with open(stats_file, "a") as f:
                f.write(json.dumps(stats) + "\n")
        except Exception as e:
            logger.error(f"Failed to save scrape stats: {e}")
    
    def scheduled_scrape(self):
        """The function called by the scheduler."""
        if not self.is_business_hours():
            logger.info("Outside business hours, skipping scrape")
            return
        
        if not self.is_running:
            logger.info("Scraper is stopped, skipping scrape")
            return
        
        logger.info(f"Business hours active, starting scheduled scrape")
        success = self.scrape_permits()
        
        if not success:
            logger.warning(f"Scrape failed (total errors: {self.error_count})")
    
    def start(self):
        """Start the automated scraper."""
        logger.info("Starting automated permit scraper...")
        self.is_running = True
        
        # Schedule the scraping job
        schedule.every(self.interval_minutes).minutes.do(self.scheduled_scrape)
        
        logger.info(f"Scheduler started - will scrape every {self.interval_minutes} minutes during business hours")
        
        # Run an initial scrape if we're in business hours
        if self.is_business_hours():
            logger.info("Running initial scrape...")
            self.scrape_permits()
        
        # Main scheduler loop
        try:
            while self.is_running:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logger.info("Received shutdown signal...")
            self.stop()
    
    def stop(self):
        """Stop the automated scraper."""
        logger.info("Stopping automated permit scraper...")
        self.is_running = False
        schedule.clear()
        
        logger.info(f"Scraper stopped. Final stats:")
        logger.info(f"  - Total scrapes: {self.scrape_count}")
        logger.info(f"  - Total errors: {self.error_count}")
        logger.info(f"  - Last scrape: {self.last_scrape_time}")
    
    def status(self) -> dict:
        """Get current scraper status."""
        return {
            "running": self.is_running,
            "business_hours": self.is_business_hours(),
            "interval_minutes": self.interval_minutes,
            "total_scrapes": self.scrape_count,
            "total_errors": self.error_count,
            "last_scrape": self.last_scrape_time.isoformat() if self.last_scrape_time else None,
            "next_business_hours": self._next_business_hours()
        }
    
    def _next_business_hours(self) -> str:
        """Calculate when the next business hours period starts."""
        now = datetime.now()
        
        # If we're currently in business hours, return current time
        if self.is_business_hours():
            return "Currently in business hours"
        
        # Find next business day
        next_day = now
        while True:
            # If it's a weekday and before start time, return start time today
            if next_day.weekday() < 5 and next_day.hour < self.start_hour:
                return next_day.replace(hour=self.start_hour, minute=0, second=0).isoformat()
            
            # Move to next day at start time
            next_day = (next_day + timedelta(days=1)).replace(hour=self.start_hour, minute=0, second=0)
            
            # If it's a weekday, this is our answer
            if next_day.weekday() < 5:
                return next_day.isoformat()

def main():
    """Main function to run the scraper."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Automated Permit Scraper")
    parser.add_argument("--interval", type=int, default=10, 
                       help="Scraping interval in minutes (default: 10)")
    parser.add_argument("--start-hour", type=int, default=7,
                       help="Start hour in 24-hour format (default: 7)")
    parser.add_argument("--end-hour", type=int, default=18,
                       help="End hour in 24-hour format (default: 18)")
    parser.add_argument("--api-url", default="http://localhost:8000/w1/search",
                       help="API endpoint URL")
    parser.add_argument("--status", action="store_true",
                       help="Show current status and exit")
    
    args = parser.parse_args()
    
    # Create scraper instance
    scraper = PermitScraperScheduler(
        api_url=args.api_url,
        interval_minutes=args.interval,
        start_hour=args.start_hour,
        end_hour=args.end_hour
    )
    
    if args.status:
        status = scraper.status()
        print(json.dumps(status, indent=2))
        return
    
    # Start the scraper
    try:
        scraper.start()
    except Exception as e:
        logger.error(f"Scraper failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
