#!/usr/bin/env python3
"""
Railway-optimized cron service for permit scraping.
This runs as a separate service that periodically scrapes permits.
"""

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
        logging.StreamHandler()  # Railway captures stdout
    ]
)
logger = logging.getLogger(__name__)

class RailwayCronService:
    """
    Railway-optimized cron service for automated permit scraping.
    """
    
    def __init__(self):
        # Get Railway environment variables
        self.railway_env = os.getenv('RAILWAY_ENVIRONMENT')
        self.database_url = os.getenv('DATABASE_URL')
        
        # Configure API URL based on environment
        if self.railway_env:
            # In Railway, use the internal service URL
            self.api_url = "http://permittracker.up.railway.app/w1/search"
        else:
            # Local development
            self.api_url = "http://localhost:8000/w1/search"
        
        logger.info(f"🚀 Railway Cron Service initialized")
        logger.info(f"   Environment: {self.railway_env or 'local'}")
        logger.info(f"   API URL: {self.api_url}")
        logger.info(f"   Database: {'✅ Connected' if self.database_url else '❌ Not configured'}")
    
    def is_business_hours(self) -> bool:
        """Check if current time is within business hours (7 AM - 6 PM, weekdays)."""
        now = datetime.now()
        
        # Check if it's a weekday (Monday=0, Sunday=6)
        if now.weekday() >= 5:  # Saturday or Sunday
            return False
        
        # Check if it's within business hours (7 AM - 6 PM)
        if now.hour < 7 or now.hour >= 18:
            return False
        
        return True
    
    async def scrape_permits(self) -> Optional[dict]:
        """
        Scrape today's permits using the API endpoint.
        """
        try:
            # Get today's date
            today = datetime.now().strftime("%m/%d/%Y")
            
            logger.info(f"🔍 Scraping permits for {today}")
            
            # Make API request
            params = {
                'begin': today,
                'end': today,
                'pages': 10  # Limit pages to prevent overload
            }
            
            response = requests.get(self.api_url, params=params, timeout=300)  # 5 minute timeout
            
            if response.status_code == 200:
                data = response.json()
                permit_count = len(data.get('items', []))
                
                logger.info(f"✅ Successfully scraped {permit_count} permits")
                
                # Log statistics
                stats = {
                    'timestamp': datetime.now().isoformat(),
                    'permits_found': permit_count,
                    'date_scraped': today,
                    'status': 'success'
                }
                
                return stats
                
            else:
                logger.error(f"❌ API request failed: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Scraping failed: {e}")
            return None
    
    def run_scrape_job(self):
        """Run the scraping job (synchronous wrapper for async function)."""
        try:
            if not self.is_business_hours():
                logger.info("⏰ Outside business hours, skipping scrape")
                return
            
            logger.info("🔄 Starting scheduled scrape...")
            
            # Run the async scraping function
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                stats = loop.run_until_complete(self.scrape_permits())
                if stats:
                    logger.info(f"📊 Scrape completed: {stats['permits_found']} permits found")
                else:
                    logger.warning("⚠️ Scrape completed but no data returned")
            finally:
                loop.close()
                
        except Exception as e:
            logger.error(f"❌ Scrape job failed: {e}")
    
    def start(self):
        """Start the cron service."""
        logger.info("🚀 Starting Railway Cron Service")
        logger.info("=" * 50)
        logger.info("⚙️  Configuration:")
        logger.info("   📅 Schedule: Every 10 minutes during business hours")
        logger.info("   🕐 Business Hours: Monday-Friday, 7:00 AM - 6:00 PM")
        logger.info("   🎯 Target: Today's permits only")
        logger.info("   💾 Database: Railway PostgreSQL")
        logger.info("")
        
        # Schedule the job to run every 10 minutes
        schedule.every(10).minutes.do(self.run_scrape_job)
        
        # Run an initial scrape if we're in business hours
        if self.is_business_hours():
            logger.info("🔄 Running initial scrape...")
            self.run_scrape_job()
        
        # Keep the service running
        logger.info("⏰ Cron service is running... (Press Ctrl+C to stop)")
        
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
                
                # Log heartbeat every hour
                if datetime.now().minute == 0:
                    logger.info(f"💓 Cron service heartbeat - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    
            except KeyboardInterrupt:
                logger.info("🛑 Cron service stopped by user")
                break
            except Exception as e:
                logger.error(f"❌ Cron service error: {e}")
                time.sleep(60)  # Wait a minute before trying again

def main():
    """Main entry point for the cron service."""
    try:
        cron_service = RailwayCronService()
        cron_service.start()
    except Exception as e:
        logger.error(f"❌ Failed to start cron service: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
