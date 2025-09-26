#!/usr/bin/env python3
"""
Background cron service that runs alongside the main FastAPI app.
This runs in a separate thread within the same container.
"""

import threading
import time
import schedule
import logging
import requests
from datetime import datetime
import os

logger = logging.getLogger(__name__)

class BackgroundCron:
    """Background cron that runs in the same container as the main app."""
    
    def __init__(self):
        self.running = False
        self.thread = None
        
    def is_business_hours(self) -> bool:
        """Check if current time is within business hours."""
        now = datetime.now()
        
        # Check if it's a weekday (Monday=0, Sunday=6)
        if now.weekday() >= 5:  # Saturday or Sunday
            return False
        
        # Check if it's within business hours (7 AM - 6 PM)
        if now.hour < 7 or now.hour >= 18:
            return False
        
        return True
    
    def scrape_permits(self):
        """Scrape today's permits."""
        try:
            if not self.is_business_hours():
                logger.info("⏰ Outside business hours, skipping scrape")
                return
            
            # Get today's date
            today = datetime.now().strftime("%m/%d/%Y")
            logger.info(f"🔍 Background scrape starting for {today}")
            
            # Use localhost since we're in the same container
            api_url = "http://localhost:8000/w1/search"
            params = {
                'begin': today,
                'end': today,
                'pages': 10
            }
            
            response = requests.get(api_url, params=params, timeout=300)
            
            if response.status_code == 200:
                data = response.json()
                permit_count = len(data.get('items', []))
                logger.info(f"✅ Background scrape completed: {permit_count} permits found")
            else:
                logger.warning(f"⚠️ Background scrape failed: {response.status_code}")
                
        except Exception as e:
            logger.error(f"❌ Background scrape error: {e}")
    
    def run_scheduler(self):
        """Run the scheduler in a background thread."""
        logger.info("🚀 Starting background cron scheduler")
        
        # Schedule scraping every 10 minutes
        schedule.every(10).minutes.do(self.scrape_permits)
        
        # Run initial scrape if in business hours
        if self.is_business_hours():
            logger.info("🔄 Running initial background scrape")
            self.scrape_permits()
        
        while self.running:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
        
        logger.info("🛑 Background cron scheduler stopped")
    
    def start(self):
        """Start the background cron in a separate thread."""
        if self.running:
            return
        
        self.running = True
        self.thread = threading.Thread(target=self.run_scheduler, daemon=True)
        self.thread.start()
        logger.info("✅ Background cron started")
    
    def stop(self):
        """Stop the background cron."""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        logger.info("🛑 Background cron stopped")

# Global instance
background_cron = BackgroundCron()
