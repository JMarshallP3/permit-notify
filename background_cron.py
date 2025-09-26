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
        """Scrape and enrich today's permits using the combined endpoint."""
        try:
            if not self.is_business_hours():
                logger.info("⏰ Outside business hours, skipping scrape")
                return
            
            # Get today's date
            today = datetime.now().strftime("%m/%d/%Y")
            logger.info(f"🔍 Background scrape-and-enrich starting for {today}")
            
            # Step 1: Scrape today's permits
            scrape_url = "http://localhost:8000/w1/search"
            scrape_params = {
                'begin': today,
                'end': today,
                'pages': 5
            }
            
            permits_found = 0
            try:
                scrape_response = requests.get(scrape_url, params=scrape_params, timeout=300)
                if scrape_response.status_code == 200:
                    scrape_data = scrape_response.json()
                    permits_found = len(scrape_data.get('items', []))
                    db_info = scrape_data.get('database', {})
                    permits_inserted = db_info.get('inserted', 0)
                    permits_updated = db_info.get('updated', 0)
                    
                    logger.info(f"✅ Background scrape completed:")
                    logger.info(f"   📊 Permits found: {permits_found}")
                    logger.info(f"   ➕ New permits: {permits_inserted}")
                    logger.info(f"   🔄 Updated permits: {permits_updated}")
                else:
                    logger.warning(f"⚠️ Background scrape failed: {scrape_response.status_code}")
            except Exception as scrape_error:
                logger.error(f"❌ Background scrape error: {scrape_error}")
            
            # Step 2: Enrich permits (using available endpoint)
            if permits_found > 0 or True:  # Always try enrichment
                enrich_url = "http://localhost:8000/enrich/run"
                enrich_params = {'n': min(20, max(5, permits_found))}  # Enrich 5-20 permits
                
                # Retry with backoff for server restarts
                max_retries = 3
                retry_delay = 5  # seconds
                
                for attempt in range(max_retries):
                    try:
                        enrich_response = requests.post(enrich_url, params=enrich_params, timeout=600)
                        
                        if enrich_response.status_code == 200:
                            enrich_data = enrich_response.json()
                            processed = enrich_data.get('processed', 0)
                            successful = enrich_data.get('ok', 0)
                            errors = enrich_data.get('errors', 0)
                            
                            logger.info(f"✅ Background enrichment completed:")
                            logger.info(f"   🔍 Processed: {processed}")
                            logger.info(f"   ✅ Successful: {successful}")
                            logger.info(f"   ❌ Errors: {errors}")
                            break  # Success, exit retry loop
                            
                        elif enrich_response.status_code == 404:
                            if attempt < max_retries - 1:
                                logger.info(f"🔄 Server restarting (404), retrying in {retry_delay}s... (attempt {attempt + 1}/{max_retries})")
                                time.sleep(retry_delay)
                                continue
                            else:
                                logger.warning(f"⚠️ Background enrichment failed after {max_retries} attempts: 404")
                        else:
                            logger.warning(f"⚠️ Background enrichment failed: {enrich_response.status_code}")
                            if enrich_response.text:
                                logger.warning(f"   Response: {enrich_response.text[:200]}")
                            break
                            
                    except requests.exceptions.ConnectionError as e:
                        if attempt < max_retries - 1:
                            logger.info(f"🔄 Connection failed, server may be restarting. Retrying in {retry_delay}s... (attempt {attempt + 1}/{max_retries})")
                            time.sleep(retry_delay)
                            continue
                        else:
                            logger.warning(f"⚠️ Background enrichment connection failed after {max_retries} attempts: {e}")
                    except Exception as e:
                        logger.error(f"❌ Background enrichment error: {e}")
                        break
                
        except Exception as e:
            logger.error(f"❌ Background scrape-and-enrich error: {e}")
    
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
