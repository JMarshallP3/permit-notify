#!/usr/bin/env python3
"""
Automated enrichment scheduler for Railway deployment.
Runs enrichment jobs automatically to process new permits.
"""

import asyncio
import aiohttp
import logging
import os
from datetime import datetime, time
import schedule
import time as time_module

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EnrichmentScheduler:
    """Scheduler for automated permit enrichment."""
    
    def __init__(self, base_url: str = None):
        self.base_url = base_url or os.getenv('BASE_URL', 'https://permittracker.up.railway.app')
        self.session = None
        
    async def create_session(self):
        """Create HTTP session."""
        if self.session is None:
            self.session = aiohttp.ClientSession()
    
    async def close_session(self):
        """Close HTTP session."""
        if self.session:
            await self.session.close()
            self.session = None
    
    async def run_enrichment(self, batch_size: int = 10, max_batches: int = 5):
        """Run enrichment via API call."""
        try:
            await self.create_session()
            
            url = f"{self.base_url}/enrich/auto"
            params = {
                'batch_size': batch_size,
                'max_batches': max_batches
            }
            
            logger.info(f"🔄 Starting automated enrichment: {batch_size} per batch, max {max_batches} batches")
            
            async with self.session.post(url, params=params) as response:
                if response.status == 200:
                    result = await response.json()
                    logger.info(f"✅ Enrichment completed: {result['processed']} processed, "
                               f"{result['successful']} successful, {result['failed']} failed")
                    return result
                else:
                    error_text = await response.text()
                    logger.error(f"❌ Enrichment failed with status {response.status}: {error_text}")
                    return None
                    
        except Exception as e:
            logger.error(f"❌ Error running enrichment: {e}")
            return None
    
    def schedule_enrichment_job(self):
        """Wrapper for scheduled enrichment job."""
        try:
            # Run the async enrichment
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(self.run_enrichment())
            loop.close()
            
            if result:
                logger.info(f"📊 Scheduled enrichment completed successfully")
            else:
                logger.warning(f"⚠️  Scheduled enrichment had issues")
                
        except Exception as e:
            logger.error(f"❌ Scheduled enrichment failed: {e}")

def main():
    """Main scheduler loop."""
    logger.info("🚀 Starting Permit Enrichment Scheduler")
    logger.info("=" * 50)
    
    scheduler = EnrichmentScheduler()
    
    # Schedule enrichment jobs
    # Run every 2 hours during business hours (7 AM - 7 PM)
    schedule.every().day.at("07:00").do(scheduler.schedule_enrichment_job)
    schedule.every().day.at("09:00").do(scheduler.schedule_enrichment_job)
    schedule.every().day.at("11:00").do(scheduler.schedule_enrichment_job)
    schedule.every().day.at("13:00").do(scheduler.schedule_enrichment_job)
    schedule.every().day.at("15:00").do(scheduler.schedule_enrichment_job)
    schedule.every().day.at("17:00").do(scheduler.schedule_enrichment_job)
    schedule.every().day.at("19:00").do(scheduler.schedule_enrichment_job)
    
    logger.info("📅 Scheduled enrichment jobs:")
    logger.info("   • 7:00 AM - Initial morning run")
    logger.info("   • 9:00 AM, 11:00 AM, 1:00 PM, 3:00 PM, 5:00 PM, 7:00 PM - Regular runs")
    logger.info("   • Processes up to 50 permits per run (10 per batch, 5 batches max)")
    
    # Run initial enrichment
    logger.info("\n🔄 Running initial enrichment...")
    scheduler.schedule_enrichment_job()
    
    # Main scheduler loop
    logger.info("\n⏰ Scheduler started - waiting for scheduled times...")
    try:
        while True:
            schedule.run_pending()
            time_module.sleep(60)  # Check every minute
            
    except KeyboardInterrupt:
        logger.info("\n🛑 Scheduler stopped by user")
    except Exception as e:
        logger.error(f"❌ Scheduler error: {e}")
    finally:
        # Clean up
        asyncio.run(scheduler.close_session())

if __name__ == "__main__":
    main()
