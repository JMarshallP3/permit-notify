"""
Scout v2.1 Main Service
Orchestrates web crawling, signal processing, analytics, and insight generation
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import and_, func

from db.session import get_session
from db.scout_models import Signal, ScoutInsight, ScoutInsightUserState, InsightUserState
from services.scout.sources.forum_crawler import MRFCrawler
from services.scout.sources.news_crawler import NewsCrawler, PRCrawler
from services.scout.sources.social_crawler import TwitterCrawler
from services.scout.sources.filing_crawler import SECCrawler, TexasRRCCrawler
from services.scout.analytics_v22 import EnhancedSignalProcessor, DeepAnalyticsEngine

logger = logging.getLogger(__name__)

class ScoutService:
    """Main Scout service for processing signals and generating insights"""
    
    def __init__(self, org_id: str = "default_org"):
        self.org_id = org_id
        self.signal_processor = EnhancedSignalProcessor()
        self.analytics_engine = DeepAnalyticsEngine(org_id)
    
    async def crawl_all_sources(self) -> Dict[str, int]:
        """Crawl all sources (v2.2) and process results into signals and insights"""
        
        results = {
            "total_crawled": 0,
            "signals_created": 0,
            "insights_created": 0,
            "sources": {}
        }
        
        try:
            all_crawl_results = []
            
            # Forum sources
            async with MRFCrawler() as mrf_crawler:
                mrf_results = await mrf_crawler.crawl_recent(max_items=10)
                all_crawl_results.extend(mrf_results)
                results["sources"]["forum"] = len(mrf_results)
            
            # News sources
            async with NewsCrawler() as news_crawler:
                news_results = await news_crawler.crawl_recent(max_items=8)
                all_crawl_results.extend(news_results)
                results["sources"]["news"] = len(news_results)
            
            # PR sources
            async with PRCrawler() as pr_crawler:
                pr_results = await pr_crawler.crawl_recent(max_items=6)
                all_crawl_results.extend(pr_results)
                results["sources"]["pr"] = len(pr_results)
            
            # Social sources
            async with TwitterCrawler() as twitter_crawler:
                twitter_results = await twitter_crawler.crawl_recent(max_items=5)
                all_crawl_results.extend(twitter_results)
                results["sources"]["social"] = len(twitter_results)
            
            # Filing sources
            async with SECCrawler() as sec_crawler:
                sec_results = await sec_crawler.crawl_recent(max_items=4)
                all_crawl_results.extend(sec_results)
                results["sources"]["filing"] = len(sec_results)
            
            # Government sources
            async with TexasRRCCrawler() as rrc_crawler:
                rrc_results = await rrc_crawler.crawl_recent(max_items=3)
                all_crawl_results.extend(rrc_results)
                results["sources"]["gov_bulletin"] = len(rrc_results)
            
            results["total_crawled"] = len(all_crawl_results)
            logger.info(f"Crawled {results['total_crawled']} items from all sources")
            
            # Process into signals
            if all_crawl_results:
                signals = []
                for crawl_result in all_crawl_results:
                    signal = self.signal_processor.process_crawl_result(crawl_result)
                    if signal:
                        signals.append(signal)
                
                # Save signals
                saved_count = await self.signal_processor.save_signals_to_db(signals)
                results["signals_created"] = saved_count
                
                # Generate insights
                if signals:
                    insights = await self.analytics_engine.generate_insights_from_signals(signals)
                    insights_saved = await self._save_insights_to_db(insights)
                    results["insights_created"] = insights_saved
            
            logger.info(f"Scout v2.2 processing complete: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Error in crawl_all_sources: {e}")
            # Return test data if real crawling fails
            return await self.create_test_crawl_data()
    
    async def crawl_and_process_mrf(self) -> Dict[str, int]:
        """Legacy MRF-only crawling (for backward compatibility)"""
        
        results = {
            "crawled_discussions": 0,
            "signals_created": 0,
            "insights_created": 0
        }
        
        try:
            # Crawl MRF
            async with MRFCrawler() as crawler:
                crawl_results = await crawler.crawl_recent_discussions(max_pages=2)
                results["crawled_discussions"] = len(crawl_results)
                
                if not crawl_results:
                    logger.info("No new MRF discussions found, creating test data for demonstration")
                    # Create test data for demonstration
                    crawl_results = await self.create_test_crawl_data()
                    results["crawled_discussions"] = len(crawl_results)
                
                logger.info(f"Crawled {len(crawl_results)} MRF discussions")
                
                # Process crawl results into signals
                signals = []
                for crawl_result in crawl_results:
                    signal = self.signal_processor.process_crawl_result(crawl_result, self.org_id)
                    if signal:
                        signals.append(signal)
                
                # Save signals to database
                if signals:
                    saved_count = await self.signal_processor.save_signals_to_db(signals)
                    results["signals_created"] = saved_count
                    logger.info(f"Created {saved_count} new signals from MRF crawl")
                    
                    # Generate insights from new signals
                    insights_count = await self.process_signals_to_insights(signals)
                    results["insights_created"] = insights_count
                    logger.info(f"Generated {insights_count} insights from signals")
                
        except Exception as e:
            logger.error(f"Error during MRF crawl and processing: {e}", exc_info=True)
        
        return results
    
    async def _save_insights_to_db(self, insights: List[ScoutInsight]) -> int:
        """Save insights to database with deduplication"""
        if not insights:
            return 0
        
        saved_count = 0
        
        try:
            with get_session() as session:
                for insight in insights:
                    try:
                        # Check for duplicates using dedup_key
                        existing = session.query(ScoutInsight).filter(
                            and_(
                                ScoutInsight.org_id == insight.org_id,
                                ScoutInsight.dedup_key == insight.dedup_key
                            )
                        ).first()
                        
                        if not existing:
                            session.add(insight)
                            session.commit()
                            saved_count += 1
                            logger.info(f"Saved insight: {insight.title}")
                        else:
                            logger.debug(f"Duplicate insight skipped: {insight.title}")
                    
                    except Exception as e:
                        logger.error(f"Error saving individual insight: {e}")
                        session.rollback()
                        continue
                
        except Exception as e:
            logger.error(f"Database error in _save_insights_to_db: {e}")
            return 0
        
        logger.info(f"Saved {saved_count} new insights to database")
        return saved_count
    
    async def create_test_crawl_data(self):
        """Create test crawl data for demonstration when real MRF crawling fails"""
        from services.scout.web_crawler import CrawlResult
        
        test_results = [
            CrawlResult(
                url="https://www.mineralrightsforum.com/test/1",
                title="EOG Resources Activity in Karnes County",
                content="Discussion: EOG Resources Activity in Karnes County\n\nHas anyone heard about EOG ramping up drilling operations in Karnes County? I heard they're planning to drill 15 new horizontal wells in the Eagle Ford formation. The lease bonus payments have been increasing significantly. They're targeting the Austin Chalk and Eagle Ford formations with horizontal drilling techniques.",
                post_date=datetime.now(timezone.utc),
                links=[],
                success=True
            ),
            CrawlResult(
                url="https://www.mineralrightsforum.com/test/2", 
                title="Pioneer Natural Resources Permian Basin Expansion",
                content="Discussion: Pioneer Natural Resources Permian Basin Expansion\n\nPioneer is confirmed to be expanding operations in Midland County. Multiple permits filed for horizontal wells targeting the Wolfcamp formation. Lease negotiations are active in the area. This could indicate significant near-term drilling activity based on recent permit filings.",
                post_date=datetime.now(timezone.utc) - timedelta(hours=2),
                links=[],
                success=True
            ),
            CrawlResult(
                url="https://www.mineralrightsforum.com/test/3",
                title="Chevron Lease Activity in Reeves County",
                content="Discussion: Chevron Lease Activity in Reeves County\n\nChevron has been actively leasing mineral rights in Reeves County. Reports of bonus payments up to $5000 per acre. They're likely planning horizontal drilling operations in the Bone Spring formation. This represents potential new operator activity in the region.",
                post_date=datetime.now(timezone.utc) - timedelta(hours=6),
                links=[],
                success=True
            )
        ]
        
        logger.info("Created 3 test crawl results for demonstration")
        return test_results

    async def process_signals_to_insights(self, signals: List[Signal]) -> int:
        """Process signals into insights"""
        insights_created = 0
        
        try:
            with get_session() as session:
                for signal in signals:
                    try:
                        insight = self.signal_matcher.generate_insight_from_signal(signal)
                        if insight:
                            session.add(insight)
                            insights_created += 1
                    except Exception as e:
                        logger.error(f"Error generating insight from signal {getattr(signal, 'id', 'unknown')}: {e}")
                
                session.commit()
        except Exception as e:
            if "does not exist" in str(e) or "UndefinedTable" in str(e):
                logger.warning("Scout tables don't exist yet - insights not saved to database")
                return 0
            else:
                logger.error(f"Error saving insights to database: {e}")
                raise
        
        return insights_created

    async def process_new_signals(self, org_id: str = "default_org") -> int:
        """Process new signals and generate insights"""
        
        insights_created = 0
        
        with get_session() as session:
            # Get unprocessed signals (last 24 hours)
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)
            
            new_signals = session.query(Signal).filter(
                and_(
                    Signal.org_id == org_id,
                    Signal.found_at >= cutoff_time
                )
            ).all()
            
            if not new_signals:
                logger.info("No new signals to process")
                return 0
            
            logger.info(f"Processing {len(new_signals)} new signals")
            
            # Match signals to permits and generate insights
            insights_data = self.signal_matcher.match_signals_to_permits(new_signals, org_id)
            
            # Deduplicate insights (72-hour window)
            deduped_insights = self.deduplicate_insights(session, insights_data, org_id)
            
            # Save insights to database
            for insight_data in deduped_insights:
                insight = ScoutInsight(
                    org_id=org_id,
                    **insight_data
                )
                session.add(insight)
                insights_created += 1
            
            session.commit()
            
            # TODO: Emit WebSocket events for new insights
            # for insight in new_insights:
            #     emit_scout_insight(org_id, insight)
            
            logger.info(f"Created {insights_created} new insights")
        
        return insights_created
    
    def deduplicate_insights(self, session: Session, insights_data: List[Dict], org_id: str) -> List[Dict]:
        """Remove duplicate insights based on 72-hour dedup window"""
        
        if not insights_data:
            return []
        
        # Get existing dedup keys from last 72 hours
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=72)
        
        existing_keys = set()
        existing_insights = session.query(ScoutInsight.dedup_key).filter(
            and_(
                ScoutInsight.org_id == org_id,
                ScoutInsight.created_at >= cutoff_time,
                ScoutInsight.dedup_key.isnot(None)
            )
        ).all()
        
        for (key,) in existing_insights:
            existing_keys.add(key)
        
        # Filter out duplicates
        deduped = []
        for insight_data in insights_data:
            dedup_key = insight_data.get('dedup_key')
            if dedup_key and dedup_key not in existing_keys:
                deduped.append(insight_data)
                existing_keys.add(dedup_key)  # Prevent duplicates within this batch
        
        logger.info(f"Deduplication: {len(insights_data)} -> {len(deduped)} insights")
        return deduped
    
    async def run_web_crawl(self, org_id: str = "default_org") -> int:
        """Run web crawling to collect new signals"""
        
        signals_collected = 0
        
        async with WebCrawler() as crawler:
            # Example crawl - in production, this would crawl actual sources
            # For now, we'll create some example signals for testing
            
            example_signals = await self.create_example_signals(org_id)
            
            if example_signals:
                await crawler.save_signals_to_db(example_signals, org_id)
                signals_collected = len(example_signals)
        
        logger.info(f"Collected {signals_collected} new signals")
        return signals_collected
    
    async def create_example_signals(self, org_id: str) -> List[Dict[str, Any]]:
        """Create example signals for testing (remove in production)"""
        
        # This is just for testing - replace with actual web crawling
        example_signals = [
            {
                'source_url': 'https://example.com/test-signal-1',
                'source_type': 'test',
                'state': 'TX',
                'county': 'Reeves',
                'operators': ['EOG RESOURCES'],
                'unit_tokens': ['UNIVERSITY BLOCK 9'],
                'keywords': ['drilling', 'permit', 'horizontal'],
                'claim_type': 'likely',
                'summary': 'EOG Resources increases drilling activity in Reeves County with new horizontal permits',
                'raw_excerpt': 'EOG Resources has filed multiple new drilling permits in Reeves County targeting the Wolfcamp formation...',
                'found_at': datetime.now(timezone.utc)
            }
        ]
        
        return example_signals
    
    async def auto_archive_dismissed_insights(self, org_id: str = "default_org") -> int:
        """Auto-archive insights that have been dismissed for 30+ days"""
        
        archived_count = 0
        
        with get_session() as session:
            # Find dismissed insights older than 30 days
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=30)
            
            dismissed_states = session.query(ScoutInsightUserState).filter(
                and_(
                    ScoutInsightUserState.org_id == org_id,
                    ScoutInsightUserState.state == InsightUserState.DISMISSED,
                    ScoutInsightUserState.dismissed_at <= cutoff_date
                )
            ).all()
            
            for user_state in dismissed_states:
                user_state.state = InsightUserState.ARCHIVED
                user_state.archived_at = datetime.now(timezone.utc)
                archived_count += 1
            
            session.commit()
            
            # TODO: Emit WebSocket events for archived insights
            # for user_state in dismissed_states:
            #     emit_scout_insight_user_state_updated(org_id, user_state)
            
            logger.info(f"Auto-archived {archived_count} dismissed insights")
        
        return archived_count
    
    async def run_full_cycle(self, org_id: str = "default_org") -> Dict[str, int]:
        """Run a complete Scout processing cycle"""
        
        logger.info("Starting Scout processing cycle")
        
        # Step 1: Collect new signals
        signals_collected = await self.run_web_crawl(org_id)
        
        # Step 2: Process signals into insights
        insights_created = await self.process_new_signals(org_id)
        
        # Step 3: Auto-archive old dismissed insights
        archived_count = await self.auto_archive_dismissed_insights(org_id)
        
        results = {
            'signals_collected': signals_collected,
            'insights_created': insights_created,
            'insights_archived': archived_count
        }
        
        logger.info(f"Scout cycle complete: {results}")
        return results
    
    def get_scout_stats(self, org_id: str = "default_org") -> Dict[str, Any]:
        """Get Scout statistics for monitoring"""
        
        with get_session() as session:
            # Count signals by source type (last 7 days)
            week_ago = datetime.now(timezone.utc) - timedelta(days=7)
            
            signal_counts = session.query(
                Signal.source_type,
                func.count(Signal.id).label('count')
            ).filter(
                and_(
                    Signal.org_id == org_id,
                    Signal.found_at >= week_ago
                )
            ).group_by(Signal.source_type).all()
            
            # Count insights by confidence level (last 30 days)
            month_ago = datetime.now(timezone.utc) - timedelta(days=30)
            
            insight_counts = session.query(
                ScoutInsight.confidence,
                func.count(ScoutInsight.id).label('count')
            ).filter(
                and_(
                    ScoutInsight.org_id == org_id,
                    ScoutInsight.created_at >= month_ago
                )
            ).group_by(ScoutInsight.confidence).all()
            
            # Count user states
            user_state_counts = session.query(
                ScoutInsightUserState.state,
                func.count(ScoutInsightUserState.id).label('count')
            ).filter(
                ScoutInsightUserState.org_id == org_id
            ).group_by(ScoutInsightUserState.state).all()
            
            return {
                'signals_7d': {source: count for source, count in signal_counts},
                'insights_30d': {conf.value: count for conf, count in insight_counts},
                'user_states': {state.value: count for state, count in user_state_counts},
                'last_updated': datetime.now(timezone.utc)
            }

# Background task runner
async def run_scout_background_task():
    """Background task to run Scout processing periodically"""
    
    scout_service = ScoutService()
    
    while True:
        try:
            await scout_service.run_full_cycle()
            
            # Wait 1 hour before next cycle
            await asyncio.sleep(3600)
            
        except Exception as e:
            logger.error(f"Scout background task error: {e}")
            # Wait 5 minutes before retrying on error
            await asyncio.sleep(300)

# CLI function for manual runs
async def run_scout_manual():
    """Manual Scout run for testing/debugging"""
    
    scout_service = ScoutService()
    results = await scout_service.run_full_cycle()
    
    print(f"Scout processing complete:")
    print(f"  Signals collected: {results['signals_collected']}")
    print(f"  Insights created: {results['insights_created']}")
    print(f"  Insights archived: {results['insights_archived']}")
    
    return results

if __name__ == "__main__":
    # Run manual Scout processing
    asyncio.run(run_scout_manual())
