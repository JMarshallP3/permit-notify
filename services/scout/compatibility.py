"""
Scout v2.2 Compatibility Layer
Provides fallback functionality when v2.2 database migrations haven't run yet
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

class CompatibilityService:
    """Provides Scout functionality using existing v2.1 database structure"""
    
    def __init__(self, org_id: str = "default_org"):
        self.org_id = org_id
    
    async def create_demo_insights_v22(self) -> List[Dict[str, Any]]:
        """Create demo insights with v2.2 features using the existing structure"""
        
        demo_insights = [
            {
                "id": "demo-v22-1",
                "title": "EOG Resources Permian Basin Activity Surge",
                "what_happened": [
                    "EOG Resources filed 8 new drilling permits in Midland County",
                    "Activity represents 3x increase from previous 30-day average",
                    "Targeting Wolfcamp A and B formations with horizontal wells"
                ],
                "why_it_matters": [
                    "Breakout activity suggests significant resource development",
                    "High permit density indicates active development phase"
                ],
                "confidence": "high",
                "confidence_reasons": ["Multiple sources", "SEC filing corroboration", "Historical pattern analysis"],
                "next_checks": [
                    "Monitor EOG completion schedules for Q4 2025",
                    "Track rig deployment in Midland County",
                    "Check for additional operator announcements"
                ],
                "source_urls": [
                    {"url": "https://www.eogresources.com/news", "label": "EOG Press Release"},
                    {"url": "https://www.sec.gov/edgar", "label": "SEC Filing"}
                ],
                "analytics": {
                    "permit_velocity_7d": 1.2,
                    "permit_velocity_30d": 0.8,
                    "is_breakout": True,
                    "breakout_zscore": 2.4,
                    "is_new_operator": False,
                    "near_term_activity": True,
                    "agreement_score": 0.85,
                    "median_days_permit_to_spud": 42
                },
                "county": "Midland",
                "state": "TX",
                "operator_keys": ["eog"],
                "created_at": datetime.now(timezone.utc).isoformat(),
                "user_state": "default"
            },
            {
                "id": "demo-v22-2", 
                "title": "Pioneer Natural Resources Eagle Ford Expansion",
                "what_happened": [
                    "Pioneer announced $150M investment in Eagle Ford assets",
                    "Plans to drill 12 new horizontal wells in Karnes County",
                    "Expected spud dates in Q1 2026"
                ],
                "why_it_matters": [
                    "First major Pioneer activity in Eagle Ford since 2023",
                    "Signals renewed interest in South Texas plays"
                ],
                "confidence": "medium",
                "confidence_reasons": ["Press release confirmation", "Historical activity gap"],
                "next_checks": [
                    "Monitor permit filings in Karnes County",
                    "Track Pioneer quarterly earnings calls",
                    "Watch for rig contract announcements"
                ],
                "source_urls": [
                    {"url": "https://www.pxd.com/news", "label": "Pioneer Press Release"},
                    {"url": "https://www.hartenergy.com", "label": "Hart Energy News"}
                ],
                "analytics": {
                    "permit_velocity_7d": 0.0,
                    "permit_velocity_30d": 0.1,
                    "is_breakout": False,
                    "is_new_operator": True,
                    "near_term_activity": True,
                    "agreement_score": 0.72
                },
                "county": "Karnes",
                "state": "TX", 
                "operator_keys": ["pioneer"],
                "created_at": datetime.now(timezone.utc).isoformat(),
                "user_state": "default"
            },
            {
                "id": "demo-v22-3",
                "title": "Chevron Permian Completion Acceleration",
                "what_happened": [
                    "Chevron completed 15 wells in Reeves County ahead of schedule",
                    "Average completion time reduced from 45 to 32 days",
                    "Initial production rates exceeding type curve by 20%"
                ],
                "why_it_matters": [
                    "Operational efficiency gains suggest technology improvements",
                    "Higher IP rates indicate premium acreage development"
                ],
                "confidence": "high",
                "confidence_reasons": ["SEC 10-Q filing", "Operational data", "Multiple confirmations"],
                "next_checks": [
                    "Monitor Chevron Q4 production guidance",
                    "Track completion crew deployment",
                    "Compare with peer operator performance"
                ],
                "source_urls": [
                    {"url": "https://www.chevron.com/newsroom", "label": "Chevron Newsroom"},
                    {"url": "https://www.sec.gov/edgar", "label": "SEC 10-Q Filing"},
                    {"url": "https://www.rigzone.com", "label": "Rigzone Analysis"}
                ],
                "analytics": {
                    "permit_velocity_7d": 0.6,
                    "permit_velocity_30d": 0.5,
                    "is_breakout": False,
                    "is_new_operator": False,
                    "near_term_activity": True,
                    "agreement_score": 0.91,
                    "median_days_permit_to_spud": 38
                },
                "county": "Reeves",
                "state": "TX",
                "operator_keys": ["chevron"],
                "created_at": datetime.now(timezone.utc).isoformat(),
                "user_state": "default"
            }
        ]
        
        logger.info(f"Created {len(demo_insights)} demo insights with v2.2 analytics")
        return demo_insights
    
    def get_source_breakdown_demo(self) -> Dict[str, int]:
        """Return demo source breakdown matching the console output"""
        return {
            "forum": 0,
            "news": 1, 
            "pr": 2,
            "social": 0,
            "filing": 1,
            "gov_bulletin": 0
        }
    
    async def simulate_crawl_all_sources(self) -> Dict[str, Any]:
        """Simulate the all-sources crawl with demo data"""
        
        source_breakdown = self.get_source_breakdown_demo()
        total_crawled = sum(source_breakdown.values())
        
        # Create demo insights
        insights = await self.create_demo_insights_v22()
        
        return {
            "total_crawled": total_crawled,
            "signals_created": len(insights),
            "insights_created": len(insights),
            "sources": source_breakdown
        }
