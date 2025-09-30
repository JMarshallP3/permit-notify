"""
Scout v2.1 Deep Analytics Engine
Compute velocity, breakout detection, permit matching, and trend analysis
"""

import logging
import hashlib
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timezone, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc, or_
import statistics
import re
from collections import defaultdict, Counter

from db.session import get_session
from db.models import Permit
from db.scout_models import Signal, ScoutInsight, ConfidenceLevel, ClaimType
import json

logger = logging.getLogger(__name__)

class PermitAnalytics:
    """Deep analytics for permit data and signal correlation"""
    
    def __init__(self):
        self.session = None
    
    def __enter__(self):
        self.session = get_session()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            self.session.close()
    
    def calculate_permit_velocity(self, county: str, operator: str = None, days: int = 30) -> Dict[str, float]:
        """
        Calculates permit velocity for a given county and optional operator.
        Returns permits per day for the specified period.
        """
        with get_session() as session:
            query = session.query(Permit).filter(
                Permit.county.ilike(f"%{county}%"),
                Permit.status_date >= (datetime.now(timezone.utc) - timedelta(days=days)).date()
            )
            if operator:
                query = query.filter(Permit.operator_name.ilike(f"%{operator}%"))
            
            permit_count = query.count()
            return {"permits_per_day": permit_count / days if days > 0 else 0}

    def detect_breakout(self, county: str, operator: str = None) -> bool:
        """
        Detects if there's a significant 'breakout' in permit activity.
        Compares recent activity (7 days) to a longer historical average (90 days).
        """
        velocity_7d = self.calculate_permit_velocity(county, operator, days=7)["permits_per_day"]
        velocity_90d = self.calculate_permit_velocity(county, operator, days=90)["permits_per_day"]
        
        if velocity_90d == 0:
            return velocity_7d > 0  # Any activity is a breakout if no prior activity
        
        # Simple breakout: 7-day velocity is significantly higher than 90-day average
        return velocity_7d > (velocity_90d * 1.5)  # 50% increase over average

    def is_new_operator_in_county(self, operator: str, county: str, lookback_days: int = 365) -> bool:
        """
        Checks if an operator is 'new' to a specific county within a lookback period.
        """
        with get_session() as session:
            existing_permits = session.query(Permit).filter(
                Permit.operator_name.ilike(f"%{operator}%"),
                Permit.county.ilike(f"%{county}%"),
                Permit.created_at >= (datetime.now(timezone.utc) - timedelta(days=lookback_days))
            ).count()
            
            return existing_permits == 0

class SignalMatcher:
    """Matches signals to permits and generates insights."""
    
    def __init__(self, db_session_factory):
        self.db_session_factory = db_session_factory
        self.permit_analytics = PermitAnalytics()

    def match_signal_to_permits(self, signal: Signal) -> List[str]:
        """
        Attempts to match a signal to existing permits based on operator, county, and keywords.
        Returns a list of matching permit status numbers.
        """
        matched_permit_ids = []
        with self.db_session_factory() as session:
            query = session.query(Permit).filter(
                Permit.created_at >= (signal.found_at - timedelta(days=30)),
                Permit.created_at <= (signal.found_at + timedelta(days=30))
            )
            
            if signal.county:
                query = query.filter(Permit.county.ilike(f"%{signal.county}%"))
            
            if signal.operators:
                operator_filters = [Permit.operator_name.ilike(f"%{op}%") for op in signal.operators]
                query = query.filter(or_(*operator_filters))
            
            matched_permits = query.limit(10).all()
            matched_permit_ids = [p.status_no for p in matched_permits]
        
        return matched_permit_ids

    def generate_insight_from_signal(self, signal: Signal) -> Optional[ScoutInsight]:
        """
        Generates a ScoutInsight from a Signal, enriching it with analytics.
        """
        try:
            related_permit_ids = self.match_signal_to_permits(signal)
            
            analytics_data = {}
            if signal.county and signal.operators:
                operator_key = signal.operators[0]
                
                velocity_7d_data = self.permit_analytics.calculate_permit_velocity(signal.county, operator_key, 7)
                velocity_30d_data = self.permit_analytics.calculate_permit_velocity(signal.county, operator_key, 30)
                
                analytics_data['permit_velocity_7d_permits_per_day'] = velocity_7d_data['permits_per_day']
                analytics_data['permit_velocity_30d_permits_per_day'] = velocity_30d_data['permits_per_day']
                
                analytics_data['is_breakout'] = self.permit_analytics.detect_breakout(signal.county, operator_key)
                analytics_data['is_new_operator_in_county'] = self.permit_analytics.is_new_operator_in_county(operator_key, signal.county)

            # Deduplication key
            dedup_key_parts = [
                signal.summary or "",
                ",".join(sorted(signal.operators)),
                signal.county or "",
                signal.source_url
            ]
            dedup_key = hashlib.sha256("".join(filter(None, dedup_key_parts)).encode('utf-8')).hexdigest()

            # Check for existing insight with same dedup_key
            try:
                with self.db_session_factory() as session:
                    existing_insight = session.query(ScoutInsight).filter(ScoutInsight.dedup_key == dedup_key).first()
                    if existing_insight:
                        logger.info(f"Skipping duplicate insight for signal {signal.id} with dedup_key {dedup_key}")
                        return None
            except Exception as e:
                if "does not exist" in str(e) or "UndefinedTable" in str(e):
                    logger.warning("Scout tables don't exist yet - skipping duplicate check")
                else:
                    raise

            # Generate insight content
            what_happened = []
            why_it_matters = []
            
            if signal.operators and signal.county:
                what_happened.append(f"{signal.operators[0]} activity mentioned in {signal.county} County")
                why_it_matters.append("Potential new drilling activity based on forum discussion")
            
            if analytics_data.get('is_breakout'):
                why_it_matters.append("Recent permit activity shows statistical breakout pattern")
            
            if analytics_data.get('is_new_operator_in_county'):
                why_it_matters.append("New operator entry into this county")

            # Construct insight
            insight = ScoutInsight(
                org_id=signal.org_id,
                title=f"Activity Signal: {signal.county or 'Unknown County'} - {signal.operators[0] if signal.operators else 'Unknown Operator'}",
                what_happened=what_happened or ["Forum discussion about potential oil/gas activity"],
                why_it_matters=why_it_matters or ["Indicates potential future drilling activity"],
                confidence=ConfidenceLevel.MEDIUM,
                confidence_reasons=["Derived from public forum discussion"],
                next_checks=["Monitor RRC permits for this area", "Check for additional operator announcements"],
                source_urls=[{"url": signal.source_url, "title": "MRF Discussion"}],
                related_permit_ids=related_permit_ids,
                county=signal.county,
                state=signal.state,
                operator_keys=signal.operators,
                analytics=analytics_data,
                dedup_key=dedup_key
            )
            return insight
        except Exception as e:
            if "does not exist" in str(e) or "UndefinedTable" in str(e):
                logger.warning("Scout tables don't exist yet - cannot generate insight")
                return None
            else:
                logger.error(f"Error generating insight from signal: {e}")
                raise

class SignalProcessor:
    """Processes crawled content into structured signals"""
    
    def __init__(self):
        # Texas counties for matching
        self.texas_counties = {
            'andrews', 'atascosa', 'austin', 'bastrop', 'bee', 'brazoria', 'brazos', 
            'burleson', 'caldwell', 'calhoun', 'colorado', 'dewitt', 'dimmit', 'duval',
            'eagle ford', 'fayette', 'frio', 'goliad', 'gonzales', 'grimes', 'harris',
            'jackson', 'karnes', 'lavaca', 'lee', 'leon', 'live oak', 'madison', 
            'matagorda', 'mcmullen', 'milam', 'nueces', 'refugio', 'robertson', 'travis',
            'victoria', 'walker', 'washington', 'webb', 'wilson', 'zavala', 'reeves',
            'ward', 'winkler', 'loving', 'culberson', 'pecos', 'terrell', 'brewster',
            'presidio', 'jeff davis', 'hudspeth', 'el paso', 'crane', 'upton', 'midland',
            'ector', 'glasscock', 'sterling', 'coke', 'runnels', 'coleman', 'brown',
            'comanche', 'erath', 'hood', 'johnson', 'ellis', 'navarro', 'freestone',
            'limestone', 'falls', 'bell', 'williamson', 'burnet', 'llano', 'mason',
            'menard', 'schleicher', 'sutton', 'kimble', 'gillespie', 'kerr', 'bandera',
            'medina', 'uvalde', 'kinney', 'val verde', 'edwards', 'real', 'kendall',
            'comal', 'hays', 'blanco', 'san saba', 'lampasas', 'coryell', 'hamilton'
        }
        
        # Common operators for matching
        self.operators = {
            'exxon', 'chevron', 'conocophillips', 'eog', 'pioneer', 'devon', 'apache',
            'anadarko', 'occidental', 'marathon', 'hess', 'murphy', 'noble', 'newfield',
            'range', 'chesapeake', 'encana', 'cabot', 'southwestern', 'antero',
            'consol', 'cnx', 'eclipse', 'gulfport', 'kinder morgan', 'enterprise',
            'plains', 'energy transfer', 'enbridge', 'tc energy', 'williams',
            'oneok', 'magellan', 'buckeye', 'phillips 66', 'valero', 'motiva',
            'shell', 'bp', 'total', 'equinor', 'eni', 'repsol', 'petrobras',
            'pemex', 'suncor', 'imperial', 'husky', 'cenovus', 'canadian natural'
        }
        
        # Keywords that indicate oil/gas activity
        self.activity_keywords = {
            'drilling', 'completion', 'fracking', 'hydraulic fracturing', 'spud',
            'rig', 'wellbore', 'lateral', 'horizontal', 'vertical', 'permit',
            'lease', 'acreage', 'mineral rights', 'royalty', 'bonus', 'spacing',
            'unit', 'pooling', 'force pooling', 'compulsory pooling', 'pipeline',
            'gathering', 'processing', 'refinery', 'terminal', 'storage',
            'crude oil', 'natural gas', 'condensate', 'ngl', 'lng', 'lpg',
            'shale', 'tight oil', 'unconventional', 'conventional', 'offshore',
            'onshore', 'upstream', 'midstream', 'downstream', 'exploration',
            'development', 'production', 'reserves', 'resources', 'basin',
            'play', 'formation', 'zone', 'reservoir', 'field', 'prospect'
        }
    
    def process_crawl_result(self, crawl_result, org_id: str = "default_org") -> Optional[Signal]:
        """Convert a crawl result into a structured signal"""
        if not crawl_result.success or not crawl_result.content:
            return None
        
        content = crawl_result.content.lower()
        
        # Extract operators
        operators = []
        for operator in self.operators:
            if operator in content:
                operators.append(operator.title())
        
        # Extract counties
        counties = []
        for county in self.texas_counties:
            if county in content:
                counties.append(county.title())
        
        # Extract keywords
        keywords = []
        for keyword in self.activity_keywords:
            if keyword in content:
                keywords.append(keyword)
        
        # Only create signal if we found relevant content
        if not operators and not counties and len(keywords) < 2:
            return None
        
        # Determine claim type based on content
        claim_type = ClaimType.RUMOR  # Default
        if any(word in content for word in ['confirmed', 'announced', 'filed', 'approved']):
            claim_type = ClaimType.CONFIRMED
        elif any(word in content for word in ['likely', 'expected', 'planned']):
            claim_type = ClaimType.LIKELY
        elif any(word in content for word in ['speculation', 'rumor', 'might', 'could']):
            claim_type = ClaimType.SPECULATION
        
        # Extract timeframe
        timeframe = None
        if any(word in content for word in ['soon', 'immediate', 'next month', 'this quarter']):
            timeframe = 'near-term'
        elif any(word in content for word in ['next year', 'long term', '2025', '2026']):
            timeframe = 'long-term'
        
        # Create signal
        signal = Signal(
            org_id=org_id,
            found_at=crawl_result.post_date or datetime.now(timezone.utc),
            source_url=crawl_result.url,
            source_type="MineralRightsForum",
            state="TX",  # Assuming Texas for now
            county=counties[0] if counties else None,
            operators=operators,
            unit_tokens=[],  # Could extract lease/unit names
            keywords=keywords,
            claim_type=claim_type,
            timeframe=timeframe,
            summary=crawl_result.content[:500],  # First 500 chars
            raw_excerpt=crawl_result.content
        )
        
        return signal
    
    async def save_signals_to_db(self, signals: List[Signal]) -> int:
        """Save signals to database, avoiding duplicates"""
        saved_count = 0
        
        try:
            with get_session() as session:
                for signal in signals:
                    # Check for duplicate based on URL and content hash
                    content_hash = hashlib.md5(signal.raw_excerpt.encode()).hexdigest()
                    
                    existing = session.query(Signal).filter(
                        or_(
                            Signal.source_url == signal.source_url,
                            Signal.raw_excerpt.contains(content_hash[:20])  # Partial match
                        )
                    ).first()
                    
                    if not existing:
                        session.add(signal)
                        saved_count += 1
                
                session.commit()
        except Exception as e:
            if "does not exist" in str(e) or "UndefinedTable" in str(e):
                logger.warning("Scout tables don't exist yet - signals not saved to database")
                return 0
            else:
                logger.error(f"Error saving signals to database: {e}")
                raise
        
        return saved_count
