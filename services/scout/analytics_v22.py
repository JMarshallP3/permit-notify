"""
Scout v2.2 Enhanced Analytics Engine
Deep analytics with velocity, breakout detection, sophisticated matching, and multi-source correlation
"""

import re
import hashlib
import logging
import json
import statistics
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Set, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import or_, func, and_, desc
from collections import defaultdict, Counter
from difflib import SequenceMatcher

from db.session import get_session
from db.scout_models import Signal, ScoutInsight, ConfidenceLevel, ClaimType, SourceType, Timeframe
from services.scout.sources.base_crawler import CrawlResult
from db.models import Permit

logger = logging.getLogger(__name__)

class OperatorAliasMap:
    """Enhanced operator aliasing for better matching"""
    
    def __init__(self):
        self.aliases = {
            # EOG Resources variations
            "eog": ["eog resources", "eog resources inc", "enron oil & gas", "eog res"],
            
            # ConocoPhillips variations  
            "conocophillips": ["conoco phillips", "conocophillips company", "cop", "conoco"],
            
            # Chevron variations
            "chevron": ["chevron corporation", "chevron corp", "cvx", "chevron usa"],
            
            # ExxonMobil variations
            "exxonmobil": ["exxon mobil", "exxon", "mobil", "xom", "exxon mobil corp"],
            
            # Pioneer variations
            "pioneer": ["pioneer natural resources", "pxd", "pioneer natural", "pioneer nr"],
            
            # Devon variations
            "devon": ["devon energy", "devon energy corp", "dvn", "devon energy corporation"],
            
            # Diamondback variations
            "diamondback": ["diamondback energy", "fang", "diamondback energy inc"],
            
            # Coterra variations
            "coterra": ["coterra energy", "ctra", "coterra energy inc"],
            
            # Ovintiv variations
            "ovintiv": ["ovintiv inc", "ovv", "encana", "encana corporation"],
            
            # Marathon variations
            "marathon": ["marathon oil", "mro", "marathon oil corp", "marathon petroleum"],
            
            # Hess variations
            "hess": ["hess corporation", "hes", "hess corp"],
            
            # Apache variations
            "apache": ["apache corporation", "apa", "apache corp"],
            
            # Kinder Morgan variations
            "kinder_morgan": ["kinder morgan", "kmi", "kinder morgan inc"],
            
            # Enterprise variations
            "enterprise": ["enterprise products", "epd", "enterprise products partners"]
        }
        
        # Create reverse lookup
        self.reverse_map = {}
        for canonical, variations in self.aliases.items():
            for variation in variations:
                self.reverse_map[variation.lower()] = canonical
            self.reverse_map[canonical.lower()] = canonical
    
    def normalize_operator(self, operator: str) -> str:
        """Normalize operator name to canonical form"""
        if not operator:
            return ""
        
        operator_clean = re.sub(r'[^\w\s]', '', operator.lower()).strip()
        
        # Direct lookup
        if operator_clean in self.reverse_map:
            return self.reverse_map[operator_clean]
        
        # Fuzzy matching for partial matches
        best_match = None
        best_score = 0.0
        
        for variation, canonical in self.reverse_map.items():
            score = SequenceMatcher(None, operator_clean, variation).ratio()
            if score > 0.8 and score > best_score:
                best_match = canonical
                best_score = score
        
        return best_match or operator_clean

class CountyPlayDictionary:
    """Enhanced county and play/basin mapping"""
    
    def __init__(self):
        self.texas_counties = {
            # Permian Basin counties
            "permian": ["midland", "martin", "howard", "glasscock", "reagan", "upton", 
                       "andrews", "ector", "crane", "ward", "winkler", "loving", "pecos"],
            
            # Eagle Ford counties
            "eagle_ford": ["karnes", "dewitt", "gonzales", "lavaca", "fayette", "atascosa",
                          "mcmullen", "live oak", "bee", "goliad", "wilson", "guadalupe"],
            
            # Haynesville counties  
            "haynesville": ["harrison", "panola", "shelby", "nacogdoches", "rusk", "gregg"],
            
            # Barnett counties
            "barnett": ["tarrant", "denton", "wise", "johnson", "parker", "hood"],
            
            # Other major counties
            "south_texas": ["webb", "zapata", "starr", "hidalgo", "cameron"],
            "east_texas": ["smith", "cherokee", "anderson", "henderson", "van zandt"]
        }
        
        # Reverse mapping
        self.county_to_play = {}
        for play, counties in self.texas_counties.items():
            for county in counties:
                self.county_to_play[county.lower()] = play
    
    def get_play_from_county(self, county: str) -> Optional[str]:
        """Get play/basin from county name"""
        if not county:
            return None
        
        county_clean = county.lower().replace(" county", "").strip()
        return self.county_to_play.get(county_clean)
    
    def normalize_county(self, county_text: str) -> Optional[str]:
        """Extract and normalize county from text"""
        if not county_text:
            return None
        
        # Look for "County" patterns
        county_patterns = [
            r'(\w+)\s+county',
            r'(\w+)\s+co\.',
            r'(\w+)\s+co\b'
        ]
        
        for pattern in county_patterns:
            match = re.search(pattern, county_text.lower())
            if match:
                county = match.group(1)
                if county in self.county_to_play:
                    return county.title()
        
        # Direct lookup
        county_clean = county_text.lower().replace(" county", "").strip()
        if county_clean in self.county_to_play:
            return county_clean.title()
        
        return None

class EnhancedSignalProcessor:
    """Enhanced signal processing with v2.2 features"""
    
    def __init__(self):
        self.operator_map = OperatorAliasMap()
        self.county_dict = CountyPlayDictionary()
        
        # Enhanced keyword categories
        self.activity_keywords = {
            "drilling": ["drill", "drilling", "spud", "spudded", "rig", "wellbore", "horizontal", "vertical"],
            "completion": ["frac", "frack", "completion", "stimulation", "flowback", "initial production"],
            "production": ["production", "oil", "gas", "barrel", "mcf", "boe", "flowing", "shut-in"],
            "permits": ["permit", "application", "approved", "filed", "submitted", "pending"],
            "leasing": ["lease", "leasing", "acreage", "mineral rights", "royalty", "bonus"],
            "infrastructure": ["pipeline", "facility", "plant", "compressor", "gathering", "processing"],
            "financial": ["acquisition", "divestiture", "merger", "ipo", "earnings", "capex", "investment"],
            "regulatory": ["rrc", "epa", "blm", "ferc", "environmental", "compliance", "violation"]
        }
        
        # Source credibility weights
        self.source_weights = {
            SourceType.FILING: 1.0,      # Highest credibility
            SourceType.GOV_BULLETIN: 0.95,
            SourceType.PR: 0.9,
            SourceType.NEWS: 0.8,
            SourceType.BLOG: 0.6,
            SourceType.FORUM: 0.5,
            SourceType.SOCIAL: 0.3,      # Lowest credibility
            SourceType.OTHER: 0.4
        }
    
    def process_crawl_result(self, result: CrawlResult) -> Optional[Signal]:
        """Convert CrawlResult to Signal with enhanced extraction"""
        try:
            if not result.success or not result.content:
                return None
            
            # Extract operators with enhanced matching
            operators = self._extract_operators(result.content)
            
            # Extract location information
            county = self._extract_county(result.content)
            state = self._extract_state(result.content)
            play_basin = self.county_dict.get_play_from_county(county) if county else None
            
            # Extract unit/pad tokens
            unit_tokens = self._extract_unit_tokens(result.content)
            
            # Classify keywords and activity
            keywords = self._classify_keywords(result.content)
            
            # Determine claim type and timeframe
            claim_type = self._determine_claim_type(result.content, result.source_type)
            timeframe = self._determine_timeframe(result.content)
            
            # Generate summary
            summary = self._generate_summary(result, operators, county, keywords)
            
            return Signal(
                org_id="default_org",  # TODO: Make configurable
                found_at=result.post_date or datetime.now(timezone.utc),
                source_url=result.url,
                source_type=result.source_type,
                state=state,
                county=county,
                play_basin=play_basin,
                operators=operators,
                unit_tokens=unit_tokens,
                keywords=keywords,
                claim_type=claim_type,
                timeframe=timeframe,
                summary=summary,
                raw_excerpt=result.content[:1000]  # Limit excerpt size
            )
            
        except Exception as e:
            logger.error(f"Error processing crawl result: {e}")
            return None
    
    def _extract_operators(self, content: str) -> List[str]:
        """Enhanced operator extraction with fuzzy matching"""
        operators = set()
        content_lower = content.lower()
        
        # Look for known operators and their aliases
        for canonical, aliases in self.operator_map.aliases.items():
            for alias in aliases:
                if alias in content_lower:
                    operators.add(canonical)
                    break
        
        # Pattern-based extraction for unknown operators
        operator_patterns = [
            r'(\w+(?:\s+\w+)*)\s+(?:resources|energy|oil|gas|petroleum|corp|inc|llc|company)',
            r'(\w+(?:\s+\w+)*)\s+(?:drilled|completed|filed|announced)',
            r'operator\s+(\w+(?:\s+\w+)*)',
            r'(\w+(?:\s+\w+)*)\s+(?:has|will|plans to|announced)'
        ]
        
        for pattern in operator_patterns:
            matches = re.finditer(pattern, content_lower)
            for match in matches:
                candidate = match.group(1).strip()
                if len(candidate) > 2 and candidate not in ['the', 'and', 'for', 'with']:
                    normalized = self.operator_map.normalize_operator(candidate)
                    if normalized:
                        operators.add(normalized)
        
        return list(operators)
    
    def _extract_county(self, content: str) -> Optional[str]:
        """Enhanced county extraction"""
        return self.county_dict.normalize_county(content)
    
    def _extract_state(self, content: str) -> Optional[str]:
        """Extract state information"""
        # For now, focus on Texas
        if any(indicator in content.lower() for indicator in ['texas', 'tx', 'permian', 'eagle ford']):
            return 'TX'
        return None
    
    def _extract_unit_tokens(self, content: str) -> List[str]:
        """Extract unit, pad, lease, and abstract tokens"""
        tokens = set()
        
        # Pattern for unit/pad/lease names
        unit_patterns = [
            r'(\w+(?:\s+\w+)*)\s+(?:unit|pad|lease)',
            r'(?:unit|pad|lease)\s+(\w+(?:\s+\w+)*)',
            r'(\w+)\s+#\s*\d+[a-z]?h?',  # Well naming patterns
            r'abstract\s+(\d+)',
            r'survey\s+(\w+(?:\s+\w+)*)'
        ]
        
        for pattern in unit_patterns:
            matches = re.finditer(pattern, content.lower())
            for match in matches:
                token = match.group(1).strip()
                if len(token) > 1:
                    tokens.add(token)
        
        return list(tokens)
    
    def _classify_keywords(self, content: str) -> List[str]:
        """Classify content into keyword categories"""
        keywords = []
        content_lower = content.lower()
        
        for category, terms in self.activity_keywords.items():
            if any(term in content_lower for term in terms):
                keywords.append(category)
        
        return keywords
    
    def _determine_claim_type(self, content: str, source_type: SourceType) -> ClaimType:
        """Determine claim type based on content and source"""
        content_lower = content.lower()
        
        # High confidence indicators
        confirmed_indicators = [
            "announced", "completed", "filed", "approved", "commenced", 
            "spudded", "producing", "reported", "confirmed"
        ]
        
        # Medium confidence indicators
        likely_indicators = [
            "plans", "expects", "will", "scheduled", "targeting", "anticipated"
        ]
        
        # Low confidence indicators
        rumor_indicators = [
            "rumor", "speculation", "might", "could", "possibly", "allegedly",
            "sources say", "unconfirmed"
        ]
        
        # Source type influences confidence
        if source_type in [SourceType.FILING, SourceType.GOV_BULLETIN, SourceType.PR]:
            if any(indicator in content_lower for indicator in confirmed_indicators):
                return ClaimType.CONFIRMED
            return ClaimType.LIKELY
        
        elif source_type == SourceType.NEWS:
            if any(indicator in content_lower for indicator in confirmed_indicators):
                return ClaimType.CONFIRMED
            elif any(indicator in content_lower for indicator in likely_indicators):
                return ClaimType.LIKELY
            return ClaimType.RUMOR
        
        else:  # Forum, social, blog
            if any(indicator in content_lower for indicator in rumor_indicators):
                return ClaimType.RUMOR
            elif any(indicator in content_lower for indicator in likely_indicators):
                return ClaimType.LIKELY
            return ClaimType.RUMOR
    
    def _determine_timeframe(self, content: str) -> Optional[Timeframe]:
        """Determine timeframe from content"""
        content_lower = content.lower()
        
        # Past indicators
        past_indicators = [
            "completed", "drilled", "announced", "filed", "spudded",
            "produced", "last", "previous", "ago"
        ]
        
        # Current indicators
        now_indicators = [
            "currently", "now", "today", "this week", "this month",
            "ongoing", "active", "in progress"
        ]
        
        # Future indicators (next 90 days)
        future_indicators = [
            "will", "plans", "scheduled", "expects", "next", "upcoming",
            "q1", "q2", "q3", "q4", "2025", "2026"
        ]
        
        if any(indicator in content_lower for indicator in past_indicators):
            return Timeframe.PAST
        elif any(indicator in content_lower for indicator in now_indicators):
            return Timeframe.NOW
        elif any(indicator in content_lower for indicator in future_indicators):
            return Timeframe.NEXT_90D
        
        return None
    
    def _generate_summary(self, result: CrawlResult, operators: List[str], 
                         county: Optional[str], keywords: List[str]) -> str:
        """Generate concise summary"""
        parts = []
        
        if operators:
            parts.append(f"Operators: {', '.join(operators[:3])}")
        
        if county:
            parts.append(f"Location: {county} County")
        
        if keywords:
            parts.append(f"Activity: {', '.join(keywords[:3])}")
        
        # Add source context
        source_name = result.url.split('/')[2] if result.url else "Unknown"
        parts.append(f"Source: {source_name}")
        
        return " | ".join(parts)
    
    async def save_signals_to_db(self, signals: List[Signal]) -> int:
        """Save signals to database with deduplication"""
        if not signals:
            return 0
        
        saved_count = 0
        
        try:
            with get_session() as session:
                for signal in signals:
                    try:
                        # Check for duplicates
                        existing = session.query(Signal).filter(
                            or_(
                                Signal.source_url == signal.source_url,
                                and_(
                                    Signal.raw_excerpt.like(f'%{signal.raw_excerpt[:50]}%'),
                                    Signal.org_id == signal.org_id
                                )
                            )
                        ).first()
                        
                        if not existing:
                            session.add(signal)
                            session.commit()
                            saved_count += 1
                            logger.info(f"Saved signal from {signal.source_url}")
                        else:
                            logger.debug(f"Duplicate signal skipped: {signal.source_url}")
                    
                    except Exception as e:
                        logger.error(f"Error saving individual signal: {e}")
                        session.rollback()
                        continue
                
        except Exception as e:
            logger.error(f"Database error in save_signals_to_db: {e}")
            return 0
        
        logger.info(f"Saved {saved_count} new signals to database")
        return saved_count

class DeepAnalyticsEngine:
    """Enhanced analytics engine for v2.2"""
    
    def __init__(self, org_id: str = "default_org"):
        self.org_id = org_id
        self.operator_map = OperatorAliasMap()
    
    async def generate_insights_from_signals(self, signals: List[Signal]) -> List[ScoutInsight]:
        """Generate insights with deep analytics"""
        if not signals:
            return []
        
        insights = []
        
        try:
            with get_session() as session:
                # Group signals by similarity for deduplication
                signal_groups = self._group_similar_signals(signals)
                
                for group in signal_groups:
                    insight = await self._create_insight_from_group(group, session)
                    if insight:
                        insights.append(insight)
                
        except Exception as e:
            logger.error(f"Error generating insights: {e}")
            return []
        
        return insights
    
    def _group_similar_signals(self, signals: List[Signal]) -> List[List[Signal]]:
        """Group similar signals for deduplication (72h window)"""
        groups = []
        processed = set()
        
        for i, signal in enumerate(signals):
            if i in processed:
                continue
            
            group = [signal]
            processed.add(i)
            
            # Find similar signals within 72 hours
            for j, other_signal in enumerate(signals[i+1:], i+1):
                if j in processed:
                    continue
                
                if self._are_signals_similar(signal, other_signal):
                    group.append(other_signal)
                    processed.add(j)
            
            groups.append(group)
        
        return groups
    
    def _are_signals_similar(self, signal1: Signal, signal2: Signal) -> bool:
        """Check if two signals are similar (for 72h deduplication)"""
        # Time window check
        time_diff = abs((signal1.found_at - signal2.found_at).total_seconds())
        if time_diff > 72 * 3600:  # 72 hours
            return False
        
        # Location similarity
        if signal1.county and signal2.county and signal1.county != signal2.county:
            return False
        
        # Operator overlap
        operator_overlap = set(signal1.operators) & set(signal2.operators)
        if not operator_overlap and (signal1.operators and signal2.operators):
            return False
        
        # Unit token overlap
        token_overlap = set(signal1.unit_tokens) & set(signal2.unit_tokens)
        if len(token_overlap) > 0:
            return True
        
        # Content similarity
        content_similarity = SequenceMatcher(
            None, 
            signal1.summary.lower(), 
            signal2.summary.lower()
        ).ratio()
        
        return content_similarity > 0.7
    
    async def _create_insight_from_group(self, signal_group: List[Signal], 
                                       session: Session) -> Optional[ScoutInsight]:
        """Create insight from grouped signals with deep analytics"""
        try:
            primary_signal = signal_group[0]  # Use first signal as primary
            
            # Combine all operators and locations
            all_operators = list(set(op for signal in signal_group for op in signal.operators))
            all_counties = list(set(s.county for s in signal_group if s.county))
            all_keywords = list(set(kw for signal in signal_group for kw in signal.keywords))
            
            # Get related permits
            related_permits = await self._find_related_permits(
                all_operators, all_counties, primary_signal.unit_tokens, session
            )
            
            # Calculate deep analytics
            analytics = await self._calculate_deep_analytics(
                all_operators, all_counties, related_permits, session
            )
            
            # Determine confidence based on sources and analytics
            confidence, confidence_reasons = self._calculate_confidence(
                signal_group, analytics
            )
            
            # Generate content
            title = self._generate_insight_title(primary_signal, all_operators, all_counties)
            what_happened = self._generate_what_happened(signal_group, analytics)
            why_it_matters = self._generate_why_it_matters(analytics, related_permits)
            next_checks = self._generate_next_checks(all_operators, all_counties, analytics)
            
            # Collect source URLs with labels
            source_urls = []
            for signal in signal_group:
                domain = signal.source_url.split('/')[2] if signal.source_url else "Unknown"
                source_urls.append({
                    "url": signal.source_url,
                    "label": f"{signal.source_type.value.title()} ({domain})"
                })
            
            # Generate dedup key
            dedup_key = self._generate_dedup_key(all_operators, all_counties, all_keywords)
            
            insight = ScoutInsight(
                org_id=self.org_id,
                title=title,
                what_happened=what_happened,
                why_it_matters=why_it_matters,
                confidence=confidence,
                confidence_reasons=json.dumps(confidence_reasons),
                next_checks=json.dumps(next_checks),
                source_urls=json.dumps(source_urls),
                related_permit_ids=[p.status_no for p in related_permits],
                county=all_counties[0] if all_counties else None,
                state=primary_signal.state,
                operator_keys=all_operators,
                analytics=analytics,
                dedup_key=dedup_key
            )
            
            return insight
            
        except Exception as e:
            logger.error(f"Error creating insight from signal group: {e}")
            return None
    
    async def _find_related_permits(self, operators: List[str], counties: List[str], 
                                  unit_tokens: List[str], session: Session) -> List[Permit]:
        """Find related permits using enhanced matching"""
        try:
            query = session.query(Permit)
            
            # Location filter
            if counties:
                query = query.filter(Permit.county.in_([c.upper() for c in counties]))
            
            # Operator matching (fuzzy)
            if operators:
                operator_conditions = []
                for op in operators:
                    # Try exact match first
                    operator_conditions.append(Permit.operator.ilike(f'%{op}%'))
                    
                    # Try normalized variations
                    normalized = self.operator_map.normalize_operator(op)
                    if normalized != op:
                        operator_conditions.append(Permit.operator.ilike(f'%{normalized}%'))
                
                if operator_conditions:
                    query = query.filter(or_(*operator_conditions))
            
            # Unit token matching
            if unit_tokens:
                token_conditions = []
                for token in unit_tokens:
                    token_conditions.extend([
                        Permit.lease_name.ilike(f'%{token}%'),
                        Permit.field_name.ilike(f'%{token}%'),
                        Permit.well_name.ilike(f'%{token}%')
                    ])
                
                if token_conditions:
                    query = query.filter(or_(*token_conditions))
            
            # Limit to recent permits (last 2 years)
            cutoff_date = datetime.now() - timedelta(days=730)
            query = query.filter(Permit.status_date >= cutoff_date)
            
            return query.limit(10).all()
            
        except Exception as e:
            logger.error(f"Error finding related permits: {e}")
            return []
    
    async def _calculate_deep_analytics(self, operators: List[str], counties: List[str], 
                                      related_permits: List[Permit], session: Session) -> Dict[str, Any]:
        """Calculate deep analytics metrics"""
        analytics = {}
        
        try:
            if not operators or not counties:
                return analytics
            
            primary_operator = operators[0]
            primary_county = counties[0]
            
            # Velocity analysis (7d, 30d)
            velocity_7d = await self._calculate_permit_velocity(
                primary_operator, primary_county, 7, session
            )
            velocity_30d = await self._calculate_permit_velocity(
                primary_operator, primary_county, 30, session
            )
            
            analytics['permit_velocity_7d'] = velocity_7d
            analytics['permit_velocity_30d'] = velocity_30d
            
            # Breakout detection (z-score > 2)
            historical_avg = await self._get_historical_average(
                primary_operator, primary_county, session
            )
            
            if historical_avg > 0:
                z_score = (velocity_30d - historical_avg) / max(historical_avg * 0.5, 1)
                analytics['is_breakout'] = z_score > 2.0
                analytics['breakout_zscore'] = round(z_score, 2)
            
            # New operator detection
            analytics['is_new_operator'] = await self._is_new_operator(
                primary_operator, primary_county, session
            )
            
            # Timing analysis
            if related_permits:
                timing_stats = self._calculate_timing_stats(related_permits)
                analytics.update(timing_stats)
            
            # Agreement score (multi-source corroboration)
            analytics['agreement_score'] = self._calculate_agreement_score(operators, counties)
            
            # Near-term activity prediction
            analytics['near_term_activity'] = await self._predict_near_term_activity(
                primary_operator, primary_county, session
            )
            
        except Exception as e:
            logger.error(f"Error calculating deep analytics: {e}")
        
        return analytics
    
    async def _calculate_permit_velocity(self, operator: str, county: str, 
                                       days: int, session: Session) -> float:
        """Calculate permit velocity for operator in county"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            
            count = session.query(Permit).filter(
                and_(
                    Permit.operator.ilike(f'%{operator}%'),
                    Permit.county.ilike(f'%{county}%'),
                    Permit.status_date >= cutoff_date
                )
            ).count()
            
            return round(count / days, 2)
            
        except Exception as e:
            logger.error(f"Error calculating permit velocity: {e}")
            return 0.0
    
    async def _get_historical_average(self, operator: str, county: str, 
                                    session: Session) -> float:
        """Get historical average permit rate"""
        try:
            # Look at previous 6 months, excluding last 30 days
            end_date = datetime.now() - timedelta(days=30)
            start_date = end_date - timedelta(days=180)
            
            count = session.query(Permit).filter(
                and_(
                    Permit.operator.ilike(f'%{operator}%'),
                    Permit.county.ilike(f'%{county}%'),
                    Permit.status_date >= start_date,
                    Permit.status_date <= end_date
                )
            ).count()
            
            return count / 180  # Daily average
            
        except Exception as e:
            logger.error(f"Error getting historical average: {e}")
            return 0.0
    
    async def _is_new_operator(self, operator: str, county: str, session: Session) -> bool:
        """Check if operator is new to county (first permits in 12 months)"""
        try:
            cutoff_date = datetime.now() - timedelta(days=365)
            
            historical_count = session.query(Permit).filter(
                and_(
                    Permit.operator.ilike(f'%{operator}%'),
                    Permit.county.ilike(f'%{county}%'),
                    Permit.status_date <= cutoff_date
                )
            ).count()
            
            return historical_count == 0
            
        except Exception as e:
            logger.error(f"Error checking new operator: {e}")
            return False
    
    def _calculate_timing_stats(self, permits: List[Permit]) -> Dict[str, Any]:
        """Calculate timing statistics from permits"""
        stats = {}
        
        try:
            # This would require additional permit data (spud dates, completion dates)
            # For now, return placeholder values
            stats['median_days_permit_to_spud'] = 45  # Placeholder
            stats['median_days_spud_to_completion'] = 30  # Placeholder
            
        except Exception as e:
            logger.error(f"Error calculating timing stats: {e}")
        
        return stats
    
    def _calculate_agreement_score(self, operators: List[str], counties: List[str]) -> float:
        """Calculate agreement score based on source diversity"""
        # Simple implementation - could be enhanced
        base_score = 0.5
        
        if len(operators) > 1:
            base_score += 0.2
        if len(counties) > 0:
            base_score += 0.2
        
        return min(base_score, 1.0)
    
    async def _predict_near_term_activity(self, operator: str, county: str, 
                                        session: Session) -> bool:
        """Predict near-term activity based on recent patterns"""
        try:
            # Look at recent permit trends
            recent_permits = session.query(Permit).filter(
                and_(
                    Permit.operator.ilike(f'%{operator}%'),
                    Permit.county.ilike(f'%{county}%'),
                    Permit.status_date >= datetime.now() - timedelta(days=60)
                )
            ).count()
            
            return recent_permits > 2  # Threshold for "active"
            
        except Exception as e:
            logger.error(f"Error predicting near-term activity: {e}")
            return False
    
    def _calculate_confidence(self, signal_group: List[Signal], 
                            analytics: Dict[str, Any]) -> Tuple[ConfidenceLevel, List[str]]:
        """Calculate confidence level and reasons"""
        reasons = []
        base_score = 0.0
        
        # Source type weighting
        source_weights = [self._get_source_weight(s.source_type) for s in signal_group]
        avg_source_weight = sum(source_weights) / len(source_weights)
        base_score += avg_source_weight * 0.4
        
        # Multi-source bonus
        unique_sources = len(set(s.source_type for s in signal_group))
        if unique_sources > 1:
            base_score += 0.2
            reasons.append(f"Multiple sources ({unique_sources})")
        
        # Analytics boost
        if analytics.get('is_breakout'):
            base_score += 0.2
            reasons.append("Breakout activity detected")
        
        if analytics.get('is_new_operator'):
            base_score += 0.1
            reasons.append("New operator in area")
        
        # Claim type influence
        confirmed_signals = [s for s in signal_group if s.claim_type == ClaimType.CONFIRMED]
        if confirmed_signals:
            base_score += 0.2
            reasons.append("Confirmed information")
        
        # Determine final confidence
        if base_score >= 0.8:
            return ConfidenceLevel.HIGH, reasons
        elif base_score >= 0.6:
            return ConfidenceLevel.MEDIUM, reasons
        else:
            return ConfidenceLevel.LOW, reasons
    
    def _get_source_weight(self, source_type: SourceType) -> float:
        """Get credibility weight for source type"""
        weights = {
            SourceType.FILING: 1.0,
            SourceType.GOV_BULLETIN: 0.95,
            SourceType.PR: 0.9,
            SourceType.NEWS: 0.8,
            SourceType.BLOG: 0.6,
            SourceType.FORUM: 0.5,
            SourceType.SOCIAL: 0.3,
            SourceType.OTHER: 0.4
        }
        return weights.get(source_type, 0.4)
    
    def _generate_insight_title(self, signal: Signal, operators: List[str], 
                              counties: List[str]) -> str:
        """Generate insight title (≤90 chars)"""
        operator_str = operators[0] if operators else "Unknown Operator"
        county_str = f" in {counties[0]} County" if counties else ""
        activity = signal.keywords[0] if signal.keywords else "activity"
        
        title = f"{operator_str} {activity}{county_str}"
        return title[:90]
    
    def _generate_what_happened(self, signal_group: List[Signal], 
                              analytics: Dict[str, Any]) -> str:
        """Generate what happened section (markdown)"""
        facts = []
        
        primary_signal = signal_group[0]
        
        # Primary fact
        if primary_signal.operators and primary_signal.county:
            facts.append(f"- {primary_signal.operators[0]} activity detected in {primary_signal.county} County")
        
        # Activity details
        if primary_signal.keywords:
            activity_str = ", ".join(primary_signal.keywords[:3])
            facts.append(f"- Activity type: {activity_str}")
        
        # Analytics facts
        if analytics.get('permit_velocity_30d', 0) > 0:
            velocity = analytics['permit_velocity_30d']
            facts.append(f"- Recent permit velocity: {velocity} permits/day (30-day average)")
        
        if analytics.get('is_breakout'):
            facts.append(f"- Breakout activity detected (z-score: {analytics.get('breakout_zscore', 'N/A')})")
        
        return "\n".join(facts[:4])  # Limit to 4 facts
    
    def _generate_why_it_matters(self, analytics: Dict[str, Any], 
                               related_permits: List[Permit]) -> str:
        """Generate why it matters section (markdown)"""
        impacts = []
        
        if analytics.get('is_new_operator'):
            impacts.append("- New operator entry could signal increased competition and activity")
        
        if analytics.get('is_breakout'):
            impacts.append("- Breakout activity suggests significant resource development")
        
        if len(related_permits) > 5:
            impacts.append(f"- High permit density ({len(related_permits)} related permits) indicates active development")
        
        if analytics.get('near_term_activity'):
            impacts.append("- Near-term drilling activity likely based on recent patterns")
        
        if not impacts:
            impacts.append("- Indicates ongoing development activity in the area")
        
        return "\n".join(impacts[:2])  # Limit to 2 impacts
    
    def _generate_next_checks(self, operators: List[str], counties: List[str], 
                            analytics: Dict[str, Any]) -> List[str]:
        """Generate next checks list"""
        checks = []
        
        if operators and counties:
            checks.append(f"Monitor {operators[0]} permit filings in {counties[0]} County")
        
        if analytics.get('is_breakout'):
            checks.append("Track completion and production data for breakout confirmation")
        
        checks.append("Check for additional operator announcements or SEC filings")
        
        return checks[:3]  # Limit to 3 checks
    
    def _generate_dedup_key(self, operators: List[str], counties: List[str], 
                          keywords: List[str]) -> str:
        """Generate deduplication key for 72h window"""
        key_parts = []
        
        if operators:
            key_parts.append(f"op:{operators[0]}")
        if counties:
            key_parts.append(f"co:{counties[0]}")
        if keywords:
            key_parts.append(f"kw:{':'.join(sorted(keywords[:2]))}")
        
        key_string = "|".join(key_parts)
        return hashlib.md5(key_string.encode()).hexdigest()[:16]
