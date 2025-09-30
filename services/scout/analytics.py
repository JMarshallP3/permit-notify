"""
Scout v2.1 Deep Analytics Engine
Compute velocity, breakout detection, permit matching, and trend analysis
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timezone, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc
import statistics
import re
from collections import defaultdict, Counter

from db.session import get_session
from db.models import Permit
from db.scout_models import Signal, ScoutInsight, ConfidenceLevel
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
        """Calculate permit filing velocity for county/operator"""
        
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=days)
        prior_start = start_date - timedelta(days=days)
        
        # Current period query
        current_query = self.session.query(func.count(Permit.id)).filter(
            and_(
                Permit.status_date >= start_date,
                Permit.status_date <= end_date,
                Permit.county.ilike(f"%{county}%")
            )
        )
        
        # Prior period query
        prior_query = self.session.query(func.count(Permit.id)).filter(
            and_(
                Permit.status_date >= prior_start,
                Permit.status_date < start_date,
                Permit.county.ilike(f"%{county}%")
            )
        )
        
        # Add operator filter if specified
        if operator:
            current_query = current_query.filter(Permit.operator_name.ilike(f"%{operator}%"))
            prior_query = prior_query.filter(Permit.operator_name.ilike(f"%{operator}%"))
        
        current_count = current_query.scalar() or 0
        prior_count = prior_query.scalar() or 0
        
        # Calculate velocity metrics
        velocity_per_day = current_count / days if days > 0 else 0
        
        # Calculate change vs prior period
        if prior_count > 0:
            delta_vs_prior = (current_count - prior_count) / prior_count
            velocity_multiplier = current_count / prior_count if prior_count > 0 else float('inf')
        else:
            delta_vs_prior = 1.0 if current_count > 0 else 0.0
            velocity_multiplier = float('inf') if current_count > 0 else 0.0
        
        return {
            'current_count': current_count,
            'prior_count': prior_count,
            'velocity_per_day': velocity_per_day,
            'delta_vs_prior': delta_vs_prior,
            'velocity_multiplier': velocity_multiplier
        }
    
    def detect_breakout_activity(self, county: str, operator: str = None, z_threshold: float = 2.0) -> Dict[str, Any]:
        """Detect if current activity is a statistical breakout (z-score > threshold)"""
        
        # Get historical data for the last 12 months in 30-day windows
        end_date = datetime.now(timezone.utc).date()
        historical_counts = []
        
        for i in range(12):  # 12 months of history
            window_end = end_date - timedelta(days=i*30)
            window_start = window_end - timedelta(days=30)
            
            query = self.session.query(func.count(Permit.id)).filter(
                and_(
                    Permit.status_date >= window_start,
                    Permit.status_date < window_end,
                    Permit.county.ilike(f"%{county}%")
                )
            )
            
            if operator:
                query = query.filter(Permit.operator_name.ilike(f"%{operator}%"))
            
            count = query.scalar() or 0
            historical_counts.append(count)
        
        if len(historical_counts) < 3:
            return {'is_breakout': False, 'z_score': 0, 'reason': 'Insufficient historical data'}
        
        # Calculate z-score for current period (first in list)
        current_count = historical_counts[0]
        historical_mean = statistics.mean(historical_counts[1:])  # Exclude current period
        historical_std = statistics.stdev(historical_counts[1:]) if len(historical_counts) > 2 else 1
        
        if historical_std == 0:
            z_score = 0
        else:
            z_score = (current_count - historical_mean) / historical_std
        
        is_breakout = z_score > z_threshold
        
        return {
            'is_breakout': is_breakout,
            'z_score': round(z_score, 2),
            'current_count': current_count,
            'historical_mean': round(historical_mean, 1),
            'historical_std': round(historical_std, 1),
            'reason': f'Z-score {z_score:.1f} {">" if is_breakout else "<="} {z_threshold}'
        }
    
    def check_new_operator(self, county: str, operator: str, lookback_months: int = 12) -> bool:
        """Check if operator is new to this county in the lookback period"""
        
        cutoff_date = datetime.now(timezone.utc).date() - timedelta(days=lookback_months*30)
        
        historical_count = self.session.query(func.count(Permit.id)).filter(
            and_(
                Permit.status_date >= cutoff_date,
                Permit.county.ilike(f"%{county}%"),
                Permit.operator_name.ilike(f"%{operator}%")
            )
        ).scalar() or 0
        
        return historical_count == 0
    
    def analyze_spacing_patterns(self, county: str, operator: str = None) -> Dict[str, Any]:
        """Analyze pad/unit clustering and spacing patterns"""
        
        # Get recent permits with location data
        query = self.session.query(Permit).filter(
            and_(
                Permit.county.ilike(f"%{county}%"),
                Permit.status_date >= datetime.now(timezone.utc).date() - timedelta(days=90),
                Permit.lease_name.isnot(None)
            )
        )
        
        if operator:
            query = query.filter(Permit.operator_name.ilike(f"%{operator}%"))
        
        permits = query.all()
        
        # Group by lease/unit patterns
        lease_groups = defaultdict(list)
        for permit in permits:
            # Extract base lease name (remove well numbers)
            base_lease = re.sub(r'\s+#?\d+[A-Z]?$', '', permit.lease_name or '')
            lease_groups[base_lease].append(permit)
        
        # Analyze clustering
        multi_well_units = {k: v for k, v in lease_groups.items() if len(v) > 1}
        avg_wells_per_unit = statistics.mean([len(v) for v in multi_well_units.values()]) if multi_well_units else 1
        
        return {
            'total_permits': len(permits),
            'unique_units': len(lease_groups),
            'multi_well_units': len(multi_well_units),
            'avg_wells_per_unit': round(avg_wells_per_unit, 1),
            'clustering_score': len(multi_well_units) / len(lease_groups) if lease_groups else 0
        }
    
    def estimate_timing_metrics(self, county: str, operator: str = None) -> Dict[str, Any]:
        """Estimate permit-to-spud and spud-to-completion timing"""
        
        # This is a simplified version - in reality, you'd need completion data
        # and more sophisticated matching between permits and completions
        
        # Get permits from last 12 months
        query = self.session.query(Permit).filter(
            and_(
                Permit.county.ilike(f"%{county}%"),
                Permit.status_date >= datetime.now(timezone.utc).date() - timedelta(days=365)
            )
        )
        
        if operator:
            query = query.filter(Permit.operator_name.ilike(f"%{operator}%"))
        
        permits = query.all()
        
        # Estimate based on historical patterns (placeholder logic)
        # In reality, this would match permits to completion records
        
        # Rough industry averages for Texas unconventional
        estimated_permit_to_spud = 45  # days
        estimated_spud_to_completion = 30  # days
        
        # Adjust based on permit velocity (higher velocity = faster execution)
        velocity_data = self.calculate_permit_velocity(county, operator, 30)
        velocity_factor = min(2.0, max(0.5, velocity_data['velocity_multiplier']))
        
        adjusted_permit_to_spud = int(estimated_permit_to_spud / velocity_factor)
        
        return {
            'median_days_permit_to_spud': adjusted_permit_to_spud,
            'median_days_spud_to_completion': estimated_spud_to_completion,
            'total_cycle_days': adjusted_permit_to_spud + estimated_spud_to_completion,
            'confidence': 'estimated'  # Would be 'measured' with actual completion data
        }
    
    def assess_near_term_activity(self, county: str, operator: str = None) -> Dict[str, Any]:
        """Assess likelihood of near-term drilling activity"""
        
        # Get recent permits (last 90 days)
        recent_permits = self.session.query(func.count(Permit.id)).filter(
            and_(
                Permit.county.ilike(f"%{county}%"),
                Permit.status_date >= datetime.now(timezone.utc).date() - timedelta(days=90)
            )
        )
        
        if operator:
            recent_permits = recent_permits.filter(Permit.operator_name.ilike(f"%{operator}%"))
        
        recent_count = recent_permits.scalar() or 0
        
        # Estimate timing
        timing_data = self.estimate_timing_metrics(county, operator)
        avg_permit_to_spud = timing_data['median_days_permit_to_spud']
        
        # Permits likely to spud in next 30/60/90 days
        permits_30d = self.session.query(func.count(Permit.id)).filter(
            and_(
                Permit.county.ilike(f"%{county}%"),
                Permit.status_date >= datetime.now(timezone.utc).date() - timedelta(days=avg_permit_to_spud-30),
                Permit.status_date <= datetime.now(timezone.utc).date() - timedelta(days=avg_permit_to_spud-60)
            )
        )
        
        if operator:
            permits_30d = permits_30d.filter(Permit.operator_name.ilike(f"%{operator}%"))
        
        near_term_30d = permits_30d.scalar() or 0
        
        return {
            'recent_permits_90d': recent_count,
            'estimated_spuds_30d': near_term_30d,
            'near_term_activity': near_term_30d > 0,
            'activity_score': min(1.0, near_term_30d / 5.0)  # Normalize to 0-1
        }

class SignalMatcher:
    """Match signals against permits and generate insights"""
    
    def __init__(self):
        self.analytics = PermitAnalytics()
    
    def fuzzy_match_operator(self, signal_operator: str, permit_operator: str, threshold: float = 0.8) -> bool:
        """Fuzzy match operator names"""
        
        if not signal_operator or not permit_operator:
            return False
        
        # Simple substring matching for now
        signal_clean = signal_operator.lower().strip()
        permit_clean = permit_operator.lower().strip()
        
        # Direct substring match
        if signal_clean in permit_clean or permit_clean in signal_clean:
            return True
        
        # Token overlap
        signal_tokens = set(signal_clean.split())
        permit_tokens = set(permit_clean.split())
        
        if signal_tokens and permit_tokens:
            overlap = len(signal_tokens & permit_tokens) / len(signal_tokens | permit_tokens)
            return overlap >= threshold
        
        return False
    
    def match_signals_to_permits(self, signals: List[Signal], org_id: str) -> List[Dict[str, Any]]:
        """Match signals against permits and generate insights"""
        
        insights = []
        
        with self.analytics:
            for signal in signals:
                # Find matching permits
                matching_permits = self.find_matching_permits(signal, org_id)
                
                if matching_permits:
                    insight = self.generate_insight_from_signal(signal, matching_permits)
                    if insight:
                        insights.append(insight)
        
        return insights
    
    def find_matching_permits(self, signal: Signal, org_id: str) -> List[Permit]:
        """Find permits that match the signal"""
        
        query = self.analytics.session.query(Permit).filter(
            Permit.org_id == org_id
        )
        
        # County filter
        if signal.county:
            query = query.filter(Permit.county.ilike(f"%{signal.county}%"))
        
        # State filter
        if signal.state:
            query = query.filter(Permit.state == signal.state)
        
        # Recent permits only (last 6 months)
        cutoff_date = datetime.now(timezone.utc).date() - timedelta(days=180)
        query = query.filter(Permit.status_date >= cutoff_date)
        
        permits = query.all()
        
        # Filter by operator matching
        matching_permits = []
        for permit in permits:
            for signal_operator in signal.operators:
                if self.fuzzy_match_operator(signal_operator, permit.operator_name):
                    matching_permits.append(permit)
                    break
        
        return matching_permits
    
    def generate_insight_from_signal(self, signal: Signal, matching_permits: List[Permit]) -> Optional[Dict[str, Any]]:
        """Generate a Scout insight from signal and matching permits"""
        
        if not matching_permits:
            return None
        
        # Group permits by operator and county
        permit_groups = defaultdict(list)
        for permit in matching_permits:
            key = (permit.operator_name, permit.county)
            permit_groups[key].append(permit)
        
        # Generate insight for the largest group
        main_group = max(permit_groups.values(), key=len)
        main_operator = main_group[0].operator_name
        main_county = main_group[0].county
        
        # Calculate analytics
        velocity_data = self.analytics.calculate_permit_velocity(main_county, main_operator, 30)
        breakout_data = self.analytics.detect_breakout_activity(main_county, main_operator)
        timing_data = self.analytics.estimate_timing_metrics(main_county, main_operator)
        activity_data = self.analytics.assess_near_term_activity(main_county, main_operator)
        spacing_data = self.analytics.analyze_spacing_patterns(main_county, main_operator)
        is_new_operator = self.analytics.check_new_operator(main_county, main_operator)
        
        # Determine confidence level
        confidence = self.calculate_confidence(signal, main_group, velocity_data, breakout_data)
        
        # Generate insight content
        what_happened = [
            f"{main_operator} activity detected in {main_county} County",
            f"{len(main_group)} related permits found in last 6 months"
        ]
        
        if velocity_data['current_count'] > 0:
            what_happened.append(f"Current 30-day velocity: {velocity_data['current_count']} permits")
        
        why_it_matters = []
        if breakout_data['is_breakout']:
            why_it_matters.append(f"Statistical breakout detected (z-score: {breakout_data['z_score']})")
        
        if is_new_operator:
            why_it_matters.append(f"New operator entry into {main_county} County")
        
        if activity_data['near_term_activity']:
            why_it_matters.append("Near-term drilling activity expected")
        
        if not why_it_matters:
            why_it_matters.append("Continued operator activity in established area")
        
        # Confidence reasons
        confidence_reasons = []
        if signal.claim_type.value == 'confirmed':
            confidence_reasons.append("Confirmed source")
        if len(main_group) > 5:
            confidence_reasons.append("Strong permit correlation")
        if velocity_data['delta_vs_prior'] > 0.5:
            confidence_reasons.append("Increasing activity trend")
        
        # Next checks
        next_checks = [
            f"Monitor {main_operator} permit filings in {main_county}",
            "Track rig activity and spud dates"
        ]
        
        if spacing_data['clustering_score'] > 0.3:
            next_checks.append("Analyze pad development patterns")
        
        # Analytics object
        analytics = {
            'permit_velocity_7d': f"{velocity_data['velocity_per_day']*7:.1f}",
            'permit_velocity_30d': f"{velocity_data['current_count']}",
            'delta_vs_prior': velocity_data['delta_vs_prior'],
            'is_breakout': breakout_data['is_breakout'],
            'z_score': breakout_data['z_score'],
            'new_operator': is_new_operator,
            'near_term_activity': activity_data['near_term_activity'],
            'median_lag_permit_to_spud_days': timing_data['median_days_permit_to_spud'],
            'clustering_score': spacing_data['clustering_score'],
            'agreement_score': 0.8  # Placeholder
        }
        
        # Create dedup key for 72h deduplication
        dedup_key = f"{main_county}_{main_operator}_{signal.source_type}_{datetime.now(timezone.utc).date()}"
        
        return {
            'title': f"{main_operator} activity surge in {main_county} County",
            'what_happened': json.dumps(what_happened),
            'why_it_matters': json.dumps(why_it_matters),
            'confidence': confidence,
            'confidence_reasons': json.dumps(confidence_reasons),
            'next_checks': json.dumps(next_checks),
            'source_urls': json.dumps([{'url': signal.source_url, 'label': signal.source_type.upper()}]),
            'related_permit_ids': [p.status_no for p in main_group],
            'county': main_county,
            'state': signal.state,
            'operator_keys': [main_operator.upper()],
            'analytics': analytics,
            'dedup_key': dedup_key
        }
    
    def calculate_confidence(self, signal: Signal, permits: List[Permit], velocity_data: Dict, breakout_data: Dict) -> ConfidenceLevel:
        """Calculate confidence level for the insight"""
        
        score = 0
        
        # Source type confidence
        if signal.claim_type.value == 'confirmed':
            score += 3
        elif signal.claim_type.value == 'likely':
            score += 2
        else:
            score += 1
        
        # Permit correlation strength
        if len(permits) > 10:
            score += 3
        elif len(permits) > 5:
            score += 2
        elif len(permits) > 0:
            score += 1
        
        # Activity trend
        if velocity_data['delta_vs_prior'] > 1.0:
            score += 2
        elif velocity_data['delta_vs_prior'] > 0.5:
            score += 1
        
        # Breakout activity
        if breakout_data['is_breakout']:
            score += 2
        
        # Convert score to confidence level
        if score >= 7:
            return ConfidenceLevel.HIGH
        elif score >= 4:
            return ConfidenceLevel.MEDIUM
        else:
            return ConfidenceLevel.LOW
