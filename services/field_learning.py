#!/usr/bin/env python3
"""Intelligent field name learning system."""

import re
from typing import List, Dict, Optional, Tuple
from db.session import get_session
from db.field_corrections import FieldCorrection
from db.models import Permit
import logging

logger = logging.getLogger(__name__)

class FieldLearningSystem:
    """Learn from user corrections to improve field name extraction."""
    
    def __init__(self):
        self.learned_patterns = {}
        self.load_learned_patterns()
    
    def load_learned_patterns(self):
        """Load previously learned patterns from database."""
        try:
            with get_session() as session:
                corrections = session.query(FieldCorrection).all()
                
                for correction in corrections:
                    # Build pattern recognition from corrections
                    wrong = correction.wrong_field_name.lower()
                    correct = correction.correct_field_name.lower()
                    
                    # Store patterns for future use
                    if wrong not in self.learned_patterns:
                        self.learned_patterns[wrong] = []
                    
                    self.learned_patterns[wrong].append({
                        'correct': correction.correct_field_name,
                        'lease_name': correction.lease_name,
                        'operator': correction.operator_name,
                        'context': correction.html_context
                    })
                    
                logger.info(f"Loaded {len(corrections)} field name corrections")
                
        except Exception as e:
            # Initialize empty patterns if database table doesn't exist yet
            logger.warning(f"Field corrections table not available yet: {e}")
            self.learned_patterns = {}
    
    def record_correction(self, permit_id: int, status_no: str, wrong_field: str, 
                         correct_field: str, detail_url: str = None, 
                         html_context: str = None) -> bool:
        """Record a user correction for learning."""
        try:
            with get_session() as session:
                # Get permit details
                permit = session.query(Permit).filter(Permit.id == permit_id).first()
                
                if not permit:
                    logger.error(f"Permit {permit_id} not found for correction")
                    return False
                
                try:
                    # Try to create correction record (will fail if table doesn't exist)
                    correction = FieldCorrection(
                        permit_id=permit_id,
                        status_no=status_no,
                        lease_name=permit.lease_name,
                        operator_name=permit.operator_name,
                        wrong_field_name=wrong_field,
                        correct_field_name=correct_field,
                        detail_url=detail_url,
                        html_context=html_context
                    )
                    
                    session.add(correction)
                    session.commit()
                    logger.info(f"📝 Saved correction to database")
                    
                except Exception as db_error:
                    logger.warning(f"Could not save to field_corrections table: {db_error}")
                    # Continue anyway - we can still update the permit
                
                # Update the permit with correct field name (this should always work)
                permit.field_name = correct_field
                session.commit()
                
                # Update learned patterns in memory
                wrong_lower = wrong_field.lower()
                if wrong_lower not in self.learned_patterns:
                    self.learned_patterns[wrong_lower] = []
                
                self.learned_patterns[wrong_lower].append({
                    'correct': correct_field,
                    'lease_name': permit.lease_name,
                    'operator': permit.operator_name,
                    'context': html_context
                })
                
                logger.info(f"✅ Recorded correction: '{wrong_field}' → '{correct_field}' for permit {status_no}")
                return True
                
        except Exception as e:
            logger.error(f"Error recording correction: {e}")
            return False
    
    def suggest_field_name(self, extracted_field: str, lease_name: str = None, 
                          operator_name: str = None) -> Optional[str]:
        """Suggest correct field name based on learned patterns."""
        
        if not extracted_field:
            return None
            
        extracted_lower = extracted_field.lower()
        
        # Check for exact match in learned patterns
        if extracted_lower in self.learned_patterns:
            patterns = self.learned_patterns[extracted_lower]
            
            # If we have lease/operator context, try to find best match
            if lease_name or operator_name:
                for pattern in patterns:
                    if (lease_name and pattern.get('lease_name') == lease_name) or \
                       (operator_name and pattern.get('operator') == operator_name):
                        return pattern['correct']
            
            # Return most recent correction if no context match
            if patterns:
                return patterns[-1]['correct']
        
        # Check for partial matches (lease names being used as field names)
        if lease_name and extracted_field.upper() == lease_name.upper():
            # This is likely a lease name being extracted as field name
            similar_corrections = self._find_similar_corrections(lease_name, operator_name)
            if similar_corrections:
                return similar_corrections[0]['correct']
        
        return None
    
    def _find_similar_corrections(self, lease_name: str, operator_name: str) -> List[Dict]:
        """Find corrections for similar leases/operators."""
        similar = []
        
        for patterns in self.learned_patterns.values():
            for pattern in patterns:
                similarity_score = 0
                
                # Check lease name similarity
                if lease_name and pattern.get('lease_name'):
                    if lease_name.upper() in pattern['lease_name'].upper() or \
                       pattern['lease_name'].upper() in lease_name.upper():
                        similarity_score += 2
                
                # Check operator similarity
                if operator_name and pattern.get('operator'):
                    if operator_name.upper() in pattern['operator'].upper() or \
                       pattern['operator'].upper() in operator_name.upper():
                        similarity_score += 1
                
                if similarity_score > 0:
                    similar.append({**pattern, 'similarity': similarity_score})
        
        # Sort by similarity score
        similar.sort(key=lambda x: x['similarity'], reverse=True)
        return similar
    
    def get_correction_stats(self) -> Dict:
        """Get statistics about corrections made."""
        try:
            with get_session() as session:
                total_corrections = session.query(FieldCorrection).count()
                
                # Get most common wrong extractions
                wrong_fields = session.query(FieldCorrection.wrong_field_name).all()
                wrong_counts = {}
                for (wrong_field,) in wrong_fields:
                    wrong_counts[wrong_field] = wrong_counts.get(wrong_field, 0) + 1
                
                # Sort by frequency
                most_common_errors = sorted(wrong_counts.items(), 
                                          key=lambda x: x[1], reverse=True)[:10]
                
                return {
                    'total_corrections': total_corrections,
                    'learned_patterns': len(self.learned_patterns),
                    'most_common_errors': most_common_errors
                }
                
        except Exception as e:
            logger.error(f"Error getting correction stats: {e}")
            return {'error': str(e)}
    
    def apply_learned_corrections(self, limit: int = 50) -> Dict:
        """Apply learned corrections to permits with similar wrong field names."""
        try:
            corrected = 0
            skipped = 0
            
            with get_session() as session:
                # Find permits that might benefit from learned corrections
                for wrong_pattern, corrections in self.learned_patterns.items():
                    if corrected >= limit:
                        break
                    
                    # Find permits with this wrong field name
                    permits = session.query(Permit).filter(
                        Permit.field_name.ilike(f"%{wrong_pattern}%")
                    ).limit(10).all()
                    
                    for permit in permits:
                        if corrected >= limit:
                            break
                            
                        # Get best correction suggestion
                        suggestion = self.suggest_field_name(
                            permit.field_name, 
                            permit.lease_name, 
                            permit.operator_name
                        )
                        
                        if suggestion and suggestion != permit.field_name:
                            # Apply the correction
                            old_field = permit.field_name
                            permit.field_name = suggestion
                            session.commit()
                            
                            logger.info(f"🤖 Auto-corrected permit {permit.status_no}: '{old_field}' → '{suggestion}'")
                            corrected += 1
                        else:
                            skipped += 1
            
            return {
                'corrected': corrected,
                'skipped': skipped,
                'message': f"Applied learned corrections to {corrected} permits"
            }
            
        except Exception as e:
            logger.error(f"Error applying learned corrections: {e}")
            return {'error': str(e)}

# Global instance
field_learning = FieldLearningSystem()
