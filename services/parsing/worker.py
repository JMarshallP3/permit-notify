"""
Enhanced Parsing Worker for PermitTracker
Integrates with the queue system to provide robust, retry-capable parsing.
"""

import logging
import asyncio
from typing import Dict, Any, Optional, Tuple
from datetime import datetime

from .queue import parsing_queue, ParseStatus, ParseStrategy
from ..enrichment.worker import EnrichmentWorker
from ..enrichment.detail_parser import parse_detail_page
from ..enrichment.pdf_parse import extract_text_from_pdf
from db.repo import get_session
from db.models import Permit

logger = logging.getLogger(__name__)

class EnhancedParsingWorker:
    """
    Enhanced parsing worker with queue integration and retry capabilities.
    """
    
    def __init__(self):
        self.enrichment_worker = EnrichmentWorker()
    
    async def process_permit(self, permit_id: str, status_no: str, strategy: ParseStrategy = ParseStrategy.STANDARD) -> Tuple[bool, Dict[str, Any], float]:
        """
        Process a single permit with enhanced parsing capabilities.
        
        Returns:
            (success, parsed_data, confidence_score)
        """
        try:
            # Mark job as in progress
            parsing_queue.update_job(permit_id, ParseStatus.IN_PROGRESS)
            
            # Get permit data from database (extract needed fields to avoid session issues)
            permit_data = None
            with get_session() as session:
                permit = session.query(Permit).filter(Permit.status_no == status_no).first()
                if not permit:
                    logger.error(f"Permit {status_no} not found in database")
                    return False, {}, 0.0
                
                # Extract the data we need to avoid detached instance issues
                permit_data = {
                    'id': permit.id,
                    'status_no': permit.status_no,
                    'lease_name': permit.lease_name,
                    'detail_url': permit.detail_url,
                    'operator_name': permit.operator_name,
                    'county': permit.county,
                    'status_date': permit.status_date
                }
            
            # Choose parsing strategy
            if strategy == ParseStrategy.RETRY_FRESH_SESSION:
                success, data, confidence = await self._parse_with_fresh_session(permit_data)
            elif strategy == ParseStrategy.ALTERNATIVE_PDF:
                success, data, confidence = await self._parse_with_alternative_methods(permit_data)
            else:  # Standard strategy
                success, data, confidence = await self._parse_standard(permit_data)
            
            # Update database if successful
            if success and data:
                await self._update_permit_in_database(permit_id, data)
            
            return success, data, confidence
            
        except Exception as e:
            logger.error(f"Error processing permit {permit_id}: {e}")
            return False, {}, 0.0
    
    async def _parse_standard(self, permit_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], float]:
        """Standard parsing approach."""
        try:
            if not permit_data.get('detail_url'):
                return False, {}, 0.0
            
            # Fetch and parse detail page
            import requests
            response = requests.get(permit_data['detail_url'], timeout=30)
            if response.status_code != 200:
                return False, {}, 0.0
            
            detail_data = parse_detail_page(response.text, permit_data['detail_url'])
            if not detail_data:
                return False, {}, 0.0
            
            # Extract PDF data if available
            pdf_data = {}
            confidence = 0.5  # Base confidence for detail page only
            
            if detail_data.get('pdf_url'):
                try:
                    pdf_text = extract_text_from_pdf(detail_data['pdf_url'])
                    if pdf_text:
                        pdf_data = self._parse_pdf_text(pdf_text)
                        confidence = 0.8  # Higher confidence with PDF data
                except Exception as e:
                    logger.warning(f"PDF parsing failed for permit {permit_data.get('status_no', 'unknown')}: {e}")
            
            # Combine data
            combined_data = {**detail_data, **pdf_data}
            
            # Calculate confidence based on completeness
            confidence = self._calculate_confidence(combined_data)
            
            return True, combined_data, confidence
            
        except Exception as e:
            logger.error(f"Standard parsing failed for permit {permit_data.get('status_no', 'unknown')}: {e}")
            return False, {}, 0.0
    
    async def _parse_with_fresh_session(self, permit_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], float]:
        """Parse with fresh session to handle dynamic URLs."""
        try:
            # Create fresh session for this permit
            import requests
            session = requests.Session()
            
            # First, visit the detail page to establish session
            if not permit_data.get('detail_url'):
                return False, {}, 0.0
            
            # Parse detail page with fresh session
            response = session.get(permit_data['detail_url'], timeout=30)
            if response.status_code != 200:
                return False, {}, 0.0
            
            detail_data = parse_detail_page(response.text, permit_data['detail_url'])
            if not detail_data:
                return False, {}, 0.0
            
            # Get fresh PDF URL and parse
            pdf_data = {}
            confidence = 0.5
            
            if detail_data.get('pdf_url'):
                try:
                    pdf_response = session.get(detail_data['pdf_url'], timeout=30)
                    if pdf_response.status_code == 200:
                        pdf_text = extract_text_from_pdf(pdf_response.content)
                        if pdf_text:
                            pdf_data = self._parse_pdf_text(pdf_text)
                            confidence = 0.9  # High confidence for fresh session + PDF
                except Exception as e:
                    logger.warning(f"Fresh session PDF parsing failed for permit {permit_data.get('status_no', 'unknown')}: {e}")
            
            combined_data = {**detail_data, **pdf_data}
            confidence = self._calculate_confidence(combined_data)
            
            return True, combined_data, confidence
            
        except Exception as e:
            logger.error(f"Fresh session parsing failed for permit {permit_data.get('status_no', 'unknown')}: {e}")
            return False, {}, 0.0
    
    async def _parse_with_alternative_methods(self, permit_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], float]:
        """Try alternative parsing methods for difficult permits."""
        try:
            # Method 1: Try different user agents
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            import requests
            session = requests.Session()
            session.headers.update(headers)
            
            # Try parsing with different approach
            response = session.get(permit_data['detail_url'], timeout=30)
            if response.status_code != 200:
                return False, {}, 0.0
            
            detail_data = parse_detail_page(response.text, permit_data['detail_url'])
            if detail_data:
                confidence = self._calculate_confidence(detail_data)
                return True, detail_data, confidence
            
            # Method 2: Try direct PDF access if we have cached URL
            # (Implementation would go here for alternative PDF access methods)
            
            return False, {}, 0.0
            
        except Exception as e:
            logger.error(f"Alternative parsing failed for permit {permit_data.get('status_no', 'unknown')}: {e}")
            return False, {}, 0.0
    
    def _parse_pdf_text(self, pdf_text: str) -> Dict[str, Any]:
        """Parse structured data from PDF text."""
        from ..enrichment.pdf_parse import parse_pdf_fields
        
        try:
            return parse_pdf_fields(pdf_text)
        except Exception as e:
            logger.error(f"PDF text parsing failed: {e}")
            return {}
    
    def _calculate_confidence(self, data: Dict[str, Any]) -> float:
        """Calculate confidence score based on data completeness and quality."""
        if not data:
            return 0.0
        
        # Key fields that indicate good parsing
        key_fields = ['section', 'block', 'survey', 'abstract_no', 'acres', 'field_name', 'reservoir_well_count']
        
        filled_fields = sum(1 for field in key_fields if data.get(field) is not None and data.get(field) != '')
        base_confidence = filled_fields / len(key_fields)
        
        # Bonus points for having PDF data
        if any(data.get(field) for field in ['section', 'block', 'survey']):
            base_confidence += 0.2
        
        # Penalty for obvious parsing errors (like "47" for everything)
        if data.get('section') == '47' and data.get('block') == '47':
            base_confidence *= 0.5
        
        return min(1.0, base_confidence)
    
    async def _update_permit_in_database(self, permit_id: str, data: Dict[str, Any]):
        """Update permit in database with parsed data."""
        try:
            with get_session() as session:
                permit = session.query(Permit).filter(Permit.status_no == permit_id).first()
                if permit:
                    # Update fields
                    for field, value in data.items():
                        if hasattr(permit, field) and value is not None:
                            setattr(permit, field, value)
                    
                    # Set parsing metadata
                    permit.w1_parse_status = 'success'
                    permit.updated_at = datetime.now()
                    
                    session.commit()
                    logger.info(f"Updated permit {permit_id} with parsed data")
        except Exception as e:
            logger.error(f"Failed to update permit {permit_id} in database: {e}")
    
    async def process_queue(self, batch_size: int = 5):
        """Process pending jobs from the parsing queue."""
        pending_jobs = parsing_queue.get_pending_jobs(batch_size)
        
        if not pending_jobs:
            return
        
        logger.info(f"Processing {len(pending_jobs)} parsing jobs")
        
        for job in pending_jobs:
            try:
                success, data, confidence = await self.process_permit(
                    job.permit_id, 
                    job.status_no, 
                    job.strategy
                )
                
                if success:
                    parsing_queue.update_job(
                        job.permit_id,
                        ParseStatus.SUCCESS,
                        parsed_fields=data,
                        confidence_score=confidence
                    )
                    logger.info(f"Successfully parsed permit {job.status_no} (confidence: {confidence:.2f})")
                else:
                    parsing_queue.update_job(
                        job.permit_id,
                        ParseStatus.FAILED,
                        error_message="Parsing failed with current strategy"
                    )
                    logger.warning(f"Failed to parse permit {job.status_no}")
                
            except Exception as e:
                parsing_queue.update_job(
                    job.permit_id,
                    ParseStatus.FAILED,
                    error_message=str(e)
                )
                logger.error(f"Error processing job {job.permit_id}: {e}")
            
            # Small delay between jobs to be respectful
            await asyncio.sleep(1)
    
    def add_permit_to_queue(self, permit_id: str, status_no: str):
        """Add a new permit to the parsing queue."""
        return parsing_queue.add_job(permit_id, status_no)

# Global parsing worker instance
parsing_worker = EnhancedParsingWorker()
