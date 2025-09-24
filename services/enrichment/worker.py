"""
Enrichment worker for processing permits and extracting detailed information.
"""

import time
import requests
import argparse
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from sqlalchemy import or_, and_
import sys
import os

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from db.session import get_session
from db.models import Permit
from .detail_parser import parse_detail_page
from .pdf_parse import extract_text_from_pdf, parse_reservoir_well_count, parse_pdf_fields

logger = logging.getLogger(__name__)

class EnrichmentWorker:
    """Worker for enriching permits with detailed information."""
    
    def __init__(self, base_url: str = "https://webapps.rrc.state.tx.us", rate_limit: float = 1.0):
        """
        Initialize the enrichment worker.
        
        Args:
            base_url: Base URL for RRC website
            rate_limit: Requests per second (default: 1.0)
        """
        self.base_url = base_url
        self.rate_limit = rate_limit
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        self.last_request_time = 0.0
        
    def _rate_limit_wait(self, sleep_ms: int = 0):
        """Implement rate limiting between requests."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        min_interval = 1.0 / self.rate_limit
        
        # Add custom sleep if specified
        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000.0)
        
        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _backoff_wait(self, attempt: int, base_delay: float = 1.0) -> float:
        """
        Calculate backoff delay for retries.
        
        Args:
            attempt: Current attempt number (0-based)
            base_delay: Base delay in seconds
            
        Returns:
            Delay in seconds
        """
        # Exponential backoff: 1s, 3s, 10s
        delays = [base_delay, base_delay * 3, base_delay * 10]
        return delays[min(attempt, len(delays) - 1)]
    
    def _make_request(self, url: str, max_retries: int = 3, headers: Optional[dict] = None) -> Optional[requests.Response]:
        """Make HTTP request with exponential backoff."""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=30, headers=headers or {})
                if response.status_code == 200:
                    return response
                elif response.status_code in [429, 500, 502, 503, 504]:
                    wait_time = [1, 3, 10][min(attempt, 2)]
                    self.logger.warning(f"HTTP {response.status_code}, retrying {url} in {wait_time}s")
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"HTTP {response.status_code} for {url}")
                    return None
            except Exception as e:
                self.logger.error(f"Request failed for {url}: {e}")
                if attempt < max_retries - 1:
                    time.sleep([1, 3, 10][min(attempt, 2)])
        return None
    
    def get_pending_permits(self, limit: int = 5) -> List[Permit]:
        """Pick brand-new OR stale rows to retry."""
        with get_session() as session:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=6)
            q = session.query(Permit).filter(
                Permit.detail_url.isnot(None),
                or_(
                    Permit.w1_parse_status.is_(None),
                    and_(Permit.w1_parse_status.in_(['partial','parse_error','download_error','no_pdf']),
                         or_(Permit.w1_last_enriched_at.is_(None),
                             Permit.w1_last_enriched_at < cutoff))
                )
            ).order_by(Permit.id.desc()).limit(limit)
            permits = q.all()
            session.expunge_all()
            return permits
    
    def _enrich_permit(self, permit: Permit, sleep_ms: int = 0) -> Dict[str, Any]:
        """
        Enrich a single permit with detailed information.
        
        Args:
            permit: Permit object to enrich
            sleep_ms: Additional sleep time between requests
            
        Returns:
            Dictionary with enrichment results
        """
        result = {
            'permit_id': permit.id,
            'status_no': permit.status_no,
            'success': False,
            'horizontal_wellbore': None,
            'field_name': None,
            'acres': None,
            'section': None,
            'block': None,
            'survey': None,
            'abstract_no': None,
            'reservoir_well_count': None,
            'w1_pdf_url': None,
            'w1_parse_status': 'failed',
            'w1_parse_confidence': 0.0,
            'w1_text_snippet': '',
            'error': None
        }
        
        try:
            # Check if permit has a detail URL
            if not hasattr(permit, 'detail_url') or not permit.detail_url:
                result['w1_parse_status'] = 'no_detail_url'
                result['error'] = 'No detail URL available'
                return result
            
            # Step 1: GET detail_url
            logger.info(f"Fetching detail page for permit {permit.status_no}: {permit.detail_url}")
            response = self._make_request(permit.detail_url)
            
            if not response:
                result['w1_parse_status'] = 'download_error'
                result['error'] = 'Failed to fetch detail page'
                return result
            
            # Step 2: Parse detail page
            detail_data = parse_detail_page(response.text, permit.detail_url)
            
            # Update result with parsed data
            result.update({
                'horizontal_wellbore': detail_data.get('horizontal_wellbore'),
                'field_name': detail_data.get('field_name'),
                'acres': detail_data.get('acres'),
                'section': detail_data.get('section'),
                'block': detail_data.get('block'),
                'survey': detail_data.get('survey'),
                'abstract_no': detail_data.get('abstract_no'),
                'w1_pdf_url': detail_data.get('view_w1_pdf_url')
            })
            
            # Step 3: Process PDF if available
            if detail_data.get('view_w1_pdf_url'):
                logger.info(f"Downloading PDF for permit {permit.status_no}: {detail_data['view_w1_pdf_url']}")
                
                pdf_response = self._make_request(
                    detail_data['view_w1_pdf_url'], 
                    headers={'Referer': permit.detail_url}
                )
                
                if pdf_response:
                    try:
                        # Extract text from PDF
                        pdf_text = extract_text_from_pdf(pdf_response.content)
                        
                        if pdf_text:
                            # Parse all PDF fields comprehensively
                            pdf_data = parse_pdf_fields(pdf_text)
                            
                            # Update result with all parsed fields
                            result['reservoir_well_count'] = pdf_data.get('reservoir_well_count')
                            result['section'] = pdf_data.get('section')
                            result['block'] = pdf_data.get('block')
                            result['survey'] = pdf_data.get('survey')
                            result['abstract_no'] = pdf_data.get('abstract_no')
                            result['acres'] = pdf_data.get('acres')
                            # Only update field_name from PDF if not already found in HTML
                            if not result.get('field_name'):
                                result['field_name'] = pdf_data.get('field_name')
                            result['w1_text_snippet'] = pdf_data.get('snippet')
                            result['w1_pdf_url'] = detail_data['view_w1_pdf_url']
                            result['w1_parse_confidence'] = pdf_data.get('confidence', 0.0)
                        else:
                            logger.warning(f"No text extracted from PDF for permit {permit.status_no}")
                            
                    except Exception as e:
                        logger.error(f"Error processing PDF for permit {permit.status_no}: {e}")
                        result['error'] = f"PDF processing error: {str(e)}"
                elif detail_data.get('view_w1_pdf_url') and not pdf_response:
                    result['w1_parse_status'] = 'download_error'
                    result['success'] = True  # still persist HTML fields if any
            else:
                result['w1_parse_status'] = 'no_pdf'
                result['error'] = 'No PDF URL found on detail page'
            
            # Step 4: Compute confidence score
            confidence = 0.0
            if result['horizontal_wellbore']:       confidence += 0.3
            if result['field_name']:                confidence += 0.3
            if result['acres'] is not None:         confidence += 0.2
            if result['reservoir_well_count'] is not None:
                confidence += 0.2 + min(0.2, float(result.get('w1_parse_confidence', 0.0)))
            result['w1_parse_confidence'] = min(confidence, 1.0)
            
            # Step 5: Determine final status based on what we actually parsed
            # Count how many of the 4 fields we have
            fields_found = sum([
                bool(result['horizontal_wellbore']),
                bool(result['field_name']),
                result['acres'] is not None,
                result['reservoir_well_count'] is not None
            ])
            
            # Check what we actually parsed
            html_fields_parsed = any([
                result['horizontal_wellbore'],
                result['field_name'],
                result['acres'] is not None,
                result['section'],
                result['block'],
                result['survey'],
                result['abstract_no']
            ])
            
            pdf_parsed = result['reservoir_well_count'] is not None
            
            # Status rules:
            # - 'ok' only when confidence >= 0.6 AND we have >=2 of {horizontal_wellbore, field_name, acres, reservoir_well_count}
            # - 'partial' if we fetched detail page and parsed at least one HTML field
            # - 'partial' if we found PDF and parsed count (or 'ok' if confidence >= 0.6)
            # - 'no_pdf' if no PDF link
            # - 'parse_error' or 'download_error' on exceptions
            if fields_found >= 2 and confidence >= 0.6:
                result['w1_parse_status'] = 'ok'
                result['success'] = True
            elif html_fields_parsed or pdf_parsed:
                result['w1_parse_status'] = 'partial'
                result['success'] = True  # Consider partial as success since we got some data
            elif not result['w1_pdf_url']:
                result['w1_parse_status'] = 'no_pdf'
            else:
                result['w1_parse_status'] = 'parse_error'
            
            # Always set last enriched timestamp
            result['w1_last_enriched_at'] = datetime.now(timezone.utc)
            
            logger.info(f"Successfully enriched permit {permit.status_no}: confidence={confidence:.2f}, status={result['w1_parse_status']}")
            
        except Exception as e:
            logger.error(f"Error enriching permit {permit.status_no}: {e}")
            result['error'] = str(e)
            result['w1_parse_status'] = 'parse_error'
            result['w1_last_enriched_at'] = datetime.now(timezone.utc)
        
        return result
    
    def _update_permit_in_db(self, permit: Permit, result: Dict[str, Any]):
        """Update permit in database with enrichment results (no None clobbering)."""
        with get_session() as session:
            db_permit = session.query(Permit).filter(Permit.id == permit.id).first()
            if not db_permit:
                return

            def set_if(field, key):
                val = result.get(key)
                if val is not None:
                    setattr(db_permit, field, val)

            set_if('horizontal_wellbore', 'horizontal_wellbore')
            set_if('field_name',          'field_name')
            set_if('acres',               'acres')
            set_if('section',             'section')
            set_if('block',               'block')
            set_if('survey',              'survey')
            set_if('abstract_no',         'abstract_no')
            set_if('w1_pdf_url',          'w1_pdf_url')
            set_if('reservoir_well_count','reservoir_well_count')
            set_if('w1_text_snippet',     'w1_text_snippet')

            # Always refresh status/conf/timestamp
            db_permit.w1_parse_status     = result.get('w1_parse_status')
            db_permit.w1_parse_confidence = result.get('w1_parse_confidence')
            db_permit.w1_last_enriched_at = result.get('w1_last_enriched_at')

            session.commit()
    
    def run(self, limit: int = 5, sleep_ms: int = 0) -> Dict[str, Any]:
        """
        Run the enrichment worker.
        
        Args:
            limit: Maximum number of permits to process
            sleep_ms: Additional sleep time between requests
            
        Returns:
            Dictionary with processing results
        """
        logger.info(f"Starting enrichment worker for up to {limit} permits")
        
        start_time = time.time()
        results = {
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'partial': 0,
            'no_pdf': 0,
            'download_errors': 0,
            'parse_errors': 0,
            'errors': []
        }
        
        try:
            # Get pending permits
            permits = self.get_pending_permits(limit)
            
            if not permits:
                logger.info("No permits need enrichment")
                return results
            
            # Process each permit
            for permit in permits:
                try:
                    logger.info(f"Processing permit {permit.status_no} ({results['processed'] + 1}/{len(permits)})")
                    
                    # Enrich the permit
                    result = self._enrich_permit(permit, sleep_ms)
                    
                    # Update database
                    self._update_permit_in_db(permit, result)
                    
                    # Update statistics
                    results['processed'] += 1
                    
                    # Categorize results by status
                    status = result['w1_parse_status']
                    if status == 'ok':
                        results['successful'] += 1
                    elif status == 'partial':
                        results['partial'] += 1
                    elif status == 'no_pdf':
                        results['no_pdf'] += 1
                    elif status == 'download_error':
                        results['download_errors'] += 1
                    elif status in ['parse_error', 'error']:
                        results['parse_errors'] += 1
                        results['failed'] += 1
                    else:
                        results['failed'] += 1
                    
                    # Track errors for debugging
                    if result.get('error'):
                        results['errors'].append(f"Permit {permit.status_no}: {result['error']}")
                    
                except Exception as e:
                    logger.error(f"Error processing permit {permit.status_no}: {e}")
                    results['failed'] += 1
                    results['errors'].append(f"Permit {permit.status_no}: {str(e)}")
            
            # Log final results
            elapsed_time = time.time() - start_time
            logger.info(f"Enrichment completed: {results['processed']} processed, "
                       f"{results['successful']} ok, {results['partial']} partial, "
                       f"{results['failed']} failed in {elapsed_time:.1f}s")
            
            return results
            
        except Exception as e:
            logger.error(f"Error in enrichment worker: {e}")
            results['errors'].append(f"Worker error: {str(e)}")
            return results

def run_once(limit: int = 5) -> Dict[str, Any]:
    """
    Run the enrichment worker once for a specified number of permits.
    
    Args:
        limit: Maximum number of permits to process
        
    Returns:
        Dictionary with processing results
    """
    worker = EnrichmentWorker()
    return worker.run(limit=limit, sleep_ms=0)

def main():
    """Main entry point for the enrichment worker."""
    parser = argparse.ArgumentParser(description='Enrichment worker for processing permits')
    parser.add_argument('--limit', type=int, default=5, help='Maximum number of permits to process (default: 5)')
    parser.add_argument('--sleep-ms', type=int, default=0, help='Additional sleep time between requests in milliseconds (default: 0)')
    parser.add_argument('--log-level', type=str, default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help='Log level (default: INFO)')
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create and run worker
    worker = EnrichmentWorker()
    results = worker.run(limit=args.limit, sleep_ms=args.sleep_ms)
    
    # Print summary
    print(f"\n📊 ENRICHMENT SUMMARY:")
    print(f"Processed: {results['processed']}")
    print(f"OK: {results['successful']}")
    print(f"Partial: {results['partial']}")
    print(f"Failed: {results['failed']}")
    print(f"No PDF: {results['no_pdf']}")
    print(f"Download Errors: {results['download_errors']}")
    print(f"Parse Errors: {results['parse_errors']}")
    
    if results['errors']:
        print(f"\n❌ ERRORS:")
        for error in results['errors'][:10]:  # Show first 10 errors
            print(f"  {error}")
        if len(results['errors']) > 10:
            print(f"  ... and {len(results['errors']) - 10} more errors")

if __name__ == "__main__":
    main()