#!/usr/bin/env python3
"""
Reparse a specific permit that has parsing issues.
Usage: python reparse_problem_permit.py <status_no>
"""

import sys
import os
import asyncio
import logging

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.parsing.worker import parsing_worker
from services.parsing.queue import parsing_queue, ParseStrategy, ParseStatus
from db.session import get_session
from db.models import Permit

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def reparse_permit(status_no: str):
    """Reparse a specific permit by status number."""
    
    # Get permit from database
    with get_session() as session:
        permit = session.query(Permit).filter(Permit.status_no == status_no).first()
        if not permit:
            print(f"❌ Permit {status_no} not found in database")
            return False
        
        print(f"🔍 Found permit: {status_no} - {permit.lease_name}")
        print(f"📄 Detail URL: {permit.detail_url}")
        
        # Show current parsing status
        print(f"\n📊 Current parsing status:")
        print(f"   Section: {permit.section}")
        print(f"   Block: {permit.block}")
        print(f"   Survey: {permit.survey}")
        print(f"   Abstract: {permit.abstract_no}")
        print(f"   Acres: {permit.acres}")
        print(f"   Field: {permit.field_name}")
        print(f"   Reservoir Well Count: {permit.reservoir_well_count}")
        print(f"   Parse Status: {permit.w1_parse_status}")
        print(f"   Parse Confidence: {permit.w1_parse_confidence}")
        
        if permit.w1_text_snippet:
            print(f"   Text Snippet: {permit.w1_text_snippet[:200]}...")
    
    print(f"\n🔄 Starting reparse process...")
    
    # Add to parsing queue and process immediately
    job = parsing_queue.add_job(status_no, status_no, ParseStrategy.RETRY_FRESH_SESSION)
    print(f"✅ Added to parsing queue: {job.status.value}")
    
    # Process the permit
    success, data, confidence = await parsing_worker.process_permit(
        status_no, 
        status_no, 
        ParseStrategy.RETRY_FRESH_SESSION
    )
    
    if success:
        print(f"\n🎉 Parsing successful!")
        print(f"   Confidence Score: {confidence:.2f}")
        print(f"   Parsed Fields: {len(data)} fields")
        
        for field, value in data.items():
            if value is not None:
                print(f"   {field}: {value}")
        
        # Update parsing queue
        parsing_queue.update_job(
            status_no,
            ParseStatus.SUCCESS,
            parsed_fields=data,
            confidence_score=confidence
        )
        
    else:
        print(f"\n❌ Parsing failed")
        parsing_queue.update_job(
            status_no,
            ParseStatus.FAILED,
            error_message="Manual reparse attempt failed"
        )
    
    # Show final status
    with get_session() as session:
        permit = session.query(Permit).filter(Permit.status_no == status_no).first()
        print(f"\n📊 Final parsing status:")
        print(f"   Section: {permit.section}")
        print(f"   Block: {permit.block}")
        print(f"   Survey: {permit.survey}")
        print(f"   Abstract: {permit.abstract_no}")
        print(f"   Acres: {permit.acres}")
        print(f"   Field: {permit.field_name}")
        print(f"   Reservoir Well Count: {permit.reservoir_well_count}")
        print(f"   Parse Status: {permit.w1_parse_status}")
        print(f"   Parse Confidence: {permit.w1_parse_confidence}")
    
    return success

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python reparse_problem_permit.py <status_no>")
        print("\nExample permits that need reparsing:")
        print("  910711 - STATE MAYFLY UNIT")
        print("  910712 - CLAY PASTURE -B- STATE UNIT") 
        print("  910713 - STATE MAYFLY UNIT")
        print("  910714 - STATE MAYFLY UNIT")
        print("  910715 - UL GOLD A")
        sys.exit(1)
    
    status_no = sys.argv[1]
    
    try:
        success = asyncio.run(reparse_permit(status_no))
        if success:
            print(f"\n✅ Successfully reparsed permit {status_no}")
        else:
            print(f"\n❌ Failed to reparse permit {status_no}")
    except Exception as e:
        print(f"\n💥 Error reparsing permit {status_no}: {e}")
        import traceback
        traceback.print_exc()
