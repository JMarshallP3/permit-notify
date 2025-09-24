from fastapi import FastAPI, Query, HTTPException
import sys
import os
import logging
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from routes import api_router
from services.scraper.scraper import Scraper
from services.scraper.rrc_w1 import RRCW1Client, EngineRedirectToLogin
from services.enrichment.worker import EnrichmentWorker, run_once
from services.enrichment.detail_parser import parse_detail_page
from services.enrichment.pdf_parse import extract_text_from_pdf, parse_reservoir_well_count
from db.models import Permit
from db.session import Base, engine
from db.repo import upsert_permits, get_recent_permits

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Permit Notify API",
    description="API for permit notification system",
    version="1.0.0"
)

# Create a single Scraper instance for reuse
scraper_instance = Scraper()

# Create a single RRCW1Client instance for reuse
rrc_w1_client = RRCW1Client()

# Create a single EnrichmentWorker instance for reuse
enrichment_worker = EnrichmentWorker()

# Include the API routes
app.include_router(api_router, prefix="/api/v1")

@app.on_event("startup")
async def startup_event():
    """Initialize database tables on startup."""
    try:
        # Check if we're in Railway environment
        if os.getenv('RAILWAY_ENVIRONMENT'):
            logger.info("Running in Railway environment - enabling database operations")
            # Base.metadata.create_all(bind=engine)
            logger.info("Database initialization completed")
        else:
            logger.info("Running locally - database operations disabled for testing")
    except Exception as e:
        logger.error(f"Failed to create database tables: {e}")
        # Don't raise - just log the error
        pass

@app.get("/")
async def root():
    return {"message": "Permit Notify API running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.post("/migrate")
async def run_migration():
    """Run database migrations."""
    try:
        from tools.migrate import run_migrations
        result = run_migrations()
        return {"status": "success", "message": "Migrations completed", "result": result}
    except Exception as e:
        logger.error(f"Migration error: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/scrape")
async def scrape():
    """Scrape permit data and store in database."""
    try:
        # Run scraper
        result = scraper_instance.run()
        
        # Store results in database if we have items
        if result.get("items"):
            upsert_result = upsert_permits(result["items"])
            result["database"] = upsert_result
            logger.info(f"Stored {upsert_result['inserted']} new permits, updated {upsert_result['updated']} permits")
        
        return result
    except Exception as e:
        logger.error(f"Scraping error: {e}")
        return {"error": str(e), "items": [], "warning": "Scraping failed"}

@app.get("/api/v1/permits")
async def get_permits(limit: int = Query(50, ge=1, le=1000)):
    """Get recent permits from database."""
    try:
        # Enable database operations in Railway or Docker development environment
        railway_env = os.getenv('RAILWAY_ENVIRONMENT')
        if railway_env or os.getenv('DATABASE_URL'):  # Enable if Railway env or DATABASE_URL is set
            logger.info(f"Enabling database queries (env: {railway_env or 'docker'})")
            permits = get_recent_permits(limit)
            return {
                "permits": permits,
                "count": len(permits),
                "limit": limit
            }
        else:
            logger.info("Running locally - database query disabled for testing")
            return {
                "permits": [],
                "count": 0,
                "limit": limit,
                "note": "Database query disabled for testing"
            }
    except Exception as e:
        logger.error(f"Database query error: {e}")
        return {"error": str(e), "permits": []}

@app.get("/w1/search")
async def w1_search(
    begin: str = Query(..., description="Start date in MM/DD/YYYY format"),
    end: str = Query(..., description="End date in MM/DD/YYYY format"),
    pages: int = Query(None, description="Maximum number of pages to fetch (None for all)")
):
    """
    Search RRC W-1 drilling permits by date range.
    
    Args:
        begin: Start date in MM/DD/YYYY format (required)
        end: End date in MM/DD/YYYY format (required)
        pages: Maximum number of pages to fetch (optional, None for all)
    
    Returns:
        Dictionary with query results and metadata
    """
    try:
        logger.info(f"W-1 search request: begin={begin}, end={end}, pages={pages}")
        
        # Validate date format (basic validation)
        import re
        date_pattern = r'^\d{2}/\d{2}/\d{4}$'
        if not re.match(date_pattern, begin) or not re.match(date_pattern, end):
            raise HTTPException(
                status_code=400,
                detail="Date format must be MM/DD/YYYY"
            )
        
        # Fetch results using RRCW1Client
        result = rrc_w1_client.fetch_all(begin, end, pages)
        
        # Store results in database if we have items
        if result.get("items"):
            # Enable database operations in Railway or Docker development environment
            railway_env = os.getenv('RAILWAY_ENVIRONMENT')
            if railway_env or os.getenv('DATABASE_URL'):  # Enable if Railway env or DATABASE_URL is set
                logger.info(f"Storing {len(result['items'])} permits in database (env: {railway_env or 'docker'})")
                upsert_result = upsert_permits(result["items"])
                result["database"] = upsert_result
                logger.info(f"Stored {upsert_result['inserted']} new permits, updated {upsert_result['updated']} permits")
            else:
                logger.info(f"Found {len(result['items'])} permits (database storage disabled for testing)")
                result["database"] = {"inserted": 0, "updated": 0, "note": "Database storage disabled for testing"}
        else:
            result["database"] = {"inserted": 0, "updated": 0, "note": "No permits found"}
            logger.info("No permits found")
        
        logger.info(f"W-1 search completed: {result['pages']} pages, {result['count']} items")
        return result
        
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except EngineRedirectToLogin as e:
        logger.warning(f"RRC W-1 search redirected to login: {e}")
        raise HTTPException(
            status_code=502,
            detail="RRC W-1 search redirected to login page. Please try again later."
        )
    except Exception as e:
        logger.error(f"W-1 search error: {e}")
        raise HTTPException(
            status_code=502,
            detail=f"RRC W-1 search failed: {str(e)}"
        )

@app.post("/enrich/run")
async def run_enrichment(n: int = Query(5, ge=1, le=50, description="Maximum number of permits to process")):
    """
    Run the enrichment worker for N permits.
    
    Args:
        n: Maximum number of permits to process (1-50)
        
    Returns:
        Dictionary with enrichment results
    """
    try:
        logger.info(f"Starting enrichment for {n} permits")
        
        # Run the enrichment worker
        results = run_once(limit=n)
        
        logger.info(f"Enrichment completed: {results['processed']} processed, "
                   f"{results['successful']} successful, {results['failed']} failed")
        
        return {
            "processed": results['processed'],
            "ok": results['successful'],
            "errors": results['failed']
        }
        
    except Exception as e:
        logger.error(f"Enrichment error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Enrichment failed: {str(e)}"
        )

@app.get("/enrich/debug/{permit_id}")
async def debug_enrichment(permit_id: int):
    """
    Debug endpoint to test enrichment parsing for a specific permit.
    Fetches detail page and PDF (if available) without writing to database.
    
    Args:
        permit_id: ID of the permit to debug
        
    Returns:
        Dictionary with parsed data for debugging
    """
    try:
        logger.info(f"Debug enrichment for permit ID: {permit_id}")
        
        # Load permit from database
        from db.session import get_session
        with get_session() as session:
            permit = session.query(Permit).filter(Permit.id == permit_id).first()
            if not permit:
                raise HTTPException(
                    status_code=404,
                    detail=f"Permit with ID {permit_id} not found"
                )
            
            # Check if permit has detail URL
            if not permit.detail_url:
                raise HTTPException(
                    status_code=400,
                    detail=f"Permit {permit_id} has no detail URL"
                )
        
        # Use enrichment worker to fetch data (but don't update DB)
        worker = EnrichmentWorker()
        
        # Fetch detail page
        logger.info(f"Fetching detail page: {permit.detail_url}")
        detail_response = worker._make_request(permit.detail_url)
        
        if not detail_response:
            raise HTTPException(
                status_code=502,
                detail="Failed to fetch detail page"
            )
        
        # Parse detail page
        detail_data = parse_detail_page(detail_response.text, permit.detail_url)
        
        result = {
            "permit_id": permit.id,
            "status_no": permit.status_no,
            "detail_url": permit.detail_url,
            "parsed_data": detail_data,
            "pdf_data": None,
            "error": None
        }
        
        # If PDF URL found, fetch and parse it
        if detail_data.get('view_w1_pdf_url'):
            logger.info(f"Fetching PDF: {detail_data['view_w1_pdf_url']}")
            
            pdf_response = worker._make_request(
                detail_data['view_w1_pdf_url'],
                referer=permit.detail_url
            )
            
            if pdf_response:
                try:
                    # Extract text from PDF
                    pdf_text = extract_text_from_pdf(pdf_response.content)
                    
                    if pdf_text:
                        # Parse reservoir well count
                        well_count, confidence, snippet = parse_reservoir_well_count(pdf_text)
                        
                        result["pdf_data"] = {
                            "reservoir_well_count": well_count,
                            "confidence": confidence,
                            "text_snippet": snippet,
                            "pdf_url": detail_data['view_w1_pdf_url'],
                            "text_length": len(pdf_text)
                        }
                    else:
                        result["pdf_data"] = {
                            "error": "No text extracted from PDF",
                            "pdf_url": detail_data['view_w1_pdf_url']
                        }
                        
                except Exception as e:
                    result["pdf_data"] = {
                        "error": f"PDF processing error: {str(e)}",
                        "pdf_url": detail_data['view_w1_pdf_url']
                    }
            else:
                result["pdf_data"] = {
                    "error": "Failed to download PDF",
                    "pdf_url": detail_data['view_w1_pdf_url']
                }
        
        # Calculate confidence score for debugging
        confidence = 0.0
        if detail_data.get('horizontal_wellbore'): confidence += 0.3
        if detail_data.get('field_name'): confidence += 0.3
        if detail_data.get('acres') is not None: confidence += 0.2
        if result.get('pdf_data', {}).get('reservoir_well_count') is not None: confidence += 0.3
        confidence = min(confidence, 1.0)
        
        result["debug_info"] = {
            "confidence": confidence,
            "fields_found": {
                "horizontal_wellbore": bool(detail_data.get('horizontal_wellbore')),
                "field_name": bool(detail_data.get('field_name')),
                "acres": detail_data.get('acres') is not None,
                "section": bool(detail_data.get('section')),
                "block": bool(detail_data.get('block')),
                "survey": bool(detail_data.get('survey')),
                "abstract_no": bool(detail_data.get('abstract_no')),
                "reservoir_well_count": result.get('pdf_data', {}).get('reservoir_well_count') is not None
            }
        }
        
        logger.info(f"Debug enrichment completed for permit {permit_id}: confidence={confidence:.2f}")
        return result
        
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Debug enrichment error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Debug enrichment failed: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
