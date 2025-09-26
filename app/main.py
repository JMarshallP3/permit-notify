from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import sys
import os
import logging
import requests
from datetime import datetime
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from routes import api_router
from services.scraper.scraper import Scraper
from services.scraper.rrc_w1 import RRCW1Client, EngineRedirectToLogin
from services.enrichment.worker import EnrichmentWorker, run_once
from services.enrichment.detail_parser import parse_detail_page
from services.enrichment.pdf_parse import extract_text_from_pdf, parse_reservoir_well_count
from db.models import Permit
from db.session import Base, engine
from db.repo import upsert_permits, get_recent_permits, get_reservoir_trends

logger = logging.getLogger(__name__)

app = FastAPI(
    title="PermitTracker",
    description="Professional permit monitoring dashboard and API",
    version="1.0.0"
)

# Mount static files and templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Create a single Scraper instance for reuse
scraper_instance = Scraper()

# Create a single RRCW1Client instance for reuse
rrc_w1_client = RRCW1Client()

# Create a single EnrichmentWorker instance for reuse
enrichment_worker = EnrichmentWorker()

# Include the API routes
app.include_router(api_router, prefix="/api/v1")

# Dashboard routes
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Serve the modern dashboard interface."""
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_alt(request: Request):
    """Alternative dashboard route."""
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.on_event("startup")
async def startup_event():
    """Initialize database tables and start background cron on startup."""
    # Start background cron for permit scraping
    try:
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from background_cron import background_cron
        background_cron.start()
        logger.info("🚀 Background permit scraper started (every 10 minutes)")
    except Exception as e:
        logger.error(f"❌ Failed to start background cron: {e}")
    
    # Initialize database tables on startup.
    try:
        logger.info("Initializing database operations")
        # Base.metadata.create_all(bind=engine)  # Uncomment if needed
        logger.info("✅ Database initialization completed")
    except Exception as e:
        logger.error(f"❌ Failed to initialize database: {e}")
        # Don't raise - just log the error
        pass

@app.get("/api/status")
async def api_status():
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
        logger.info(f"Fetching {limit} recent permits from database")
        permits = get_recent_permits(limit)
        return {
            "permits": permits,
            "count": len(permits),
            "limit": limit
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
            try:
                logger.info(f"Storing {len(result['items'])} permits in database")
                upsert_result = upsert_permits(result["items"])
                result["database"] = upsert_result
                logger.info(f"✅ Stored {upsert_result['inserted']} new permits, updated {upsert_result['updated']} permits")
            except Exception as db_error:
                logger.error(f"❌ Database storage failed: {db_error}")
                result["database"] = {"inserted": 0, "updated": 0, "error": str(db_error)}
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

@app.post("/enrich/auto")
async def run_auto_enrichment(
    batch_size: int = Query(10, ge=1, le=50, description="Number of permits to process per batch"),
    max_batches: int = Query(5, ge=1, le=20, description="Maximum number of batches to run")
):
    """
    Run automated enrichment worker to process all pending permits.
    
    Args:
        batch_size: Number of permits to process per batch (1-50)
        max_batches: Maximum number of batches to process (1-20)
        
    Returns:
        Dictionary with enrichment results
    """
    try:
        logger.info(f"Starting automated enrichment: {batch_size} permits per batch, max {max_batches} batches")
        
        total_processed = 0
        total_successful = 0
        total_failed = 0
        batches_run = 0
        
        # Run enrichment in batches until no more permits need processing
        while batches_run < max_batches:
            results = run_once(limit=batch_size)
            
            # If no permits were processed, we're done
            if results['processed'] == 0:
                logger.info("No more permits need enrichment")
                break
                
            total_processed += results['processed']
            total_successful += results['successful']
            total_failed += results['failed']
            batches_run += 1
            
            logger.info(f"Batch {batches_run}: {results['processed']} processed, "
                       f"{results['successful']} successful, {results['failed']} failed")
            
            # Small delay between batches to be respectful to the RRC servers
            import asyncio
            await asyncio.sleep(2)
        
        logger.info(f"Auto-enrichment completed: {total_processed} total processed, "
                   f"{total_successful} successful, {total_failed} failed across {batches_run} batches")
        
        return {
            "processed": total_processed,
            "successful": total_successful,
            "failed": total_failed,
            "batches_run": batches_run,
            "status": "completed" if batches_run < max_batches else "max_batches_reached"
        }
        
    except Exception as e:
        logger.error(f"Auto-enrichment error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Auto-enrichment failed: {str(e)}"
        )

@app.get("/scrape-and-enrich")
async def scrape_and_enrich():
    """
    Combined endpoint that scrapes today's permits AND enriches them.
    Perfect for cron jobs - does everything in one call.
    
    Returns:
        Dictionary with scraping and enrichment results
    """
    try:
        logger.info("🔄 Starting combined scrape-and-enrich process")
        
        # Step 1: Scrape today's permits
        today = datetime.now().strftime("%m/%d/%Y")
        logger.info(f"📅 Scraping permits for {today}")
        
        # Call internal scraping endpoint
        try:
            scrape_response = requests.get(
                f"http://localhost:8000/w1/search",
                params={"begin": today, "end": today, "pages": 5},
                timeout=120
            )
            
            if scrape_response.status_code == 200:
                scrape_data = scrape_response.json()
                permits_found = len(scrape_data.get("items", []))
                db_info = scrape_data.get("database", {})
                permits_inserted = db_info.get("inserted", 0)
                permits_updated = db_info.get("updated", 0)
                
                logger.info(f"✅ Scraping completed: {permits_found} found, {permits_inserted} new, {permits_updated} updated")
            else:
                logger.warning(f"⚠️ Scraping returned status {scrape_response.status_code}")
                permits_found = permits_inserted = permits_updated = 0
                
        except Exception as scrape_error:
            logger.error(f"❌ Scraping failed: {scrape_error}")
            permits_found = permits_inserted = permits_updated = 0
        
        # Step 2: Enrich permits (regardless of scraping results)
        logger.info("🔍 Starting enrichment process")
        
        enrichment_results = run_once(limit=20)  # Process up to 20 permits
        
        logger.info(f"✅ Enrichment completed: {enrichment_results['processed']} processed, "
                   f"{enrichment_results['successful']} successful, {enrichment_results['failed']} failed")
        
        # Step 3: Return combined results
        return {
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "date_processed": today,
            "scraping": {
                "permits_found": permits_found,
                "permits_inserted": permits_inserted,
                "permits_updated": permits_updated
            },
            "enrichment": {
                "permits_processed": enrichment_results['processed'],
                "permits_successful": enrichment_results['successful'],
                "permits_failed": enrichment_results['failed']
            },
            "message": f"Scraped {permits_found} permits ({permits_inserted} new), enriched {enrichment_results['processed']} permits"
        }
        
    except Exception as e:
        logger.error(f"❌ Combined scrape-and-enrich error: {e}")
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/enrich/trigger")
async def trigger_enrichment():
    """
    Simple trigger endpoint for automated enrichment.
    Can be called by external cron services or monitoring tools.
    
    Returns:
        Dictionary with enrichment results
    """
    try:
        logger.info("🔄 Triggered automated enrichment via GET endpoint")
        
        # Run auto-enrichment with sensible defaults
        results = run_once(limit=15)  # Process up to 15 permits
        
        if results['processed'] > 0:
            logger.info(f"✅ Triggered enrichment completed: {results['processed']} processed, "
                       f"{results['successful']} successful, {results['failed']} failed")
        else:
            logger.info("ℹ️  No permits needed enrichment")
        
        return {
            "success": True,
            "processed": results['processed'],
            "successful": results['successful'],
            "failed": results['failed'],
            "message": f"Processed {results['processed']} permits" if results['processed'] > 0 else "No permits needed enrichment",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Triggered enrichment error: {e}")
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

# Flagging endpoints temporarily disabled due to import conflicts
# Will be re-enabled once database import issues are resolved

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

@app.get("/api/v1/reservoir-trends")
async def get_reservoir_trends_api(
    days: int = Query(default=90, description="Number of days to look back"),
    reservoirs: str = Query(default="", description="Comma-separated list of specific reservoirs"),
    view_type: str = Query(default="daily", description="View type: 'daily' or 'cumulative'"),
    mappings: str = Query(default="", description="JSON string of reservoir mappings")
):
    """Get historical reservoir permit trends for charting."""
    try:
        reservoir_list = [r.strip() for r in reservoirs.split(",") if r.strip()] if reservoirs else None
        
        # Parse reservoir mappings if provided
        reservoir_mappings = {}
        if mappings:
            import json
            try:
                reservoir_mappings = json.loads(mappings)
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON in mappings parameter: {mappings}")
        
        trends_data = get_reservoir_trends(
            days_back=days, 
            specific_reservoirs=reservoir_list, 
            view_type=view_type,
            reservoir_mappings=reservoir_mappings
        )
        
        return {
            "success": True,
            "data": trends_data,
            "days_back": days,
            "total_reservoirs": len(trends_data.get("reservoirs", {})),
            "view_type": view_type
        }
        
    except Exception as e:
        logger.error(f"Reservoir trends error: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch reservoir trends")

@app.post("/enrich/all-missing")
async def enrich_all_missing_permits():
    """
    Enrich ALL permits that are missing detailed information (regardless of date).
    This is useful for backfilling existing permits.
    """
    try:
        from datetime import datetime, timedelta
        from db.session import get_session
        from db.models import Permit
        from services.enrichment.worker import EnrichmentWorker
        
        # Get all permits that need enrichment (missing field_name, acres, or section)
        with get_session() as session:
            permits = session.query(Permit).filter(
                (Permit.field_name == None) | 
                (Permit.field_name == '') |
                (Permit.acres == None) |
                (Permit.section == None)
            ).limit(50).all()  # Limit to 50 to prevent overload
            
            if not permits:
                return {
                    "success": True,
                    "message": "No permits need enrichment",
                    "enriched_count": 0
                }
            
            logger.info(f"🔄 Starting enrichment for {len(permits)} permits missing data")
            
            # Initialize enrichment worker
            worker = EnrichmentWorker()
            enriched_count = 0
            
            # Enrich each permit
            for permit in permits:
                try:
                    # Enrich the permit
                    success = await worker.enrich_permit(permit.id)
                    if success:
                        enriched_count += 1
                        logger.info(f"✅ Enriched permit {permit.status_no}")
                    else:
                        logger.warning(f"⚠️ Failed to enrich permit {permit.status_no}")
                        
                except Exception as e:
                    logger.error(f"❌ Error enriching permit {permit.status_no}: {e}")
                    continue
            
            return {
                "success": True,
                "message": f"Backfill enrichment completed",
                "total_permits": len(permits),
                "enriched_count": enriched_count
            }
            
    except Exception as e:
        logger.error(f"Backfill enrichment error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to enrich missing permits: {str(e)}")

@app.post("/enrich/today")
async def enrich_today_permits():
    """
    Enrich all permits from today with detailed information.
    This extracts field names, acres, location data, etc. from detail pages.
    """
    try:
        from datetime import datetime, timedelta
        from db.session import get_session
        from db.models import Permit
        from services.enrichment.worker import EnrichmentWorker
        
        today = datetime.now().date()
        
        # Get today's permits that need enrichment
        with get_session() as session:
            permits = session.query(Permit).filter(
                Permit.status_date >= today,
                Permit.status_date < today + timedelta(days=1)
            ).all()
            
            if not permits:
                return {
                    "success": True,
                    "message": "No permits found for today",
                    "enriched_count": 0,
                    "date": today.isoformat()
                }
            
            logger.info(f"🔄 Starting enrichment for {len(permits)} permits from {today}")
            
            # Initialize enrichment worker
            worker = EnrichmentWorker()
            enriched_count = 0
            
            # Enrich each permit
            for permit in permits:
                try:
                    # Check if already enriched
                    if permit.field_name or permit.acres or permit.section:
                        continue  # Skip already enriched permits
                    
                    # Enrich the permit
                    success = await worker.enrich_permit(permit.id)
                    if success:
                        enriched_count += 1
                        logger.info(f"✅ Enriched permit {permit.status_no}")
                    else:
                        logger.warning(f"⚠️ Failed to enrich permit {permit.status_no}")
                        
                except Exception as e:
                    logger.error(f"❌ Error enriching permit {permit.status_no}: {e}")
                    continue
            
            return {
                "success": True,
                "message": f"Enrichment completed for {today}",
                "total_permits": len(permits),
                "enriched_count": enriched_count,
                "date": today.isoformat()
            }
            
    except Exception as e:
        logger.error(f"Enrichment error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to enrich today's permits: {str(e)}")

@app.post("/api/v1/permits/reenrich")
async def reenrich_permits(request_data: dict):
    """
    Queue permits for re-enrichment (more reliable than parsing).
    Expected payload: {
        "status_numbers": ["123456", "789012"],
        "reason": "Manual flag from dashboard"
    }
    """
    try:
        status_numbers = request_data.get("status_numbers", [])
        reason = request_data.get("reason", "Manual re-enrichment request")
        
        if not status_numbers:
            raise HTTPException(status_code=400, detail="status_numbers is required")
        
        if not isinstance(status_numbers, list):
            raise HTTPException(status_code=400, detail="status_numbers must be a list")
        
        # Limit to prevent overwhelming the system
        if len(status_numbers) > 50:
            raise HTTPException(status_code=400, detail="Cannot re-enrich more than 50 permits at once")
        
        # Import enrichment components
        from services.enrichment.worker import EnrichmentWorker
        from db.session import get_session
        from db.models import Permit
        
        results = []
        worker = EnrichmentWorker()
        
        for status_no in status_numbers:
            try:
                # Find the permit in the database
                with get_session() as session:
                    permit = session.query(Permit).filter(Permit.status_no == status_no).first()
                    if not permit:
                        results.append({
                            "status_no": status_no,
                            "status": "error",
                            "error": "Permit not found"
                        })
                        continue
                    
                    permit_id = permit.id
                
                # Trigger enrichment for this permit
                success = await worker.enrich_permit(permit_id)
                
                if success:
                    results.append({
                        "status_no": status_no,
                        "status": "enriched",
                        "message": "Successfully re-enriched"
                    })
                    logger.info(f"Successfully re-enriched permit {status_no}: {reason}")
                else:
                    results.append({
                        "status_no": status_no,
                        "status": "failed",
                        "error": "Enrichment failed"
                    })
                    logger.warning(f"Failed to re-enrich permit {status_no}: {reason}")
                
            except Exception as e:
                logger.error(f"Failed to re-enrich permit {status_no}: {e}")
                results.append({
                    "status_no": status_no,
                    "status": "error",
                    "error": str(e)
                })
        
        successful_enriched = len([r for r in results if r["status"] == "enriched"])
        
        return {
            "success": True,
            "message": f"Re-enriched {successful_enriched} of {len(status_numbers)} permits",
            "results": results,
            "reason": reason
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Re-enrichment API error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to re-enrich permits: {str(e)}")

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up background cron on shutdown."""
    try:
        from background_cron import background_cron
        background_cron.stop()
        logger.info("🛑 Background permit scraper stopped")
    except Exception as e:
        logger.error(f"❌ Failed to stop background cron: {e}")

@app.get("/api/v1/parsing/status")
async def get_parsing_status():
    """Get current parsing queue status and statistics."""
    try:
        # Import here to avoid initialization issues
        from services.parsing.queue import parsing_queue
        stats = parsing_queue.get_statistics()
        
        return {
            "success": True,
            "stats": stats,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Parsing status error: {e}")
        # Return default stats instead of failing completely
        return {
            "success": True,
            "stats": {
                "total_jobs": 0,
                "pending": 0,
                "in_progress": 0,
                "success": 0,
                "failed": 0,
                "manual_review": 0,
                "success_rate": 0.0,
                "avg_confidence": 0.0
            },
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }

@app.get("/api/v1/parsing/failed")
async def get_failed_parsing_jobs(limit: int = Query(default=20, description="Number of failed jobs to return")):
    """Get failed parsing jobs for manual review."""
    try:
        # Import here to avoid initialization issues
        from services.parsing.queue import parsing_queue
        failed_jobs = parsing_queue.get_failed_jobs(limit)
        
        # Convert jobs to serializable format
        jobs_data = []
        for job in failed_jobs:
            jobs_data.append({
                "permit_id": job.permit_id,
                "status_no": job.status_no,
                "status": job.status.value,
                "attempt_count": job.attempt_count,
                "error_message": job.error_message,
                "last_attempt": job.last_attempt.isoformat() if job.last_attempt else None,
                "confidence_score": job.confidence_score
            })
        
        return {
            "success": True,
            "failed_jobs": jobs_data,
            "total_count": len(failed_jobs)
        }
        
    except Exception as e:
        logger.error(f"Failed jobs error: {e}")
        # Return empty results instead of failing completely
        return {
            "success": True,
            "failed_jobs": [],
            "total_count": 0,
            "error": str(e)
        }

@app.post("/api/v1/parsing/retry/{permit_id}")
async def retry_parsing_job(permit_id: str):
    """Manually retry a failed parsing job."""
    try:
        # Import here to avoid initialization issues
        from services.parsing.queue import parsing_queue
        success = parsing_queue.retry_job(permit_id)
        
        if success:
            return {
                "success": True,
                "message": f"Job {permit_id} queued for retry"
            }
        else:
            return {
                "success": False,
                "message": "Job not found or not eligible for retry"
            }
            
    except Exception as e:
        logger.error(f"Retry job error: {e}")
        return {
            "success": False,
            "message": f"Failed to retry parsing job: {str(e)}"
        }

@app.post("/api/v1/parsing/process")
async def process_parsing_queue():
    """Manually trigger parsing queue processing."""
    try:
        # Import here to avoid initialization issues
        from services.parsing.worker import parsing_worker
        await parsing_worker.process_queue(batch_size=5)
        
        return {
            "success": True,
            "message": "Parsing queue processed"
        }
        
    except Exception as e:
        logger.error(f"Process queue error: {e}")
        return {
            "success": False,
            "message": f"Failed to process parsing queue: {str(e)}"
        }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
