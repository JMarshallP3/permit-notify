from fastapi import FastAPI, Query, HTTPException
import sys
import os
import logging
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from routes import api_router
from services.scraper.scraper import Scraper
from services.scraper.rrc_w1 import RRCW1Client, EngineRedirectToLogin
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

# Include the API routes
app.include_router(api_router, prefix="/api/v1")

@app.on_event("startup")
async def startup_event():
    """Initialize database tables on startup."""
    try:
        logger.info("Skipping database initialization for testing...")
        # Base.metadata.create_all(bind=engine)
        logger.info("Database initialization skipped")
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
        # Skip database query for testing
        logger.info("Database query disabled for testing")
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
        
        # Skip database storage for testing
        if result.get("items"):
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
