from fastapi import FastAPI, Query
import sys
import os
import logging
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from routes import api_router
from services.scraper.scraper import Scraper
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

# Include the API routes
app.include_router(api_router, prefix="/api/v1")

@app.on_event("startup")
async def startup_event():
    """Initialize database tables on startup."""
    try:
        logger.info("Creating database tables...")
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Failed to create database tables: {e}")
        raise

@app.get("/")
async def root():
    return {"message": "Permit Notify API running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

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
        permits = get_recent_permits(limit)
        return {
            "permits": permits,
            "count": len(permits),
            "limit": limit
        }
    except Exception as e:
        logger.error(f"Database query error: {e}")
        return {"error": str(e), "permits": []}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
