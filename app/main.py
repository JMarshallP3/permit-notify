from fastapi import FastAPI
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from routes import api_router
from services.scraper.scraper import Scraper

app = FastAPI(
    title="Permit Notify API",
    description="API for permit notification system",
    version="1.0.0"
)

# Include the API routes
app.include_router(api_router, prefix="/api/v1")

@app.get("/")
async def root():
    return {"message": "Permit Notify API running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.get("/scrape")
async def scrape():
    scraper = Scraper()
    results = scraper.run()
    return {"status": "scraper executed", "results": results}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
