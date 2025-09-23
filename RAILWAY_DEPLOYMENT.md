# Railway Deployment Guide

## 🚀 How to Deploy and Run in Railway Environment

### 1. **Deploy to Railway**

Your project is already set up for Railway deployment with:
- ✅ `Dockerfile` configured
- ✅ `requirements.txt` with all dependencies
- ✅ FastAPI web application
- ✅ Database integration ready

**To deploy:**

1. **Connect to Railway:**
   ```bash
   # Install Railway CLI (if not already installed)
   npm install -g @railway/cli
   
   # Login to Railway
   railway login
   
   # Link to your existing project
   railway link
   ```

2. **Deploy:**
   ```bash
   # Deploy the application
   railway up
   ```

### 2. **Run Database Migrations**

Once deployed, run the database migration:

```bash
# Run migration in Railway environment
railway run python railway_migrate.py
```

Or use the web endpoint:
```
POST https://your-app.railway.app/migrate
```

### 3. **Test the Scraper**

**Option A: Use the Web API**
```bash
# Test the scraper endpoint
curl "https://your-app.railway.app/w1/search?begin=09/23/2025&end=09/23/2025&pages=1"
```

**Option B: Run the scraper script directly**
```bash
# Run the scraper in Railway environment
railway run python save_permits_to_db.py
```

### 4. **Environment Variables**

Railway automatically provides:
- ✅ `DATABASE_URL` - Connected to your Postgres database
- ✅ `RAILWAY_ENVIRONMENT` - Set to "production"

### 5. **Monitor and Debug**

**View logs:**
```bash
railway logs
```

**Access the database:**
```bash
railway connect postgres
```

**Check application status:**
```bash
curl https://your-app.railway.app/health
```

## 🔧 **Current Status**

- ✅ **Scraper**: Working (fetches real permit data)
- ✅ **Database**: Schema updated and ready
- ✅ **Integration**: Code complete
- ✅ **Deployment**: Ready for Railway

## 🎯 **Next Steps**

1. **Deploy to Railway** using the commands above
2. **Run migrations** to ensure database schema is correct
3. **Test the scraper** using the web API or direct script
4. **Monitor logs** to see the scraper working in production

The scraper will work perfectly in the Railway environment where it has direct access to the database!
