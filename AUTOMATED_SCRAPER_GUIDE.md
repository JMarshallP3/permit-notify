# 🤖 Automated Permit Scraper & AI Analysis System

## 🚀 **Quick Start**

### **Start Production Scraper:**
```bash
python start_production_scraper.py
```

### **Check Status:**
```bash
python scraper_control.py status
```

### **Generate AI Analysis:**
```bash
python ai_trend_analyzer.py --report
```

---

## ⚙️ **System Overview**

### **🔄 Automated Scraper**
- **Schedule**: Monday-Friday, 7:00 AM - 6:00 PM
- **Interval**: 5 minutes (configurable)
- **Target**: Today's permits only
- **Database**: Railway PostgreSQL (automatic updates)
- **Logging**: All activity saved to `scraper.log`
- **Statistics**: AI analysis data saved to `scrape_stats.jsonl`

### **🤖 AI Trend Analysis**
- **Daily pattern analysis**
- **Anomaly detection** (volume spikes, unusual activity)
- **Market trend identification**
- **Strategic recommendations**
- **ChatGPT integration ready**

---

## 📋 **Commands Reference**

### **Scraper Control:**
```bash
# Start with default settings (10 min intervals, 7AM-6PM)
python scraper_control.py start

# Start with 5-minute intervals
python scraper_control.py start 5

# Start with custom hours (5 min intervals, 8AM-5PM)
python scraper_control.py start 5 8 17

# Check current status and statistics
python scraper_control.py status

# Install dependencies
python scraper_control.py install
```

### **Production Scraper:**
```bash
# Start optimized production scraper (5 min intervals)
python start_production_scraper.py
```

### **AI Analysis:**
```bash
# Generate full analysis report
python ai_trend_analyzer.py --report

# Generate ChatGPT analysis prompt
python ai_trend_analyzer.py --prompt

# Get raw JSON insights
python ai_trend_analyzer.py
```

---

## 🎯 **What It Does**

### **Real-Time Monitoring:**
✅ **Scrapes permits** every 5 minutes during business hours  
✅ **Updates database** automatically (Railway PostgreSQL)  
✅ **Detects new permits** and changes  
✅ **Logs all activity** with timestamps  
✅ **Handles errors** gracefully with retry logic  

### **AI-Powered Analysis:**
✅ **Daily pattern analysis** - tracks permit volumes and trends  
✅ **Anomaly detection** - identifies unusual activity spikes  
✅ **Market insights** - trend analysis and recommendations  
✅ **ChatGPT integration** - generates prompts for deeper AI analysis  
✅ **Strategic alerts** - notifications for significant changes  

---

## 📊 **Sample AI Insights**

After running for a few days, you'll get insights like:

```
🤖 AI Permit Trend Analysis
==================================================
📊 Summary (5 days analyzed):
   Latest: 2025-09-25
   Permits: 22
   Trend: increasing (+15.2%)

🚨 Anomalies Detected:
   2025-09-24: High Volume
      Value: 35 permits
      Significance: 2.1x normal

💡 Recommendations:
   📈 Permit activity is increasing - consider monitoring for new operators
   🚨 High permit volume detected - investigate major drilling campaigns
```

---

## 🔮 **Next Steps (Phase 1.5 - AI Integration)**

### **Immediate (Ready Now):**
1. **Run production scraper** for 3-5 days to collect data
2. **Generate daily AI reports** using the trend analyzer
3. **Set up ChatGPT integration** for deeper analysis

### **Coming Soon:**
1. **Web dashboard** with real-time permit feed
2. **Email/SMS notifications** for anomalies
3. **Operator-specific tracking** and alerts
4. **Geographic clustering** analysis
5. **Mobile app** with push notifications

---

## 🛠️ **Technical Details**

### **Files Created:**
- `automated_scraper.py` - Main scraper engine
- `scraper_control.py` - Control interface
- `start_production_scraper.py` - Production launcher
- `ai_trend_analyzer.py` - AI analysis engine
- `scraper.log` - Activity log (auto-created)
- `scrape_stats.jsonl` - Statistics for AI (auto-created)

### **Dependencies:**
- `schedule>=1.2.0` - Job scheduling
- All existing requirements (FastAPI, PostgreSQL, etc.)

### **Database Integration:**
- Uses your existing Railway PostgreSQL database
- Updates permits automatically via your FastAPI endpoints
- No schema changes required

---

## 🎉 **Ready to Go!**

Your automated permit monitoring system is ready! 

**To start:**
```bash
python start_production_scraper.py
```

**Then check back in a few hours:**
```bash
python ai_trend_analyzer.py --report
```

The system will:
- ✅ Monitor permits every 5 minutes (7 AM - 6 PM weekdays)
- ✅ Update your database automatically  
- ✅ Collect data for AI analysis
- ✅ Generate insights and recommendations
- ✅ Prepare for ChatGPT integration

**Next**: After collecting a few days of data, we'll build the web dashboard and mobile notifications! 📱🚀
