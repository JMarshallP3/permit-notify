#!/usr/bin/env python3

import json
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any
from collections import defaultdict
import statistics

class PermitTrendAnalyzer:
    """
    AI-powered trend analyzer for permit data.
    Analyzes patterns and generates insights from scraping statistics.
    """
    
    def __init__(self, stats_file: str = "scrape_stats.jsonl"):
        self.stats_file = stats_file
        self.stats_data = []
        self.load_stats()
    
    def load_stats(self):
        """Load statistics from the scrape stats file."""
        if not os.path.exists(self.stats_file):
            print(f"⚠️  Stats file {self.stats_file} not found")
            return
        
        try:
            with open(self.stats_file, 'r') as f:
                self.stats_data = [json.loads(line) for line in f if line.strip()]
            print(f"📊 Loaded {len(self.stats_data)} scrape records")
        except Exception as e:
            print(f"❌ Error loading stats: {e}")
    
    def analyze_daily_patterns(self) -> Dict[str, Any]:
        """Analyze daily permit patterns."""
        if not self.stats_data:
            return {"error": "No data available"}
        
        # Group by date
        daily_data = defaultdict(list)
        for record in self.stats_data:
            timestamp = datetime.fromisoformat(record['timestamp'])
            date_key = timestamp.strftime('%Y-%m-%d')
            daily_data[date_key].append(record)
        
        # Calculate daily statistics
        daily_stats = {}
        for date, records in daily_data.items():
            total_permits = sum(r['permits_found'] for r in records)
            new_permits = sum(r['permits_inserted'] for r in records)
            scrape_count = len(records)
            
            daily_stats[date] = {
                'total_permits_found': total_permits,
                'new_permits': new_permits,
                'scrape_count': scrape_count,
                'avg_permits_per_scrape': total_permits / scrape_count if scrape_count > 0 else 0
            }
        
        return daily_stats
    
    def detect_anomalies(self) -> List[Dict[str, Any]]:
        """Detect unusual permit activity patterns."""
        daily_stats = self.analyze_daily_patterns()
        if 'error' in daily_stats:
            return []
        
        anomalies = []
        
        # Calculate baseline statistics
        permit_counts = [stats['total_permits_found'] for stats in daily_stats.values()]
        new_permit_counts = [stats['new_permits'] for stats in daily_stats.values()]
        
        if len(permit_counts) < 2:
            return anomalies
        
        avg_permits = statistics.mean(permit_counts)
        avg_new_permits = statistics.mean(new_permit_counts)
        
        # Detect anomalies (simple threshold-based for now)
        for date, stats in daily_stats.items():
            # High permit volume
            if stats['total_permits_found'] > avg_permits * 1.5:
                anomalies.append({
                    'type': 'high_volume',
                    'date': date,
                    'permits_found': stats['total_permits_found'],
                    'baseline_avg': avg_permits,
                    'significance': stats['total_permits_found'] / avg_permits
                })
            
            # High new permit rate
            if stats['new_permits'] > avg_new_permits * 2:
                anomalies.append({
                    'type': 'high_new_permits',
                    'date': date,
                    'new_permits': stats['new_permits'],
                    'baseline_avg': avg_new_permits,
                    'significance': stats['new_permits'] / avg_new_permits if avg_new_permits > 0 else float('inf')
                })
        
        return anomalies
    
    def generate_insights(self) -> Dict[str, Any]:
        """Generate AI-style insights from the data."""
        daily_stats = self.analyze_daily_patterns()
        anomalies = self.detect_anomalies()
        
        if 'error' in daily_stats:
            return {"error": "Insufficient data for analysis"}
        
        # Calculate overall trends
        dates = sorted(daily_stats.keys())
        if len(dates) < 2:
            return {"error": "Need at least 2 days of data"}
        
        recent_date = dates[-1]
        previous_date = dates[-2] if len(dates) > 1 else dates[0]
        
        recent_permits = daily_stats[recent_date]['total_permits_found']
        previous_permits = daily_stats[previous_date]['total_permits_found']
        
        trend = "stable"
        change_pct = 0
        
        if previous_permits > 0:
            change_pct = ((recent_permits - previous_permits) / previous_permits) * 100
            if change_pct > 20:
                trend = "increasing"
            elif change_pct < -20:
                trend = "decreasing"
        
        insights = {
            'summary': {
                'total_days_analyzed': len(dates),
                'latest_date': recent_date,
                'recent_permits': recent_permits,
                'trend': trend,
                'change_percentage': round(change_pct, 1)
            },
            'anomalies': anomalies,
            'recommendations': self._generate_recommendations(daily_stats, anomalies, trend)
        }
        
        return insights
    
    def _generate_recommendations(self, daily_stats: Dict, anomalies: List, trend: str) -> List[str]:
        """Generate actionable recommendations."""
        recommendations = []
        
        # Trend-based recommendations
        if trend == "increasing":
            recommendations.append("📈 Permit activity is increasing - consider monitoring for new operators or field developments")
        elif trend == "decreasing":
            recommendations.append("📉 Permit activity is declining - may indicate market slowdown or seasonal effects")
        
        # Anomaly-based recommendations
        if any(a['type'] == 'high_volume' for a in anomalies):
            recommendations.append("🚨 High permit volume detected - investigate for major drilling campaigns or new field development")
        
        if any(a['type'] == 'high_new_permits' for a in anomalies):
            recommendations.append("🆕 Unusual number of new permits - potential new operator activity or lease acquisition")
        
        # General recommendations
        if len(daily_stats) >= 7:
            recommendations.append("📊 Consider implementing weekly trend reports for deeper analysis")
        
        return recommendations
    
    def generate_ai_prompt(self) -> str:
        """Generate a prompt for ChatGPT/AI analysis."""
        insights = self.generate_insights()
        
        if 'error' in insights:
            return "Not enough data for AI analysis yet. Please run the scraper for a few days."
        
        prompt = f"""
Analyze the following Texas RRC drilling permit trends and provide strategic insights:

RECENT ACTIVITY:
- Latest date: {insights['summary']['latest_date']}
- Permits found: {insights['summary']['recent_permits']}
- Trend: {insights['summary']['trend']} ({insights['summary']['change_percentage']}% change)
- Analysis period: {insights['summary']['total_days_analyzed']} days

ANOMALIES DETECTED:
"""
        
        for anomaly in insights['anomalies']:
            prompt += f"- {anomaly['type'].replace('_', ' ').title()}: {anomaly['date']} ({anomaly.get('permits_found', anomaly.get('new_permits'))} permits, {anomaly['significance']:.1f}x normal)\n"
        
        if not insights['anomalies']:
            prompt += "- No significant anomalies detected\n"
        
        prompt += f"""
Please provide:
1. Strategic interpretation of these trends
2. Potential market implications
3. Specific areas or operators to watch
4. Recommended actions for permit monitoring
5. Any patterns that might indicate M&A activity, new field development, or market shifts

Focus on actionable insights for oil & gas professionals monitoring Texas drilling activity.
"""
        
        return prompt
    
    def print_report(self):
        """Print a formatted analysis report."""
        print("🤖 AI Permit Trend Analysis")
        print("=" * 50)
        
        insights = self.generate_insights()
        
        if 'error' in insights:
            print(f"❌ {insights['error']}")
            return
        
        # Summary
        summary = insights['summary']
        print(f"📊 Summary ({summary['total_days_analyzed']} days analyzed):")
        print(f"   Latest: {summary['latest_date']}")
        print(f"   Permits: {summary['recent_permits']}")
        print(f"   Trend: {summary['trend']} ({summary['change_percentage']:+.1f}%)")
        print()
        
        # Anomalies
        if insights['anomalies']:
            print("🚨 Anomalies Detected:")
            for anomaly in insights['anomalies']:
                print(f"   {anomaly['date']}: {anomaly['type'].replace('_', ' ').title()}")
                print(f"      Value: {anomaly.get('permits_found', anomaly.get('new_permits'))}")
                print(f"      Significance: {anomaly['significance']:.1f}x normal")
            print()
        
        # Recommendations
        if insights['recommendations']:
            print("💡 Recommendations:")
            for rec in insights['recommendations']:
                print(f"   {rec}")
            print()
        
        # AI Prompt
        print("🤖 AI Analysis Prompt:")
        print("-" * 30)
        print(self.generate_ai_prompt())

def main():
    """Main function for command line usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description="AI Permit Trend Analyzer")
    parser.add_argument("--stats-file", default="scrape_stats.jsonl",
                       help="Statistics file to analyze")
    parser.add_argument("--report", action="store_true",
                       help="Generate and display analysis report")
    parser.add_argument("--prompt", action="store_true",
                       help="Generate AI analysis prompt")
    
    args = parser.parse_args()
    
    analyzer = PermitTrendAnalyzer(args.stats_file)
    
    if args.report:
        analyzer.print_report()
    elif args.prompt:
        print(analyzer.generate_ai_prompt())
    else:
        # Default: show basic insights
        insights = analyzer.generate_insights()
        print(json.dumps(insights, indent=2))

if __name__ == "__main__":
    main()
