"""
Scout v2.2 Social Media Crawler
Handles X/Twitter and other public social platforms (public posts only, no authentication)
"""

import re
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict
from urllib.parse import urljoin, urlparse, quote
from bs4 import BeautifulSoup

from .base_crawler import BaseCrawler, CrawlResult
from db.scout_models import SourceType

logger = logging.getLogger(__name__)

class TwitterCrawler(BaseCrawler):
    """
    X/Twitter crawler for public posts only
    Note: Only crawls publicly viewable content, no authentication
    """
    
    def __init__(self):
        super().__init__(user_agent="ScoutBot/2.2 (PermitTracker; +https://permittracker.com)")
        self.base_url = "https://twitter.com"
        
        # Industry accounts to monitor (public profiles only)
        self.industry_accounts = [
            "eogresources",
            "conocophillips", 
            "chevron",
            "exxonmobil",
            "pioneernrc",
            "diamondbackenergy",
            "devonenergy",
            "cotterraenergy",
            "ovintiv",
            "permianbasin",
            "shaleexperts",
            "oilandgasmag",
            "rigzone",
            "hartenergy"
        ]
        
        # Keywords for discovery
        self.keywords = [
            "permian basin",
            "eagle ford", 
            "bakken",
            "drilling permit",
            "completion",
            "frac",
            "horizontal well",
            "unconventional",
            "shale"
        ]
    
    def get_source_type(self) -> SourceType:
        return SourceType.SOCIAL
    
    async def crawl_recent(self, max_items: int = 10) -> List[CrawlResult]:
        """Crawl recent public tweets"""
        results = []
        
        # Method 1: Try to crawl public profiles
        for account in self.industry_accounts[:5]:  # Limit to prevent overload
            if len(results) >= max_items:
                break
            
            try:
                profile_results = await self._crawl_public_profile(account)
                results.extend(profile_results[:2])  # Limit per account
            except Exception as e:
                logger.error(f"Error crawling Twitter profile @{account}: {e}")
                continue
        
        # Method 2: Try search for keywords (if available publicly)
        if len(results) < max_items:
            try:
                search_results = await self._search_public_tweets()
                results.extend(search_results[:5])
            except Exception as e:
                logger.error(f"Error searching public tweets: {e}")
        
        logger.info(f"Twitter crawler found {len(results)} posts")
        return results[:max_items]
    
    async def _crawl_public_profile(self, username: str) -> List[CrawlResult]:
        """Crawl public profile timeline"""
        try:
            profile_url = f"{self.base_url}/{username}"
            html = await self.fetch_url(profile_url)
            
            if not html:
                return []
            
            # Try to extract tweets from public profile page
            # Note: Twitter's structure changes frequently, this is best-effort
            soup = BeautifulSoup(html, 'html.parser')
            
            results = []
            
            # Look for tweet-like structures in the HTML
            # This is fragile and may need updates as Twitter changes
            tweet_selectors = [
                '[data-testid="tweet"]',
                '.tweet',
                'article[role="article"]',
                '[data-tweet-id]'
            ]
            
            for selector in tweet_selectors:
                tweets = soup.select(selector)
                if tweets:
                    for tweet in tweets[:3]:  # Limit per profile
                        result = self._extract_tweet_data(tweet, profile_url, username)
                        if result:
                            results.append(result)
                    break  # Use first successful selector
            
            return results
            
        except Exception as e:
            logger.error(f"Error crawling profile @{username}: {e}")
            return []
    
    def _extract_tweet_data(self, tweet_elem, profile_url: str, username: str) -> Optional[CrawlResult]:
        """Extract data from tweet element"""
        try:
            # Extract text content
            text_selectors = [
                '[data-testid="tweetText"]',
                '.tweet-text',
                '.TweetTextSize',
                '.js-tweet-text'
            ]
            
            content = ""
            for selector in text_selectors:
                text_elem = tweet_elem.select_one(selector)
                if text_elem:
                    content = text_elem.get_text().strip()
                    break
            
            if not content:
                # Fallback to general text extraction
                content = tweet_elem.get_text().strip()
            
            # Skip if content is too short or doesn't contain relevant keywords
            if len(content) < 20 or not self._is_relevant_content(content):
                return None
            
            # Extract timestamp (if available)
            time_elem = tweet_elem.select_one('time')
            post_date = None
            if time_elem and time_elem.get('datetime'):
                try:
                    post_date = datetime.fromisoformat(time_elem['datetime'].replace('Z', '+00:00'))
                except:
                    pass
            
            # Extract links
            links = []
            for link in tweet_elem.find_all('a', href=True):
                href = link['href']
                if href.startswith('http'):
                    links.append(href)
            
            # Determine author type
            author_type = self._classify_author(username)
            
            return CrawlResult(
                url=profile_url,  # Use profile URL since individual tweet URLs are hard to extract
                title=f"@{username}: {content[:50]}...",
                content=content,
                post_date=post_date or datetime.now(timezone.utc),
                author=f"@{username}",
                author_type=author_type,
                links=links,
                source_type=self.get_source_type(),
                success=True
            )
            
        except Exception as e:
            logger.error(f"Error extracting tweet data: {e}")
            return None
    
    async def _search_public_tweets(self) -> List[CrawlResult]:
        """Search for public tweets with relevant keywords"""
        try:
            results = []
            
            # Try a few keyword searches
            for keyword in self.keywords[:2]:  # Limit searches
                search_url = f"{self.base_url}/search?q={quote(keyword)}&src=typed_query&f=live"
                
                html = await self.fetch_url(search_url)
                if not html:
                    continue
                
                soup = BeautifulSoup(html, 'html.parser')
                
                # Similar extraction logic as profile crawling
                # This is even more fragile as search results have different structure
                search_results = self._extract_search_results(soup, keyword)
                results.extend(search_results[:3])
            
            return results
            
        except Exception as e:
            logger.error(f"Error searching public tweets: {e}")
            return []
    
    def _extract_search_results(self, soup: BeautifulSoup, keyword: str) -> List[CrawlResult]:
        """Extract tweets from search results"""
        try:
            results = []
            
            # Look for tweet-like elements in search results
            tweet_elements = soup.select('article, [data-testid="tweet"], .tweet')
            
            for elem in tweet_elements[:5]:  # Limit results
                content = elem.get_text().strip()
                
                if len(content) > 20 and keyword.lower() in content.lower():
                    results.append(CrawlResult(
                        url=f"{self.base_url}/search?q={quote(keyword)}",
                        title=f"Tweet about {keyword}",
                        content=content,
                        post_date=datetime.now(timezone.utc),
                        source_type=self.get_source_type(),
                        success=True
                    ))
            
            return results
            
        except Exception as e:
            logger.error(f"Error extracting search results: {e}")
            return []
    
    def _is_relevant_content(self, content: str) -> bool:
        """Check if content is relevant to oil & gas"""
        content_lower = content.lower()
        
        relevant_terms = [
            'oil', 'gas', 'drilling', 'well', 'permit', 'completion',
            'frac', 'horizontal', 'vertical', 'shale', 'unconventional',
            'permian', 'eagle ford', 'bakken', 'marcellus', 'haynesville',
            'operator', 'rig', 'production', 'barrel', 'mcf', 'boe',
            'upstream', 'midstream', 'downstream', 'pipeline', 'refinery'
        ]
        
        return any(term in content_lower for term in relevant_terms)
    
    def _classify_author(self, username: str) -> str:
        """Classify author type based on username"""
        username_lower = username.lower()
        
        # Corporate accounts
        corporate_indicators = [
            'resources', 'energy', 'oil', 'gas', 'petroleum', 'corp',
            'company', 'inc', 'llc', 'exploration', 'production'
        ]
        
        # Media accounts  
        media_indicators = [
            'news', 'times', 'journal', 'magazine', 'media', 'press',
            'reporter', 'editor', 'journalist'
        ]
        
        if any(indicator in username_lower for indicator in corporate_indicators):
            return 'corp'
        elif any(indicator in username_lower for indicator in media_indicators):
            return 'media'
        else:
            return 'independent'

class LinkedInCrawler(BaseCrawler):
    """
    LinkedIn crawler for public industry posts
    Note: Very limited without authentication, mainly for company pages
    """
    
    def __init__(self):
        super().__init__()
        self.base_url = "https://www.linkedin.com"
    
    def get_source_type(self) -> SourceType:
        return SourceType.SOCIAL
    
    async def crawl_recent(self, max_items: int = 5) -> List[CrawlResult]:
        """Crawl recent public LinkedIn posts"""
        # LinkedIn heavily restricts unauthenticated access
        # This would mainly work for company pages that are publicly viewable
        # For now, return empty list as implementation would be very limited
        logger.info("LinkedIn crawler: Limited functionality without authentication")
        return []

class GenericSocialCrawler(BaseCrawler):
    """Generic social media crawler for other platforms"""
    
    def __init__(self, platform_name: str, base_url: str):
        super().__init__()
        self.platform_name = platform_name
        self.base_url = base_url
    
    def get_source_type(self) -> SourceType:
        return SourceType.SOCIAL
    
    async def crawl_recent(self, max_items: int = 5) -> List[CrawlResult]:
        """Generic social media crawling"""
        # Placeholder for other social platforms
        return []
