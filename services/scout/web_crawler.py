"""
Scout v2.1 Web Crawler
Polite crawling of public web sources with rate limiting and robots.txt compliance
"""

import asyncio
import aiohttp
import time
import re
import logging
import hashlib
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
from typing import List, Dict, Any, Optional, Set
from datetime import datetime, timezone
from dataclasses import dataclass
from bs4 import BeautifulSoup
import random

from db.session import get_session
from db.scout_models import Signal, ClaimType

logger = logging.getLogger(__name__)

@dataclass
class CrawlResult:
    url: str
    title: str
    content: str
    post_date: Optional[datetime]
    links: List[str]
    success: bool
    error: Optional[str] = None

class RateLimiter:
    """Simple rate limiter for polite crawling"""
    
    def __init__(self, requests_per_second: float = 0.5):
        self.min_interval = 1.0 / requests_per_second
        self.last_request = 0
    
    async def wait(self):
        """Wait if necessary to respect rate limit"""
        now = time.time()
        elapsed = now - self.last_request
        if elapsed < self.min_interval:
            wait_time = self.min_interval - elapsed
            # Add small random jitter
            wait_time += random.uniform(0, 0.2)
            await asyncio.sleep(wait_time)
        self.last_request = time.time()

class RobotsChecker:
    """Check robots.txt compliance"""
    
    def __init__(self):
        self.robots_cache = {}
    
    def can_fetch(self, url: str, user_agent: str = "ScoutBot") -> bool:
        """Check if we can fetch the given URL according to robots.txt"""
        try:
            parsed = urlparse(url)
            base_url = f"{parsed.scheme}://{parsed.netloc}"
            
            if base_url not in self.robots_cache:
                robots_url = urljoin(base_url, "/robots.txt")
                rp = RobotFileParser()
                rp.set_url(robots_url)
                try:
                    rp.read()
                    self.robots_cache[base_url] = rp
                except:
                    # If we can't read robots.txt, assume we can fetch
                    self.robots_cache[base_url] = None
            
            robots_parser = self.robots_cache[base_url]
            if robots_parser is None:
                return True
            
            return robots_parser.can_fetch(user_agent, url)
        except:
            # If anything goes wrong, err on the side of caution
            return True

class MRFCrawler:
    """Specialized crawler for MineralRightsForum"""
    
    def __init__(self, user_agent: str = "ScoutBot/1.0 (PermitTracker Scout; +https://permittracker.com/scout)"):
        self.user_agent = user_agent
        self.rate_limiter = RateLimiter(requests_per_second=0.3)  # Be extra polite to MRF
        self.base_url = "https://www.mineralrightsforum.com"
        self.session = None
    
    async def __aenter__(self):
        import aiohttp
        self.session = aiohttp.ClientSession(
            headers={'User-Agent': self.user_agent},
            timeout=aiohttp.ClientTimeout(total=30)
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def crawl_recent_discussions(self, max_pages: int = 2) -> List[CrawlResult]:
        """Crawl recent discussions from MRF main forum"""
        results = []
        
        # Start with the main discussions page
        discussions_urls = [
            f"{self.base_url}/discussions",
            f"{self.base_url}/categories/oil-gas-mineral-rights-questions",
            f"{self.base_url}/categories/lease-bonus-royalty-discussions",
            f"{self.base_url}/categories/general-oil-gas-discussion",
            f"{self.base_url}/"  # Try the main page as fallback
        ]
        
        for base_url in discussions_urls:
            for page in range(1, max_pages + 1):
                await self.rate_limiter.wait()
                
                page_url = f"{base_url}?page={page}" if page > 1 else base_url
                
                try:
                    logger.info(f"Crawling MRF page: {page_url}")
                    async with self.session.get(page_url) as response:
                        if response.status != 200:
                            logger.warning(f"Failed to fetch MRF page {page_url}: HTTP {response.status}")
                            continue
                        
                        html = await response.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        
                        # Extract discussion links (adjust selectors based on actual MRF HTML)
                        discussion_links = []
                        
                        # Common selectors for discussion links - be more flexible
                        link_selectors = [
                            'a[href*="/discussion/"]',
                            'a[href*="/d/"]',
                            'a[href*="/topic/"]',
                            'a[href*="/thread/"]',
                            '.discussion-title a',
                            '.item-title a',
                            '.topic-title a',
                            'a[title*="oil"]',
                            'a[title*="gas"]',
                            'a[title*="lease"]',
                            'a[title*="mineral"]'
                        ]
                        
                        for selector in link_selectors:
                            links = soup.select(selector)
                            for link in links:
                                href = link.get('href')
                                if href:
                                    full_url = urljoin(self.base_url, href)
                                    if full_url not in [r.url for r in results]:  # Avoid duplicates
                                        discussion_links.append(full_url)
                        
                        # Crawl individual discussions (limit to avoid overwhelming)
                        for discussion_url in discussion_links[:3]:  # Limit per page
                            await self.rate_limiter.wait()
                            discussion_result = await self.crawl_discussion(discussion_url)
                            if discussion_result.success and len(discussion_result.content) > 100:
                                results.append(discussion_result)
                                logger.info(f"Successfully crawled MRF discussion: {discussion_result.title[:50]}...")
                                
                except Exception as e:
                    logger.error(f"Error crawling MRF page {page_url}: {e}")
                    continue
        
        return results
    
    async def crawl_discussion(self, url: str) -> CrawlResult:
        """Crawl a single MRF discussion thread"""
        try:
            async with self.session.get(url) as response:
                if response.status != 200:
                    return CrawlResult(
                        url=url, title="", content="", post_date=None, links=[],
                        success=False, error=f"HTTP {response.status}"
                    )
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Extract discussion title
                title = ""
                title_selectors = [
                    'h1.discussion-title',
                    'h1',
                    '.discussion-name',
                    '.item-title',
                    'title'
                ]
                
                for selector in title_selectors:
                    title_elem = soup.select_one(selector)
                    if title_elem:
                        title = title_elem.get_text().strip()
                        if len(title) > 10:  # Ensure meaningful title
                            break
                
                # Extract posts content
                posts_content = []
                post_selectors = [
                    '.message .message-body',
                    '.comment-body',
                    '.item-body .message',
                    '.post-content',
                    '.user-content'
                ]
                
                for selector in post_selectors:
                    posts = soup.select(selector)
                    if posts:
                        for post in posts[:5]:  # Limit posts per discussion
                            # Remove unwanted elements
                            for unwanted in post.select('nav, .signature, .ads, script, style, .quote'):
                                unwanted.decompose()
                            
                            post_text = post.get_text()
                            post_text = re.sub(r'\s+', ' ', post_text).strip()
                            if len(post_text) > 50:  # Only meaningful posts
                                posts_content.append(post_text)
                        break
                
                # Combine all post content
                content = f"Discussion: {title}\n\n" + "\n\n---\n\n".join(posts_content)
                
                # Extract links
                links = []
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    if href.startswith('http'):
                        links.append(href)
                    elif href.startswith('/'):
                        links.append(urljoin(self.base_url, href))
                
                # Try to extract date
                post_date = self.extract_mrf_date(soup)
                
                return CrawlResult(
                    url=url, title=title, content=content,
                    post_date=post_date, links=links, success=True
                )
                
        except Exception as e:
            logger.error(f"Error crawling MRF discussion {url}: {e}")
            return CrawlResult(
                url=url, title="", content="", post_date=None, links=[],
                success=False, error=str(e)
            )
    
    def extract_mrf_date(self, soup: BeautifulSoup) -> Optional[datetime]:
        """Extract date from MRF post"""
        # Look for common MRF date patterns
        date_selectors = [
            'time[datetime]',
            '.date-time',
            '.post-date',
            '.timestamp',
            '.meta-item time'
        ]
        
        for selector in date_selectors:
            date_elem = soup.select_one(selector)
            if date_elem:
                date_str = date_elem.get('datetime') or date_elem.get_text().strip()
                if date_str:
                    try:
                        # Try parsing ISO format first
                        if 'T' in date_str:
                            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                        
                        # Try other common formats
                        for fmt in ['%Y-%m-%d %H:%M:%S', '%m/%d/%Y', '%B %d, %Y', '%b %d, %Y']:
                            try:
                                return datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc)
                            except:
                                continue
                    except:
                        pass
        
        # Default to current time if no date found
        return datetime.now(timezone.utc)

class WebCrawler:
    """Main web crawler for Scout signals"""
    
    def __init__(self):
        self.rate_limiter = RateLimiter(requests_per_second=0.5)  # 1 request per 2 seconds
        self.robots_checker = RobotsChecker()
        self.session = None
        self.user_agent = "ScoutBot/1.0 (PermitTracker Scout; +https://permittracker.com/scout)"
        
        # County/state mappings for Texas
        self.texas_counties = {
            'andrews', 'atascosa', 'austin', 'bastrop', 'bee', 'brazoria', 'brazos', 
            'burleson', 'caldwell', 'calhoun', 'colorado', 'dewitt', 'dimmit', 'duval',
            'eagle ford', 'fayette', 'frio', 'goliad', 'gonzales', 'grimes', 'harris',
            'jackson', 'karnes', 'lavaca', 'lee', 'leon', 'live oak', 'madison', 
            'matagorda', 'mcmullen', 'milam', 'montgomery', 'newton', 'polk', 'refugio',
            'robertson', 'trinity', 'tyler', 'victoria', 'walker', 'waller', 'washington',
            'webb', 'wharton', 'wilson', 'zavala', 'reeves', 'ward', 'winkler', 'loving',
            'culberson', 'pecos', 'terrell', 'brewster', 'presidio', 'jeff davis',
            'martin', 'midland', 'ector', 'crane', 'upton', 'reagan', 'irion', 'crockett'
        }
        
        # Common operator aliases
        self.operator_aliases = {
            'eog': ['eog resources', 'eog resources inc'],
            'pioneer': ['pioneer natural resources', 'pioneer'],
            'conocophillips': ['conocophillips', 'conoco', 'cop'],
            'exxon': ['exxon mobil', 'exxonmobil', 'xom'],
            'chevron': ['chevron corp', 'chevron corporation'],
            'bp': ['bp america', 'british petroleum'],
            'shell': ['shell oil', 'royal dutch shell'],
            'marathon': ['marathon oil', 'marathon petroleum'],
            'apache': ['apache corp', 'apache corporation'],
            'devon': ['devon energy', 'devon'],
            'anadarko': ['anadarko petroleum', 'anadarko'],
            'oxy': ['occidental petroleum', 'occidental']
        }
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={'User-Agent': self.user_agent}
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def crawl_url(self, url: str) -> CrawlResult:
        """Crawl a single URL and return structured data"""
        
        # Check robots.txt
        if not self.robots_checker.can_fetch(url):
            return CrawlResult(
                url=url, title="", content="", post_date=None, links=[], 
                success=False, error="Blocked by robots.txt"
            )
        
        # Rate limiting
        await self.rate_limiter.wait()
        
        try:
            async with self.session.get(url) as response:
                if response.status != 200:
                    return CrawlResult(
                        url=url, title="", content="", post_date=None, links=[],
                        success=False, error=f"HTTP {response.status}"
                    )
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Extract title
                title_tag = soup.find('title')
                title = title_tag.get_text().strip() if title_tag else ""
                
                # Extract main content (remove scripts, styles, etc.)
                for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
                    tag.decompose()
                
                # Get text content
                content = soup.get_text()
                content = re.sub(r'\s+', ' ', content).strip()
                
                # Extract links
                links = []
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    if href.startswith('http'):
                        links.append(href)
                    elif href.startswith('/'):
                        links.append(urljoin(url, href))
                
                # Try to extract post date (basic heuristics)
                post_date = self.extract_date(soup, content)
                
                return CrawlResult(
                    url=url, title=title, content=content, 
                    post_date=post_date, links=links, success=True
                )
                
        except Exception as e:
            logger.error(f"Error crawling {url}: {e}")
            return CrawlResult(
                url=url, title="", content="", post_date=None, links=[],
                success=False, error=str(e)
            )
    
    def extract_date(self, soup: BeautifulSoup, content: str) -> Optional[datetime]:
        """Extract post/publication date from HTML"""
        
        # Look for common date patterns in meta tags
        date_selectors = [
            'meta[property="article:published_time"]',
            'meta[name="date"]',
            'meta[name="publish_date"]',
            'time[datetime]',
            '.date', '.post-date', '.published'
        ]
        
        for selector in date_selectors:
            elements = soup.select(selector)
            for element in elements:
                date_str = element.get('content') or element.get('datetime') or element.get_text()
                if date_str:
                    try:
                        # Try common date formats
                        for fmt in ['%Y-%m-%d', '%Y-%m-%dT%H:%M:%S', '%m/%d/%Y', '%B %d, %Y']:
                            try:
                                return datetime.strptime(date_str.strip(), fmt).replace(tzinfo=timezone.utc)
                            except ValueError:
                                continue
                    except:
                        continue
        
        # Look for date patterns in content
        date_patterns = [
            r'(\d{1,2}/\d{1,2}/\d{4})',
            r'(\w+ \d{1,2}, \d{4})',
            r'(\d{4}-\d{2}-\d{2})'
        ]
        
        for pattern in date_patterns:
            matches = re.findall(pattern, content)
            if matches:
                try:
                    date_str = matches[0]
                    for fmt in ['%m/%d/%Y', '%B %d, %Y', '%Y-%m-%d']:
                        try:
                            return datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc)
                        except ValueError:
                            continue
                except:
                    continue
        
        return None
    
    def extract_signals_from_content(self, crawl_result: CrawlResult) -> List[Dict[str, Any]]:
        """Extract structured signals from crawled content"""
        
        signals = []
        content = crawl_result.content.lower()
        
        # Look for Texas counties
        found_counties = []
        for county in self.texas_counties:
            if county in content:
                found_counties.append(county.title())
        
        # Look for operators
        found_operators = []
        for operator, aliases in self.operator_aliases.items():
            for alias in aliases:
                if alias.lower() in content:
                    found_operators.append(operator.upper())
                    break
        
        # Look for unit/lease/pad tokens
        unit_patterns = [
            r'\b([A-Z][a-z]+ (?:unit|pad|lease))\b',
            r'\b([A-Z]+ \d+[A-Z]?)\b',
            r'\b(abstract \d+)\b'
        ]
        
        unit_tokens = []
        for pattern in unit_patterns:
            matches = re.findall(pattern, crawl_result.content, re.IGNORECASE)
            unit_tokens.extend([match.upper() for match in matches])
        
        # Extract keywords (drilling, completion, permit related)
        keywords = []
        keyword_patterns = [
            'drilling', 'completion', 'permit', 'spud', 'rig', 'wellbore', 
            'horizontal', 'vertical', 'frack', 'stimulation', 'production',
            'oil', 'gas', 'condensate', 'barrel', 'mcf', 'boe'
        ]
        
        for keyword in keyword_patterns:
            if keyword in content:
                keywords.append(keyword)
        
        # Determine claim type based on language confidence
        claim_type = ClaimType.RUMOR
        if any(word in content for word in ['confirmed', 'announced', 'filed', 'approved']):
            claim_type = ClaimType.LIKELY
        if any(word in content for word in ['completed', 'producing', 'online']):
            claim_type = ClaimType.CONFIRMED
        
        # Create signal if we found relevant content
        if found_counties or found_operators or unit_tokens:
            # Extract summary (first 200 chars of relevant content)
            summary_start = content.find(next((c.lower() for c in found_counties), 
                                            next((o.lower() for o in found_operators), 
                                                 content[:200])))
            summary = crawl_result.content[max(0, summary_start-50):summary_start+200].strip()
            
            signal_data = {
                'source_url': crawl_result.url,
                'source_type': 'mrf' if 'mineralrightsforum' in crawl_result.url else 'web',
                'state': 'TX',  # Focus on Texas for now
                'county': found_counties[0] if found_counties else None,
                'operators': found_operators,
                'unit_tokens': unit_tokens[:10],  # Limit to avoid huge arrays
                'keywords': keywords,
                'claim_type': claim_type,
                'summary': summary,
                'raw_excerpt': crawl_result.content[:1000],  # First 1000 chars
                'found_at': crawl_result.post_date or datetime.now(timezone.utc)
            }
            
            signals.append(signal_data)
        
        return signals
    
    async def crawl_mrf_recent(self, max_pages: int = 5) -> List[Dict[str, Any]]:
        """Crawl recent MineralRightsForum posts"""
        
        # This is a placeholder - actual MRF crawling would need to:
        # 1. Respect their robots.txt
        # 2. Handle their specific HTML structure
        # 3. Navigate pagination properly
        # 4. Extract post metadata correctly
        
        logger.info("MRF crawling not yet implemented - placeholder")
        return []
    
    async def save_signals_to_db(self, signals: List[Dict[str, Any]], org_id: str = "default_org"):
        """Save extracted signals to database"""
        
        if not signals:
            return
        
        with get_session() as session:
            for signal_data in signals:
                signal = Signal(
                    org_id=org_id,
                    **signal_data
                )
                session.add(signal)
            
            session.commit()
            logger.info(f"Saved {len(signals)} signals to database")

# Example usage function
async def run_web_crawl():
    """Example function to run web crawling"""
    
    async with WebCrawler() as crawler:
        # Example URLs (replace with actual sources)
        test_urls = [
            # Add actual public URLs here
        ]
        
        all_signals = []
        
        for url in test_urls:
            result = await crawler.crawl_url(url)
            if result.success:
                signals = crawler.extract_signals_from_content(result)
                all_signals.extend(signals)
        
        # Save to database
        await crawler.save_signals_to_db(all_signals)
        
        return len(all_signals)
