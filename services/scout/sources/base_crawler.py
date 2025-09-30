"""
Scout v2.2 Base Crawler
Abstract base class for all source adapters with rate limiting and robots.txt compliance
"""

import asyncio
import aiohttp
import time
import random
import logging
from abc import ABC, abstractmethod
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from dataclasses import dataclass
from bs4 import BeautifulSoup

from db.scout_models import SourceType

logger = logging.getLogger(__name__)

@dataclass
class CrawlResult:
    """Standardized result from any source crawler"""
    url: str
    title: str
    content: str
    post_date: Optional[datetime]
    author: Optional[str] = None
    author_type: Optional[str] = None  # corp|media|independent
    links: List[str] = None
    source_type: SourceType = SourceType.OTHER
    success: bool = True
    error: Optional[str] = None
    
    def __post_init__(self):
        if self.links is None:
            self.links = []

class RateLimiter:
    """Polite rate limiter with jitter"""
    
    def __init__(self, requests_per_second: float = 1.0):
        self.min_interval = 1.0 / requests_per_second
        self.last_request = 0
        self.domain_last_request = {}
    
    async def wait(self, domain: Optional[str] = None):
        """Wait if necessary to respect rate limit"""
        now = time.time()
        
        # Global rate limit
        elapsed = now - self.last_request
        if elapsed < self.min_interval:
            wait_time = self.min_interval - elapsed
            wait_time += random.uniform(0, 0.2)  # Add jitter
            await asyncio.sleep(wait_time)
        
        # Per-domain rate limit (stricter)
        if domain:
            domain_elapsed = now - self.domain_last_request.get(domain, 0)
            if domain_elapsed < self.min_interval * 1.5:  # 1.5x stricter per domain
                wait_time = (self.min_interval * 1.5) - domain_elapsed
                wait_time += random.uniform(0, 0.3)
                await asyncio.sleep(wait_time)
            self.domain_last_request[domain] = time.time()
        
        self.last_request = time.time()

class RobotsChecker:
    """Check robots.txt compliance"""
    
    def __init__(self):
        self.robots_cache = {}
    
    async def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        """Check if URL can be fetched according to robots.txt"""
        try:
            parsed = urlparse(url)
            domain = f"{parsed.scheme}://{parsed.netloc}"
            
            if domain not in self.robots_cache:
                robots_url = urljoin(domain, "/robots.txt")
                
                async with aiohttp.ClientSession() as session:
                    try:
                        async with session.get(robots_url, timeout=5) as response:
                            if response.status == 200:
                                robots_txt = await response.text()
                                rp = RobotFileParser()
                                rp.set_url(robots_url)
                                rp.read()
                                # Parse the robots.txt content
                                for line in robots_txt.split('\n'):
                                    rp.read()
                                self.robots_cache[domain] = rp
                            else:
                                # No robots.txt or error - allow by default
                                self.robots_cache[domain] = None
                    except:
                        # Network error - allow by default
                        self.robots_cache[domain] = None
            
            robots_parser = self.robots_cache[domain]
            if robots_parser:
                return robots_parser.can_fetch(user_agent, url)
            return True
            
        except Exception as e:
            logger.warning(f"Error checking robots.txt for {url}: {e}")
            return True  # Err on the side of caution

class BaseCrawler(ABC):
    """Abstract base class for all Scout source crawlers"""
    
    def __init__(self, user_agent: str = "ScoutBot/2.2 (PermitTracker Scout; +https://permittracker.com/scout)"):
        self.user_agent = user_agent
        self.rate_limiter = RateLimiter(requests_per_second=1.0)
        self.robots_checker = RobotsChecker()
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            headers={'User-Agent': self.user_agent},
            timeout=aiohttp.ClientTimeout(total=30)
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def fetch_url(self, url: str) -> Optional[str]:
        """Fetch URL content with rate limiting and robots.txt check"""
        # Check robots.txt
        if not await self.robots_checker.can_fetch(url, self.user_agent):
            logger.info(f"Robots.txt disallows crawling {url}")
            return None
        
        # Rate limit
        domain = urlparse(url).netloc
        await self.rate_limiter.wait(domain)
        
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    logger.warning(f"HTTP {response.status} for {url}")
                    return None
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
    
    def extract_text_content(self, html: str, url: str) -> tuple[str, str]:
        """Extract clean text and title from HTML"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Extract title
            title_tag = soup.find('title')
            title = title_tag.get_text().strip() if title_tag else ""
            
            # Remove unwanted elements
            for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'aside', 'advertisement']):
                tag.decompose()
            
            # Get main content
            content = soup.get_text()
            content = ' '.join(content.split())  # Normalize whitespace
            
            return title, content
            
        except Exception as e:
            logger.error(f"Error parsing HTML from {url}: {e}")
            return "", ""
    
    def extract_links(self, html: str, base_url: str) -> List[str]:
        """Extract all links from HTML"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            links = []
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.startswith('http'):
                    links.append(href)
                elif href.startswith('/'):
                    links.append(urljoin(base_url, href))
            
            return links
            
        except Exception as e:
            logger.error(f"Error extracting links from {base_url}: {e}")
            return []
    
    @abstractmethod
    async def crawl_recent(self, max_items: int = 10) -> List[CrawlResult]:
        """Crawl recent content from this source"""
        pass
    
    @abstractmethod
    def get_source_type(self) -> SourceType:
        """Return the source type for this crawler"""
        pass
