"""
Scout v2.2 Forum Crawler
Handles MineralRightsForum and other public industry forums
"""

import re
import logging
from datetime import datetime, timezone
from typing import List, Optional
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

from .base_crawler import BaseCrawler, CrawlResult
from db.scout_models import SourceType

logger = logging.getLogger(__name__)

class MRFCrawler(BaseCrawler):
    """MineralRightsForum crawler - enhanced from v2.1"""
    
    def __init__(self):
        super().__init__()
        self.base_url = "https://www.mineralrightsforum.com"
        self.categories = [
            "/categories/texas-oil-gas-mineral-rights",
            "/categories/general-oil-gas-discussion", 
            "/categories/oil-gas-leasing-questions",
            "/categories/royalty-questions",
            "/categories/drilling-completion-questions"
        ]
    
    def get_source_type(self) -> SourceType:
        return SourceType.FORUM
    
    async def crawl_recent(self, max_items: int = 20) -> List[CrawlResult]:
        """Crawl recent discussions from MRF"""
        results = []
        
        try:
            for category in self.categories:
                if len(results) >= max_items:
                    break
                
                category_url = urljoin(self.base_url, category)
                html = await self.fetch_url(category_url)
                
                if not html:
                    continue
                
                discussion_links = self._extract_discussion_links(html, category_url)
                
                for link in discussion_links[:5]:  # Limit per category
                    if len(results) >= max_items:
                        break
                    
                    result = await self._crawl_discussion(link)
                    if result and result.success:
                        results.append(result)
            
            logger.info(f"MRF crawler found {len(results)} discussions")
            return results
            
        except Exception as e:
            logger.error(f"Error in MRF crawler: {e}")
            return []
    
    def _extract_discussion_links(self, html: str, base_url: str) -> List[str]:
        """Extract discussion links from category page"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            links = []
            
            # Look for discussion links - MRF uses various selectors
            selectors = [
                'a[href*="/discussion/"]',
                '.DataList-Discussion a',
                '.ItemDiscussion a',
                'h3 a',
                '.Title a'
            ]
            
            for selector in selectors:
                for link in soup.select(selector):
                    href = link.get('href')
                    if href and '/discussion/' in href:
                        full_url = urljoin(base_url, href)
                        if full_url not in links:
                            links.append(full_url)
            
            return links[:10]  # Limit to prevent overload
            
        except Exception as e:
            logger.error(f"Error extracting discussion links: {e}")
            return []
    
    async def _crawl_discussion(self, url: str) -> Optional[CrawlResult]:
        """Crawl individual discussion thread"""
        try:
            html = await self.fetch_url(url)
            if not html:
                return None
            
            soup = BeautifulSoup(html, 'html.parser')
            
            # Extract title
            title_selectors = ['h1', '.PageTitle', '.Discussion-Title', 'title']
            title = ""
            for selector in title_selectors:
                title_elem = soup.select_one(selector)
                if title_elem:
                    title = title_elem.get_text().strip()
                    break
            
            # Extract main post content
            content_selectors = [
                '.Message-Body',
                '.UserContent',
                '.Post-Body',
                '.MessageBody',
                'article'
            ]
            
            content = ""
            for selector in content_selectors:
                content_elem = soup.select_one(selector)
                if content_elem:
                    content = content_elem.get_text().strip()
                    break
            
            # If no specific content found, get general text
            if not content:
                title, content = self.extract_text_content(html, url)
            
            # Extract date
            post_date = self._extract_post_date(soup)
            
            # Extract links
            links = self.extract_links(html, url)
            
            return CrawlResult(
                url=url,
                title=title[:90] if title else "MRF Discussion",
                content=content,
                post_date=post_date,
                links=links,
                source_type=self.get_source_type(),
                success=True
            )
            
        except Exception as e:
            logger.error(f"Error crawling discussion {url}: {e}")
            return CrawlResult(
                url=url,
                title="",
                content="",
                post_date=None,
                source_type=self.get_source_type(),
                success=False,
                error=str(e)
            )
    
    def _extract_post_date(self, soup: BeautifulSoup) -> Optional[datetime]:
        """Extract post date from discussion page"""
        try:
            # Common date selectors for forums
            date_selectors = [
                'time[datetime]',
                '.DateCreated',
                '.PostDate',
                '.Message-Date',
                '[data-time]'
            ]
            
            for selector in date_selectors:
                date_elem = soup.select_one(selector)
                if date_elem:
                    # Try datetime attribute first
                    if date_elem.get('datetime'):
                        return datetime.fromisoformat(date_elem['datetime'].replace('Z', '+00:00'))
                    
                    # Try data-time attribute
                    if date_elem.get('data-time'):
                        timestamp = int(date_elem['data-time'])
                        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
                    
                    # Try parsing text content
                    date_text = date_elem.get_text().strip()
                    if date_text:
                        return self._parse_date_text(date_text)
            
            return None
            
        except Exception as e:
            logger.error(f"Error extracting post date: {e}")
            return None
    
    def _parse_date_text(self, date_text: str) -> Optional[datetime]:
        """Parse various date text formats"""
        try:
            # Remove common prefixes
            date_text = re.sub(r'^(posted|created|on)\s+', '', date_text, flags=re.IGNORECASE)
            
            # Try common formats
            formats = [
                '%B %d, %Y',
                '%b %d, %Y', 
                '%m/%d/%Y',
                '%Y-%m-%d',
                '%d %B %Y',
                '%d %b %Y'
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(date_text, fmt).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
            
            return None
            
        except Exception as e:
            logger.error(f"Error parsing date text '{date_text}': {e}")
            return None

class GenericForumCrawler(BaseCrawler):
    """Generic forum crawler for other industry forums"""
    
    def __init__(self, base_url: str, forum_name: str):
        super().__init__()
        self.base_url = base_url
        self.forum_name = forum_name
    
    def get_source_type(self) -> SourceType:
        return SourceType.FORUM
    
    async def crawl_recent(self, max_items: int = 10) -> List[CrawlResult]:
        """Crawl recent posts from generic forum"""
        # Implementation would be similar to MRF but more generic
        # For now, return empty list as we focus on MRF
        return []
