"""
Scout v2.2 News & PR Crawler
Handles industry news sites, company press releases, and blogs
"""

import re
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

from .base_crawler import BaseCrawler, CrawlResult
from db.scout_models import SourceType

logger = logging.getLogger(__name__)

class NewsCrawler(BaseCrawler):
    """Industry news sites crawler"""
    
    def __init__(self):
        super().__init__()
        self.news_sources = {
            # Major oil & gas news sites
            "https://www.oilandgasinvestor.com": {
                "name": "Oil & Gas Investor",
                "rss": "/rss",
                "article_selector": "article",
                "title_selector": "h1, .article-title",
                "content_selector": ".article-content, .entry-content"
            },
            "https://www.hartenergy.com": {
                "name": "Hart Energy", 
                "rss": "/rss.xml",
                "article_selector": "article",
                "title_selector": "h1, .article-title",
                "content_selector": ".article-body, .content"
            },
            "https://www.rigzone.com": {
                "name": "Rigzone",
                "rss": "/news/rss",
                "article_selector": "article, .news-item",
                "title_selector": "h1, .headline",
                "content_selector": ".article-content, .news-content"
            }
        }
    
    def get_source_type(self) -> SourceType:
        return SourceType.NEWS
    
    async def crawl_recent(self, max_items: int = 15) -> List[CrawlResult]:
        """Crawl recent news articles"""
        results = []
        
        for base_url, config in self.news_sources.items():
            if len(results) >= max_items:
                break
            
            try:
                # Try RSS first, then fallback to homepage scraping
                rss_results = await self._crawl_rss(base_url + config["rss"], config)
                if rss_results:
                    results.extend(rss_results[:5])
                else:
                    # Fallback to homepage scraping
                    homepage_results = await self._crawl_homepage(base_url, config)
                    results.extend(homepage_results[:3])
                    
            except Exception as e:
                logger.error(f"Error crawling news from {base_url}: {e}")
                continue
        
        logger.info(f"News crawler found {len(results)} articles")
        return results[:max_items]
    
    async def _crawl_rss(self, rss_url: str, config: Dict) -> List[CrawlResult]:
        """Crawl RSS feed for articles"""
        try:
            html = await self.fetch_url(rss_url)
            if not html:
                return []
            
            # Parse RSS/XML
            soup = BeautifulSoup(html, 'xml')
            items = soup.find_all('item')
            
            results = []
            for item in items[:5]:  # Limit per source
                try:
                    title = item.find('title').get_text().strip() if item.find('title') else ""
                    link = item.find('link').get_text().strip() if item.find('link') else ""
                    description = item.find('description').get_text().strip() if item.find('description') else ""
                    
                    # Parse date
                    pub_date = None
                    if item.find('pubDate'):
                        pub_date = self._parse_rss_date(item.find('pubDate').get_text())
                    
                    if link and title:
                        # Fetch full article content
                        article_content = await self._fetch_article_content(link, config)
                        
                        results.append(CrawlResult(
                            url=link,
                            title=title[:90],
                            content=article_content or description,
                            post_date=pub_date,
                            source_type=self.get_source_type(),
                            success=True
                        ))
                        
                except Exception as e:
                    logger.error(f"Error processing RSS item: {e}")
                    continue
            
            return results
            
        except Exception as e:
            logger.error(f"Error crawling RSS {rss_url}: {e}")
            return []
    
    async def _crawl_homepage(self, base_url: str, config: Dict) -> List[CrawlResult]:
        """Fallback: crawl homepage for article links"""
        try:
            html = await self.fetch_url(base_url)
            if not html:
                return []
            
            soup = BeautifulSoup(html, 'html.parser')
            
            # Find article links
            article_links = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if any(keyword in href.lower() for keyword in ['article', 'news', 'story']):
                    full_url = urljoin(base_url, href)
                    if full_url not in article_links:
                        article_links.append(full_url)
            
            results = []
            for link in article_links[:3]:  # Limit to prevent overload
                content = await self._fetch_article_content(link, config)
                if content:
                    title = self._extract_title_from_url(link)
                    results.append(CrawlResult(
                        url=link,
                        title=title[:90],
                        content=content,
                        post_date=datetime.now(timezone.utc),
                        source_type=self.get_source_type(),
                        success=True
                    ))
            
            return results
            
        except Exception as e:
            logger.error(f"Error crawling homepage {base_url}: {e}")
            return []
    
    async def _fetch_article_content(self, url: str, config: Dict) -> Optional[str]:
        """Fetch full article content"""
        try:
            html = await self.fetch_url(url)
            if not html:
                return None
            
            soup = BeautifulSoup(html, 'html.parser')
            
            # Try configured content selector
            content_elem = soup.select_one(config.get("content_selector", "article"))
            if content_elem:
                return content_elem.get_text().strip()
            
            # Fallback to general content extraction
            _, content = self.extract_text_content(html, url)
            return content
            
        except Exception as e:
            logger.error(f"Error fetching article content from {url}: {e}")
            return None
    
    def _parse_rss_date(self, date_str: str) -> Optional[datetime]:
        """Parse RSS date string"""
        try:
            # Common RSS date formats
            formats = [
                '%a, %d %b %Y %H:%M:%S %z',
                '%a, %d %b %Y %H:%M:%S GMT',
                '%Y-%m-%dT%H:%M:%S%z',
                '%Y-%m-%d %H:%M:%S'
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
            
            return None
            
        except Exception as e:
            logger.error(f"Error parsing RSS date '{date_str}': {e}")
            return None
    
    def _extract_title_from_url(self, url: str) -> str:
        """Extract title from URL as fallback"""
        try:
            path = urlparse(url).path
            title = path.split('/')[-1].replace('-', ' ').replace('_', ' ')
            return title.title()
        except:
            return "News Article"

class PRCrawler(BaseCrawler):
    """Company press release crawler"""
    
    def __init__(self):
        super().__init__()
        self.pr_sources = {
            # Major operators' PR pages
            "https://www.eogresources.com": "/news-releases",
            "https://www.conocophillips.com": "/news-releases", 
            "https://www.chevron.com": "/newsroom",
            "https://www.exxonmobil.com": "/news",
            "https://www.pioneernaturalresources.com": "/news-releases"
        }
    
    def get_source_type(self) -> SourceType:
        return SourceType.PR
    
    async def crawl_recent(self, max_items: int = 10) -> List[CrawlResult]:
        """Crawl recent press releases"""
        results = []
        
        for base_url, pr_path in self.pr_sources.items():
            if len(results) >= max_items:
                break
            
            try:
                pr_url = base_url + pr_path
                html = await self.fetch_url(pr_url)
                
                if not html:
                    continue
                
                pr_results = await self._extract_press_releases(html, pr_url)
                results.extend(pr_results[:2])  # Limit per company
                
            except Exception as e:
                logger.error(f"Error crawling PR from {base_url}: {e}")
                continue
        
        logger.info(f"PR crawler found {len(results)} press releases")
        return results[:max_items]
    
    async def _extract_press_releases(self, html: str, base_url: str) -> List[CrawlResult]:
        """Extract press releases from PR page"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            # Common PR page selectors
            pr_selectors = [
                '.press-release',
                '.news-item',
                '.release-item',
                'article',
                '.news-article'
            ]
            
            for selector in pr_selectors:
                items = soup.select(selector)
                if items:
                    for item in items[:3]:  # Limit per selector
                        result = await self._process_pr_item(item, base_url)
                        if result:
                            results.append(result)
                    break  # Use first successful selector
            
            return results
            
        except Exception as e:
            logger.error(f"Error extracting press releases: {e}")
            return []
    
    async def _process_pr_item(self, item, base_url: str) -> Optional[CrawlResult]:
        """Process individual press release item"""
        try:
            # Extract title
            title_elem = item.find(['h1', 'h2', 'h3', 'h4', '.title', '.headline'])
            title = title_elem.get_text().strip() if title_elem else ""
            
            # Extract link
            link_elem = item.find('a', href=True)
            if link_elem:
                link = urljoin(base_url, link_elem['href'])
            else:
                return None
            
            # Extract date
            date_elem = item.find(['time', '.date', '.published'])
            post_date = None
            if date_elem:
                date_text = date_elem.get_text().strip()
                post_date = self._parse_date_text(date_text)
            
            # Fetch full content
            content = await self._fetch_pr_content(link)
            
            return CrawlResult(
                url=link,
                title=title[:90] if title else "Press Release",
                content=content or "",
                post_date=post_date,
                source_type=self.get_source_type(),
                success=True
            )
            
        except Exception as e:
            logger.error(f"Error processing PR item: {e}")
            return None
    
    async def _fetch_pr_content(self, url: str) -> Optional[str]:
        """Fetch full press release content"""
        try:
            html = await self.fetch_url(url)
            if not html:
                return None
            
            soup = BeautifulSoup(html, 'html.parser')
            
            # Try common PR content selectors
            content_selectors = [
                '.press-release-content',
                '.release-body',
                '.news-content',
                'article .content',
                '.entry-content'
            ]
            
            for selector in content_selectors:
                content_elem = soup.select_one(selector)
                if content_elem:
                    return content_elem.get_text().strip()
            
            # Fallback
            _, content = self.extract_text_content(html, url)
            return content
            
        except Exception as e:
            logger.error(f"Error fetching PR content from {url}: {e}")
            return None
    
    def _parse_date_text(self, date_text: str) -> Optional[datetime]:
        """Parse date text from PR pages"""
        try:
            # Clean up date text
            date_text = re.sub(r'[^\w\s,/-]', '', date_text)
            
            formats = [
                '%B %d, %Y',
                '%b %d, %Y',
                '%m/%d/%Y',
                '%Y-%m-%d',
                '%d %B %Y'
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(date_text, fmt).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
            
            return None
            
        except Exception as e:
            logger.error(f"Error parsing PR date '{date_text}': {e}")
            return None
