"""
Scout v2.2 Filing & Government Bulletin Crawler
Handles SEC/EDGAR filings and state O&G bulletins
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

class SECCrawler(BaseCrawler):
    """SEC/EDGAR filings crawler for oil & gas companies"""
    
    def __init__(self):
        super().__init__()
        self.base_url = "https://www.sec.gov"
        self.edgar_url = "https://www.sec.gov/edgar"
        
        # Major oil & gas company CIKs (Central Index Keys)
        self.company_ciks = {
            "0000034088": "ExxonMobil",
            "0000093410": "Chevron", 
            "0001018724": "ConocoPhillips",
            "0001090872": "EOG Resources",
            "0001038357": "Pioneer Natural Resources",
            "0001090727": "Devon Energy",
            "0001443066": "Diamondback Energy",
            "0001792580": "Coterra Energy"
        }
        
        # Relevant form types
        self.relevant_forms = [
            "8-K",    # Current reports (material events)
            "10-Q",   # Quarterly reports
            "10-K",   # Annual reports
            "DEF 14A" # Proxy statements
        ]
    
    def get_source_type(self) -> SourceType:
        return SourceType.FILING
    
    async def crawl_recent(self, max_items: int = 10) -> List[CrawlResult]:
        """Crawl recent SEC filings"""
        results = []
        
        for cik, company_name in list(self.company_ciks.items())[:3]:  # Limit companies
            if len(results) >= max_items:
                break
            
            try:
                company_results = await self._crawl_company_filings(cik, company_name)
                results.extend(company_results[:2])  # Limit per company
            except Exception as e:
                logger.error(f"Error crawling SEC filings for {company_name}: {e}")
                continue
        
        logger.info(f"SEC crawler found {len(results)} filings")
        return results[:max_items]
    
    async def _crawl_company_filings(self, cik: str, company_name: str) -> List[CrawlResult]:
        """Crawl recent filings for a specific company"""
        try:
            # Use SEC's company filings search
            search_url = f"{self.edgar_url}/browse/?CIK={cik}&owner=exclude&count=10"
            
            html = await self.fetch_url(search_url)
            if not html:
                return []
            
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            # Find filing table rows
            filing_rows = soup.select('table.tableFile2 tr')[1:]  # Skip header
            
            for row in filing_rows[:5]:  # Limit filings per company
                try:
                    cells = row.find_all('td')
                    if len(cells) < 4:
                        continue
                    
                    form_type = cells[0].get_text().strip()
                    description = cells[2].get_text().strip()
                    filing_date = cells[3].get_text().strip()
                    
                    # Check if it's a relevant form type
                    if not any(form in form_type for form in self.relevant_forms):
                        continue
                    
                    # Get filing URL
                    link_elem = cells[1].find('a')
                    if not link_elem:
                        continue
                    
                    filing_url = urljoin(self.base_url, link_elem['href'])
                    
                    # Parse filing date
                    post_date = self._parse_sec_date(filing_date)
                    
                    # Fetch filing content (summary)
                    content = await self._fetch_filing_summary(filing_url)
                    
                    results.append(CrawlResult(
                        url=filing_url,
                        title=f"{company_name} {form_type}: {description}"[:90],
                        content=content or description,
                        post_date=post_date,
                        author=company_name,
                        author_type="corp",
                        source_type=self.get_source_type(),
                        success=True
                    ))
                    
                except Exception as e:
                    logger.error(f"Error processing filing row: {e}")
                    continue
            
            return results
            
        except Exception as e:
            logger.error(f"Error crawling company filings for {company_name}: {e}")
            return []
    
    async def _fetch_filing_summary(self, filing_url: str) -> Optional[str]:
        """Fetch summary content from SEC filing"""
        try:
            html = await self.fetch_url(filing_url)
            if not html:
                return None
            
            soup = BeautifulSoup(html, 'html.parser')
            
            # Look for filing summary or business description
            summary_selectors = [
                '.FormGrouping',
                '.info',
                'div[style*="margin-left"]',
                'p'
            ]
            
            for selector in summary_selectors:
                elements = soup.select(selector)
                for elem in elements:
                    text = elem.get_text().strip()
                    if len(text) > 100 and any(keyword in text.lower() for keyword in 
                        ['oil', 'gas', 'drilling', 'production', 'exploration', 'well', 'permit']):
                        return text[:500]  # Limit length
            
            # Fallback to general text extraction
            _, content = self.extract_text_content(html, filing_url)
            return content[:500] if content else None
            
        except Exception as e:
            logger.error(f"Error fetching filing summary from {filing_url}: {e}")
            return None
    
    def _parse_sec_date(self, date_str: str) -> Optional[datetime]:
        """Parse SEC date format"""
        try:
            # SEC typically uses YYYY-MM-DD format
            return datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        except ValueError:
            try:
                # Try alternative format
                return datetime.strptime(date_str, '%m/%d/%Y').replace(tzinfo=timezone.utc)
            except ValueError:
                logger.error(f"Could not parse SEC date: {date_str}")
                return None

class TexasRRCCrawler(BaseCrawler):
    """Texas Railroad Commission bulletins and announcements"""
    
    def __init__(self):
        super().__init__()
        self.base_url = "https://www.rrc.texas.gov"
        self.news_url = f"{self.base_url}/news"
    
    def get_source_type(self) -> SourceType:
        return SourceType.GOV_BULLETIN
    
    async def crawl_recent(self, max_items: int = 5) -> List[CrawlResult]:
        """Crawl recent RRC announcements"""
        try:
            html = await self.fetch_url(self.news_url)
            if not html:
                return []
            
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            # Find news/announcement items
            news_selectors = [
                '.news-item',
                '.announcement',
                'article',
                '.press-release'
            ]
            
            for selector in news_selectors:
                items = soup.select(selector)
                if items:
                    for item in items[:max_items]:
                        result = await self._process_rrc_item(item)
                        if result:
                            results.append(result)
                    break
            
            logger.info(f"Texas RRC crawler found {len(results)} bulletins")
            return results[:max_items]
            
        except Exception as e:
            logger.error(f"Error crawling Texas RRC: {e}")
            return []
    
    async def _process_rrc_item(self, item) -> Optional[CrawlResult]:
        """Process individual RRC news item"""
        try:
            # Extract title
            title_elem = item.find(['h1', 'h2', 'h3', '.title', '.headline'])
            title = title_elem.get_text().strip() if title_elem else ""
            
            # Extract link
            link_elem = item.find('a', href=True)
            if link_elem:
                url = urljoin(self.base_url, link_elem['href'])
            else:
                return None
            
            # Extract date
            date_elem = item.find(['time', '.date', '.published'])
            post_date = None
            if date_elem:
                date_text = date_elem.get_text().strip()
                post_date = self._parse_gov_date(date_text)
            
            # Fetch full content
            content = await self._fetch_bulletin_content(url)
            
            return CrawlResult(
                url=url,
                title=title[:90] if title else "RRC Bulletin",
                content=content or "",
                post_date=post_date or datetime.now(timezone.utc),
                author="Texas Railroad Commission",
                author_type="gov",
                source_type=self.get_source_type(),
                success=True
            )
            
        except Exception as e:
            logger.error(f"Error processing RRC item: {e}")
            return None
    
    async def _fetch_bulletin_content(self, url: str) -> Optional[str]:
        """Fetch full bulletin content"""
        try:
            html = await self.fetch_url(url)
            if not html:
                return None
            
            soup = BeautifulSoup(html, 'html.parser')
            
            # Try common content selectors
            content_selectors = [
                '.content',
                '.news-content',
                'article .body',
                '.press-release-content'
            ]
            
            for selector in content_selectors:
                content_elem = soup.select_one(selector)
                if content_elem:
                    return content_elem.get_text().strip()
            
            # Fallback
            _, content = self.extract_text_content(html, url)
            return content
            
        except Exception as e:
            logger.error(f"Error fetching bulletin content from {url}: {e}")
            return None
    
    def _parse_gov_date(self, date_text: str) -> Optional[datetime]:
        """Parse government date formats"""
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
            logger.error(f"Error parsing government date '{date_text}': {e}")
            return None

class GenericGovCrawler(BaseCrawler):
    """Generic government bulletin crawler for other states"""
    
    def __init__(self, state: str, base_url: str):
        super().__init__()
        self.state = state
        self.base_url = base_url
    
    def get_source_type(self) -> SourceType:
        return SourceType.GOV_BULLETIN
    
    async def crawl_recent(self, max_items: int = 5) -> List[CrawlResult]:
        """Generic government bulletin crawling"""
        # Placeholder for other state agencies
        # Each state would need specific implementation
        return []
