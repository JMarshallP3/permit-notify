import logging
import requests
from bs4 import BeautifulSoup
from typing import Optional, List, Dict, Any
from requests.exceptions import RequestException, Timeout, ConnectionError
import time
import os
from datetime import datetime
from urllib.parse import urljoin, urlparse
import re

logger = logging.getLogger(__name__)

class Scraper:
    """
    Web scraper class for permit notification system.
    Scrapes permit data from RRC Texas and other permit websites.
    """
    
    def __init__(self, base_url: Optional[str] = None):
        """
        Initialize the scraper.
        
        Args:
            base_url: Base URL for scraping operations
        """
        self.base_url = base_url
        self.logger = logger
        self.session = requests.Session()
        
        # Configuration from environment variables with fallback defaults
        self.user_agent = os.getenv(
            'USER_AGENT', 
            'PermitNotifyBot/1.0 (contact: marshall@craatx.com)'
        )
        self.scrape_timeout = int(os.getenv('SCRAPE_TIMEOUT_SECONDS', '15'))
        
        # Set user agent header
        self.session.headers.update({
            'User-Agent': self.user_agent
        })
        
        self.logger.info(f"Scraper initialized with User-Agent: {self.user_agent}")
        self.logger.info(f"Scraper timeout: {self.scrape_timeout} seconds")
    
    def _abs_url(self, base_url: str, href: str) -> str:
        """
        Convert relative URL to absolute URL.
        
        Args:
            base_url: Base URL
            href: Relative or absolute URL
            
        Returns:
            Absolute URL
        """
        return urljoin(base_url, href)
    
    def _parse_date(self, date_str: str) -> Optional[str]:
        """
        Parse date string into ISO 8601 format (YYYY-MM-DD).
        
        Args:
            date_str: Date string to parse
            
        Returns:
            ISO 8601 date string or None if parsing fails
        """
        if not date_str or not date_str.strip():
            return None
            
        date_str = date_str.strip()
        
        # Common date formats to try
        formats = [
            '%Y-%m-%d',      # 2023-12-25
            '%m/%d/%Y',      # 12/25/2023
            '%m-%d-%Y',      # 12-25-2023
            '%Y/%m/%d',      # 2023/12/25
            '%d/%m/%Y',      # 25/12/2023
            '%d-%m-%Y',      # 25-12-2023
            '%B %d, %Y',     # December 25, 2023
            '%b %d, %Y',     # Dec 25, 2023
            '%Y-%m-%d %H:%M:%S',  # 2023-12-25 10:30:00
        ]
        
        for fmt in formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                return parsed_date.strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        self.logger.warning(f"Could not parse date: {date_str}")
        return None
    
    def fetch_url(self, url: str, max_retries: int = 3) -> Optional[str]:
        """
        Fetch content from a given URL with retry logic and backoff.
        
        Args:
            url: URL to fetch
            max_retries: Maximum number of retry attempts
            
        Returns:
            HTML content as string, or None if error occurred
        """
        backoff_delays = [0.5, 1.0, 2.0]  # seconds
        
        for attempt in range(max_retries):
            try:
                self.logger.info(f"Fetching URL (attempt {attempt + 1}/{max_retries}): {url}")
                response = self.session.get(url, timeout=self.scrape_timeout)
                
                if response.status_code == 200:
                    self.logger.info(f"Successfully fetched {url}")
                    return response.text
                else:
                    self.logger.warning(f"Non-200 status code {response.status_code} for {url}")
                    if attempt < max_retries - 1:
                        delay = backoff_delays[min(attempt, len(backoff_delays) - 1)]
                        self.logger.info(f"Retrying in {delay} seconds...")
                        time.sleep(delay)
                        continue
                    else:
                        return None
                        
            except Timeout:
                self.logger.warning(f"Timeout error fetching {url} (attempt {attempt + 1})")
                if attempt < max_retries - 1:
                    delay = backoff_delays[min(attempt, len(backoff_delays) - 1)]
                    time.sleep(delay)
                    continue
                else:
                    return None
            except ConnectionError:
                self.logger.warning(f"Connection error fetching {url} (attempt {attempt + 1})")
                if attempt < max_retries - 1:
                    delay = backoff_delays[min(attempt, len(backoff_delays) - 1)]
                    time.sleep(delay)
                    continue
                else:
                    return None
            except RequestException as e:
                self.logger.warning(f"Request error fetching {url} (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    delay = backoff_delays[min(attempt, len(backoff_delays) - 1)]
                    time.sleep(delay)
                    continue
                else:
                    return None
            except Exception as e:
                self.logger.error(f"Unexpected error fetching {url} (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    delay = backoff_delays[min(attempt, len(backoff_delays) - 1)]
                    time.sleep(delay)
                    continue
                else:
                    return None
        
        return None
    
    def _find_permit_table(self, soup: BeautifulSoup) -> Optional[BeautifulSoup]:
        """
        Find the permit data table using robust detection strategy.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Table element or None if not found
        """
        # Look for tables with headers containing permit-related keywords
        permit_keywords = ['permit', 'operator', 'county', 'district', 'well', 'lease', 'field']
        
        tables = soup.find_all('table')
        for table in tables:
            headers = table.find_all(['th', 'td'])
            header_text = ' '.join([h.get_text().lower() for h in headers[:10]])  # Check first 10 cells
            
            # Check if any permit keywords are present in headers
            if any(keyword in header_text for keyword in permit_keywords):
                self.logger.info("Found permit table with relevant headers")
                return table
        
        self.logger.warning("No permit table found with relevant headers")
        return None
    
    def _extract_csv_link(self, soup: BeautifulSoup, base_url: str) -> Optional[str]:
        """
        Extract CSV/XLSX download link from the page.
        
        Args:
            soup: BeautifulSoup object
            base_url: Base URL for resolving relative links
            
        Returns:
            Absolute URL to CSV/XLSX file or None
        """
        # Look for links with CSV/XLSX extensions or text
        csv_patterns = [
            r'\.csv$',
            r'\.xlsx?$',
            r'csv',
            r'excel',
            r'download',
            r'export'
        ]
        
        links = soup.find_all('a', href=True)
        for link in links:
            href = link.get('href', '')
            link_text = link.get_text().lower()
            
            # Check href and link text for CSV patterns
            for pattern in csv_patterns:
                if re.search(pattern, href.lower()) or re.search(pattern, link_text):
                    abs_url = self._abs_url(base_url, href)
                    self.logger.info(f"Found CSV link: {abs_url}")
                    return abs_url
        
        self.logger.info("No CSV/XLSX link found")
        return None
    
    def _normalize_permit_row(self, row_data: Dict[str, str]) -> Dict[str, Any]:
        """
        Normalize permit row data into consistent schema.
        
        Args:
            row_data: Raw row data dictionary
            
        Returns:
            Normalized permit data
        """
        normalized = {
            "permit_no": None,
            "operator": None,
            "county": None,
            "district": None,
            "well_name": None,
            "lease_no": None,
            "field": None,
            "submission_date": None,
            "api_no": None
        }
        
        # Map common field variations to our schema
        field_mapping = {
            'permit_no': ['permit', 'permit no', 'permit number', 'permit_id'],
            'operator': ['operator', 'company', 'operator name'],
            'county': ['county', 'county name'],
            'district': ['district', 'district no', 'district number'],
            'well_name': ['well', 'well name', 'well_name'],
            'lease_no': ['lease', 'lease no', 'lease number', 'lease_id'],
            'field': ['field', 'field name'],
            'submission_date': ['date', 'submission', 'submission date', 'filed', 'filed date'],
            'api_no': ['api', 'api no', 'api number', 'api_id']
        }
        
        # Normalize keys and values
        for key, value in row_data.items():
            if not value or not str(value).strip():
                continue
                
            key_lower = key.lower().strip()
            value_clean = str(value).strip()
            
            # Find matching field
            for schema_field, variations in field_mapping.items():
                if any(var in key_lower for var in variations):
                    if schema_field == 'submission_date':
                        normalized[schema_field] = self._parse_date(value_clean)
                    else:
                        normalized[schema_field] = value_clean
                    break
        
        return normalized
    
    def _extract_table_data(self, table: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Extract data from permit table.
        
        Args:
            table: Table BeautifulSoup element
            
        Returns:
            List of normalized permit dictionaries
        """
        rows = table.find_all('tr')
        if not rows:
            return []
        
        # Get headers from first row
        header_row = rows[0]
        headers = [th.get_text().strip() for th in header_row.find_all(['th', 'td'])]
        
        if not headers:
            self.logger.warning("No headers found in table")
            return []
        
        self.logger.info(f"Found table with {len(headers)} columns: {headers}")
        
        # Extract data rows
        permit_data = []
        for row in rows[1:]:  # Skip header row
            cells = row.find_all(['td', 'th'])
            if len(cells) != len(headers):
                continue  # Skip malformed rows
            
            row_data = {}
            for i, cell in enumerate(cells):
                if i < len(headers):
                    row_data[headers[i]] = cell.get_text().strip()
            
            # Normalize the row data
            normalized_row = self._normalize_permit_row(row_data)
            permit_data.append(normalized_row)
        
        self.logger.info(f"Extracted {len(permit_data)} permit records from table")
        return permit_data
    
    def run(self, target_url: Optional[str] = None) -> Dict[str, Any]:
        """
        Run the scraper service.
        Fetches permit data from target URL and returns structured data.
        
        Args:
            target_url: URL to scrape (defaults to RRC Texas permits page)
            
        Returns:
            Dictionary with source_url, csv_link, items, and fetched_at
        """
        if target_url is None:
            target_url = "https://www.rrc.texas.gov/oil-gas/research-and-statistics/drilling-permits/"
        
        self.logger.info("Scraper service running")
        print("Scraper service running")
        
        result = {
            "source_url": target_url,
            "csv_link": None,
            "items": [],
            "fetched_at": datetime.now().isoformat(),
            "warning": None
        }
        
        try:
            # Fetch the URL
            html_content = self.fetch_url(target_url)
            if not html_content:
                result["warning"] = "Failed to fetch content from target website"
                self.logger.warning(result["warning"])
                return result
            
            # Parse HTML
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for CSV link
            csv_link = self._extract_csv_link(soup, target_url)
            result["csv_link"] = csv_link
            
            # Look for permit table
            permit_table = self._find_permit_table(soup)
            if permit_table:
                permit_data = self._extract_table_data(permit_table)
                result["items"] = permit_data
            else:
                result["warning"] = "No permit table found on the page"
                self.logger.warning(result["warning"])
            
            # Print summary
            print(f"\nScraping completed:")
            print(f"  - Source: {target_url}")
            print(f"  - CSV Link: {csv_link or 'Not found'}")
            print(f"  - Permit Records: {len(result['items'])}")
            if result["warning"]:
                print(f"  - Warning: {result['warning']}")
            
        except Exception as e:
            error_msg = f"Unexpected error during scraping: {e}"
            result["warning"] = error_msg
            self.logger.error(error_msg)
        
        return result
