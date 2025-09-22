"""
Production-grade scraper for RRC W-1 search system using Selenium.
Handles form discovery, submission, and pagination parsing.
"""

import os
import re
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import urljoin, urlparse

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class RRCW1Client:
    """
    Client for scraping RRC W-1 drilling permit search results using Selenium.
    Handles form discovery, submission, and pagination.
    """
    
    def __init__(
        self,
        base_url: str = "https://webapps.rrc.state.tx.us",
        timeout: int = 20,
        user_agent: Optional[str] = None,
        headless: bool = True,
    ):
        """
        Initialize the RRC W-1 client with Selenium.
        
        Args:
            base_url: Base URL for RRC W-1 system
            timeout: Request timeout in seconds
            user_agent: Custom user agent (defaults to env var or default)
            headless: Whether to run browser in headless mode
        """
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.headless = headless
        
        # User agent from env or default
        if user_agent:
            self.user_agent = user_agent
        else:
            self.user_agent = os.getenv(
                "USER_AGENT", 
                "PermitTrackerBot/1.0 (+mailto:marshall@craatx.com)"
            )
        
            # Initialize Selenium WebDriver
            self.driver = None
            # Don't initialize driver immediately - do it lazily when needed
            self._driver_initialized = False
            self._use_requests_fallback = False
            
            logger.info(f"RRCW1Client initialized with base_url={self.base_url}, timeout={self.timeout}, headless={self.headless}")
    
    def _setup_driver(self):
        """Set up Chrome WebDriver with appropriate options for containerized environment."""
        if self._driver_initialized:
            return
            
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument("--headless")
        
        # Essential options for containerized environment
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--disable-features=VizDisplayCompositor")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument(f"--user-agent={self.user_agent}")
        
        # Additional stability options
        chrome_options.add_argument("--remote-debugging-port=9222")
        chrome_options.add_argument("--disable-background-timer-throttling")
        chrome_options.add_argument("--disable-backgrounding-occluded-windows")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        
        # Container-specific options
        chrome_options.add_argument("--disable-setuid-sandbox")
        chrome_options.add_argument("--disable-software-rasterizer")
        chrome_options.add_argument("--disable-background-networking")
        chrome_options.add_argument("--disable-default-apps")
        chrome_options.add_argument("--disable-sync")
        chrome_options.add_argument("--disable-translate")
        chrome_options.add_argument("--hide-scrollbars")
        chrome_options.add_argument("--metrics-recording-only")
        chrome_options.add_argument("--mute-audio")
        chrome_options.add_argument("--no-first-run")
        chrome_options.add_argument("--safebrowsing-disable-auto-update")
        chrome_options.add_argument("--disable-ipc-flooding-protection")
        
        try:
            logger.info("Attempting to initialize Chrome WebDriver...")
            
            # Try to find Chrome binary path
            chrome_binary_paths = [
                "/usr/bin/google-chrome",
                "/usr/bin/google-chrome-stable", 
                "/usr/bin/chromium-browser",
                "/usr/bin/chromium"
            ]
            
            chrome_binary = None
            for path in chrome_binary_paths:
                if os.path.exists(path):
                    chrome_binary = path
                    logger.info(f"Found Chrome binary at: {chrome_binary}")
                    break
            
            if chrome_binary:
                chrome_options.binary_location = chrome_binary
            
            # Try to find ChromeDriver path
            chromedriver_paths = [
                "/usr/bin/chromedriver",
                "/usr/local/bin/chromedriver",
                "/opt/chromedriver/chromedriver"
            ]
            
            chromedriver_path = None
            for path in chromedriver_paths:
                if os.path.exists(path):
                    chromedriver_path = path
                    logger.info(f"Found ChromeDriver at: {chromedriver_path}")
                    break
            
            # Initialize WebDriver
            if chromedriver_path:
                from selenium.webdriver.chrome.service import Service
                service = Service(chromedriver_path)
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
            else:
                self.driver = webdriver.Chrome(options=chrome_options)
            
            self.driver.set_page_load_timeout(self.timeout)
            self._driver_initialized = True
            logger.info("Chrome WebDriver initialized successfully")
            
            # Test the driver with a simple navigation
            logger.info("Testing WebDriver with simple navigation...")
            self.driver.get("about:blank")
            logger.info("WebDriver test successful")
            
        except Exception as e:
            logger.error(f"Failed to initialize Chrome WebDriver: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            if hasattr(e, 'msg'):
                logger.error(f"Error message: {e.msg}")
            
            # Try fallback to requests-based approach
            logger.warning("Falling back to requests-based scraping...")
            self._use_requests_fallback = True
            self._driver_initialized = False
    
    def __del__(self):
        """Clean up WebDriver on destruction."""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
    
    def _ensure_driver(self):
        """Ensure WebDriver is initialized."""
        if not self._driver_initialized and not self._use_requests_fallback:
            self._setup_driver()
    
    def _requests_fallback_fetch_all(self, begin: str, end: str, max_pages: Optional[int] = None) -> Dict[str, Any]:
        """
        Fallback method using requests when Selenium fails.
        This is a simplified version that returns a basic response.
        """
        logger.info("Using requests-based fallback for RRC W-1 search")
        
        try:
            import requests
            from bs4 import BeautifulSoup
            
            # Create a simple session
            session = requests.Session()
            session.headers.update({
                'User-Agent': self.user_agent,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
            })
            
            # Try to access the RRC W-1 page
            url = f"{self.base_url}/DP/initializePublicQueryAction.do"
            logger.info(f"Fallback: Attempting to access {url}")
            
            response = session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for any forms or data
            forms = soup.find_all('form')
            tables = soup.find_all('table')
            
            return {
                "source_root": self.base_url,
                "query_params": {
                    "begin": begin,
                    "end": end
                },
                "pages": 1,
                "count": 0,
                "items": [],
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "warning": "Selenium failed, using requests fallback. Limited functionality available.",
                "fallback_info": {
                    "forms_found": len(forms),
                    "tables_found": len(tables),
                    "page_title": soup.title.string if soup.title else "Unknown"
                }
            }
            
        except Exception as e:
            logger.error(f"Requests fallback also failed: {e}")
            return {
                "source_root": self.base_url,
                "query_params": {
                    "begin": begin,
                    "end": end
                },
                "pages": 0,
                "count": 0,
                "items": [],
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "warning": f"Both Selenium and requests fallback failed: {str(e)}"
            }
    
    def load_form(self) -> Dict[str, Any]:
        """
        Load the W-1 query form page.
        
        Returns:
            Dictionary with page info and driver state
        """
        logger.info("Loading W-1 query form...")
        
        # Ensure driver is initialized
        self._ensure_driver()
        
        url = f"{self.base_url}/DP/initializePublicQueryAction.do"
        
        try:
            self.driver.get(url)
            
            # Wait for page to load
            WebDriverWait(self.driver, self.timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "form"))
            )
            
            logger.info(f"Successfully loaded form page: {url}")
            
            return {
                "url": url,
                "driver": self.driver,
                "page_source": self.driver.page_source
            }
            
        except TimeoutException:
            logger.error(f"Timeout loading form page: {url}")
            raise
        except Exception as e:
            logger.error(f"Error loading form page: {e}")
            raise
    
    def query(self, begin_mmddyyyy: str, end_mmddyyyy: str) -> Dict[str, Any]:
        """
        Submit a query with date range using Selenium.
        
        Args:
            begin_mmddyyyy: Start date in MM/DD/YYYY format
            end_mmddyyyy: End date in MM/DD/YYYY format
            
        Returns:
            Dictionary with results page info
        """
        logger.info(f"Submitting query for date range: {begin_mmddyyyy} to {end_mmddyyyy}")
        
        # Load the form page
        form_data = self.load_form()
        
        try:
            # Find and fill the date fields
            begin_field = self.driver.find_element(By.NAME, "submitStart")
            end_field = self.driver.find_element(By.NAME, "submitEnd")
            
            # Clear and fill the date fields
            begin_field.clear()
            begin_field.send_keys(begin_mmddyyyy)
            
            end_field.clear()
            end_field.send_keys(end_mmddyyyy)
            
            logger.info(f"Set date fields: submitStart={begin_mmddyyyy}, submitEnd={end_mmddyyyy}")
            
            # Find and click the submit button
            submit_button = self.driver.find_element(By.NAME, "submit")
            submit_button.click()
            
            logger.info("Clicked submit button")
            
            # Wait for results page to load
            WebDriverWait(self.driver, self.timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Get the results page source
            results_html = self.driver.page_source
            current_url = self.driver.current_url
            
            logger.info(f"Results page loaded: {current_url}")
            
            return {
                "results_html": results_html,
                "current_url": current_url
            }
            
        except NoSuchElementException as e:
            logger.error(f"Could not find form element: {e}")
            raise
        except TimeoutException as e:
            logger.error(f"Timeout during form submission: {e}")
            raise
        except Exception as e:
            logger.error(f"Error during form submission: {e}")
            raise
    
    def _parse_table(self, html: str) -> List[Dict[str, Any]]:
        """
        Parse the results table and extract normalized rows.
        
        Args:
            html: HTML content of results page
            
        Returns:
            List of normalized row dictionaries
        """
        logger.info("Parsing results table...")
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Find the main results table
        tables = soup.find_all('table')
        results_table = None
        
        for table in tables:
            # Look for headers that match RRC W-1 format
            headers = table.find_all('th')
            if headers:
                header_texts = [th.get_text(strip=True) for th in headers]
                # Check if this looks like the results table
                if any(keyword in ' '.join(header_texts).lower() for keyword in 
                      ['status', 'api', 'operator', 'lease', 'well', 'county']):
                    results_table = table
                    break
        
        if not results_table:
            logger.warning("No results table found on page")
            return []
        
        # Extract headers
        header_row = results_table.find('tr')
        if not header_row:
            logger.warning("No header row found in table")
            return []
        
        headers = []
        for th in header_row.find_all(['th', 'td']):
            header_text = th.get_text(strip=True)
            headers.append(header_text)
        
        logger.info(f"Found table with {len(headers)} columns: {headers}")
        
        # Build header mapping
        header_mapping = {}
        for i, header in enumerate(headers):
            header_lower = header.lower()
            
            # Map to normalized field names
            if 'status date' in header_lower:
                header_mapping[i] = 'status_date'
            elif 'status' in header_lower and ('#' in header or 'no' in header_lower or header_lower == 'status'):
                header_mapping[i] = 'status'
            elif 'api' in header_lower:
                header_mapping[i] = 'api_no'
            elif 'operator' in header_lower:
                header_mapping[i] = 'operator'
            elif 'lease' in header_lower:
                header_mapping[i] = 'lease_name'
            elif 'well' in header_lower and '#' in header:
                header_mapping[i] = 'well_id'
            elif 'dist' in header_lower:
                header_mapping[i] = 'district'
            elif 'county' in header_lower:
                header_mapping[i] = 'county'
            elif 'wellbore' in header_lower:
                header_mapping[i] = 'wellbore_profile'
            elif 'filing' in header_lower and 'purpose' in header_lower:
                header_mapping[i] = 'filing_purpose'
            elif 'amend' in header_lower:
                header_mapping[i] = 'amended'
            elif 'total depth' in header_lower:
                header_mapping[i] = 'total_depth'
            elif 'stacked' in header_lower:
                header_mapping[i] = 'stacked_parent'
            elif 'queue' in header_lower:
                header_mapping[i] = 'current_queue'
        
        # Extract data rows
        rows = []
        data_rows = results_table.find_all('tr')[1:]  # Skip header row
        
        for row_idx, row in enumerate(data_rows):
            try:
                cells = row.find_all(['td', 'th'])
                if len(cells) != len(headers):
                    logger.warning(f"Row {row_idx} has {len(cells)} cells, expected {len(headers)}")
                    continue
                
                # Build row dictionary
                row_data = {}
                for i, cell in enumerate(cells):
                    cell_text = cell.get_text(strip=True)
                    if i in header_mapping:
                        field_name = header_mapping[i]
                        row_data[field_name] = cell_text
                
                # Ensure all expected fields are present
                expected_fields = [
                    'status_date', 'status', 'api_no', 'operator', 'lease_name',
                    'well_id', 'district', 'county', 'wellbore_profile',
                    'filing_purpose', 'amended', 'total_depth', 'stacked_parent',
                    'current_queue'
                ]
                
                for field in expected_fields:
                    if field not in row_data:
                        row_data[field] = None
                
                rows.append(row_data)
                
            except Exception as e:
                logger.warning(f"Error parsing row {row_idx}: {e}")
                continue
        
        logger.info(f"Parsed {len(rows)} rows from table")
        return rows
    
    def _next_page_url(self, current_url: str) -> Optional[str]:
        """
        Detect pagination and navigate to next page.
        
        Args:
            current_url: Current page URL
            
        Returns:
            Next page URL or None if last page
        """
        logger.info("Looking for next page...")
        
        try:
            # Look for "Next >" link
            next_links = self.driver.find_elements(By.XPATH, "//a[contains(text(), 'Next') or contains(text(), '>')]")
            
            if next_links:
                next_link = next_links[0]
                href = next_link.get_attribute('href')
                if href:
                    logger.info(f"Found 'Next >' link: {href}")
                    return href
            
            # Look for pager.offset pattern
            all_links = self.driver.find_elements(By.TAG_NAME, 'a')
            for link in all_links:
                href = link.get_attribute('href')
                if href and 'pager.offset' in href:
                    logger.info(f"Found pager.offset link: {href}")
                    return href
            
            logger.info("No next page found")
            return None
            
        except Exception as e:
            logger.error(f"Error looking for next page: {e}")
            return None
    
    def fetch_all(
        self,
        begin_mmddyyyy: str,
        end_mmddyyyy: str,
        max_pages: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Fetch all results across all pages using Selenium.
        
        Args:
            begin_mmddyyyy: Start date in MM/DD/YYYY format
            end_mmddyyyy: End date in MM/DD/YYYY format
            max_pages: Maximum number of pages to fetch (None for all)
            
        Returns:
            Dictionary with query results and metadata
        """
        logger.info(f"Fetching all results for {begin_mmddyyyy} to {end_mmddyyyy}")
        
        # Check if we should use fallback
        if self._use_requests_fallback:
            logger.info("Using requests fallback due to Selenium failure")
            return self._requests_fallback_fetch_all(begin_mmddyyyy, end_mmddyyyy, max_pages)
        
        try:
            # Submit initial query
            query_result = self.query(begin_mmddyyyy, end_mmddyyyy)
            
            all_items = []
            current_html = query_result["results_html"]
            current_url = query_result["current_url"]
            page_count = 0
            
            while current_html and (max_pages is None or page_count < max_pages):
                page_count += 1
                logger.info(f"Processing page {page_count}")
                
                # Parse current page
                page_items = self._parse_table(current_html)
                all_items.extend(page_items)
                
                logger.info(f"Page {page_count}: found {len(page_items)} items")
                
                # Look for next page
                next_url = self._next_page_url(current_url)
                if not next_url:
                    logger.info("No more pages found")
                    break
                
                # Navigate to next page
                try:
                    self.driver.get(next_url)
                    WebDriverWait(self.driver, self.timeout).until(
                        EC.presence_of_element_located((By.TAG_NAME, "body"))
                    )
                    current_html = self.driver.page_source
                    current_url = self.driver.current_url
                except Exception as e:
                    logger.error(f"Error fetching next page: {e}")
                    break
            
            logger.info(f"Completed fetching: {page_count} pages, {len(all_items)} total items")
            
            return {
                "query": {
                    "begin": begin_mmddyyyy,
                    "end": end_mmddyyyy
                },
                "pages": page_count,
                "count": len(all_items),
                "items": all_items,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "source_root": self.base_url
            }
            
        except Exception as e:
            logger.error(f"Error in fetch_all: {e}")
            # Try fallback if Selenium fails
            if not self._use_requests_fallback:
                logger.warning("Selenium failed, attempting requests fallback...")
                self._use_requests_fallback = True
                return self._requests_fallback_fetch_all(begin_mmddyyyy, end_mmddyyyy, max_pages)
            else:
                raise
        finally:
            # Clean up driver
            if self.driver:
                self.driver.quit()