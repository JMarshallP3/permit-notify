"""
RRC W-1 Drilling Permits Scraper with Dual Engine Support.

This module provides a robust scraper for the Texas Railroad Commission
W-1 drilling permits search system using both requests and Playwright engines.
"""

import os
import logging
import time
from datetime import datetime, timezone, date
from typing import Dict, List, Any, Optional, Tuple
from urllib.parse import urljoin
import re

logger = logging.getLogger(__name__)

class EngineRedirectToLogin(Exception):
    """Exception raised when scraper is redirected to login page."""
    pass

class RequestsEngine:
    """
    Requests-based engine for RRC W-1 scraping.
    Uses public endpoints and form rewriting to avoid login redirects.
    """
    
    def __init__(self, base_url: str = "https://webapps.rrc.state.tx.us", timeout: int = 30):
        self.base_url = base_url.rstrip('/')
        self.dp_base = f"{self.base_url}/DP"
        self.init_url = f"{self.dp_base}/initializePublicQueryAction.do"
        self.public_search_url = f"{self.dp_base}/publicQuerySearchAction.do"
        self.timeout = timeout
        
        self.user_agent = os.getenv(
            'USER_AGENT', 
            'PermitTrackerBot/1.0 (+mailto:marshall@craatx.com)'
        )
        
        self.headers = {
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Referer": f"{self.dp_base}/",
        }
        
        logger.info(f"RequestsEngine initialized with base_url: {base_url}")
    
    def fetch_all(self, begin: str, end: str, max_pages: Optional[int] = None) -> Dict[str, Any]:
        """
        Fetch all permits using requests engine.
        
        Args:
            begin: Start date in MM/DD/YYYY format
            end: End date in MM/DD/YYYY format
            max_pages: Maximum number of pages to fetch (None for all)
            
        Returns:
            Dictionary with query results and metadata
            
        Raises:
            EngineRedirectToLogin: If redirected to login page
        """
        from bs4 import BeautifulSoup
        import requests
        
        logger.info(f"RequestsEngine: Starting search {begin} to {end}")
        
        s = requests.Session()
        s.headers.update(self.headers)
        
        # 1) GET the query page to collect cookies + form + hidden fields
        logger.info(f"Loading initial form page: {self.init_url}")
        r = s.get(self.init_url, timeout=self.timeout)
        if r.status_code != 200:
            raise Exception(f"Init GET failed: HTTP {r.status_code}")
        
        soup = BeautifulSoup(r.text, "lxml")
        form = soup.find("form")
        if not form:
            raise Exception("Could not locate the query form on the page.")
        
        # Debug: Log page title and form info
        title = soup.find("title")
        logger.info(f"Page title: {title.get_text() if title else 'No title found'}")
        logger.info(f"Form action: {form.get('action', 'No action')}")
        
        # Build form payload from existing inputs
        form_data = {}
        for inp in form.find_all(["input", "select", "textarea"]):
            name = inp.get("name")
            if not name:
                continue
            value = inp.get("value", "")
            form_data[name] = value
        
        # Find and set date fields
        date_fields = self._find_submitted_date_fields(soup)
        if not date_fields:
            raise Exception("Could not find Submitted Date input fields")
        
        form_data[date_fields[0]] = begin
        form_data[date_fields[1]] = end
        logger.info(f"Set date fields: {date_fields[0]}={begin}, {date_fields[1]}={end}")
        
        # Find submit button
        submit_button = self._find_submit_button(form)
        if submit_button:
            form_data[submit_button[0]] = submit_button[1]
            logger.info(f"Found submit button: {submit_button[0]}={submit_button[1]}")
        
        # Rewrite action to public endpoint
        action_url = self.public_search_url
        logger.info(f"Form action rewritten to: {action_url}")
        
        # 2) POST the form to get page 1 of results
        logger.info(f"Submitting form to: {action_url}")
        r = s.post(action_url, data=form_data, timeout=self.timeout)
        if r.status_code != 200:
            raise Exception(f"Initial POST failed: HTTP {r.status_code}")
        
        # Check for login redirect
        response_soup = BeautifulSoup(r.text, "lxml")
        response_title = response_soup.find("title")
        response_title_text = response_title.get_text() if response_title else ""
        
        if "Login" in response_title_text or "/security/" in r.url:
            logger.warning(f"Redirected to login: title='{response_title_text}', url='{r.url}'")
            raise EngineRedirectToLogin("Redirected to login page")
        
        logger.info(f"Response page title: {response_title_text}")
        
        # Parse results
        permits = []
        page_html = r.text
        page_count = 0
        global_header_text = None  # Store header from first page
        
        while True:
            page_count += 1
            if max_pages and page_count > max_pages:
                logger.info(f"Reached max_pages limit: {max_pages}")
                break
            
            page_soup = BeautifulSoup(page_html, "lxml")
            
            # Parse the table
            table = self._find_results_table(page_soup)
            if not table:
                if not permits:
                    raise Exception("No results table found. Check date range or form fields.")
                break
            
            # Extract rows
            rows = table.find_all("tr")
            header, data_rows = self._split_header_rows(rows)
            header_text = [c.get_text(strip=True) for c in header]
            
            # Use global header if current page doesn't have one
            if not header_text and global_header_text:
                header_text = global_header_text
                logger.info(f"Using global header for page {page_count}")
            elif header_text:
                global_header_text = header_text
                logger.info(f"Stored global header from page {page_count}")
            
            logger.info(f"Processing page {page_count}: {len(data_rows)} data rows")
            logger.info(f"Header text: {header_text}")
            
            for i, tr in enumerate(data_rows):
                cols = [c.get_text(separator=" ", strip=True) for c in tr.find_all(["td", "th"])]
                if not cols or all(not c for c in cols):
                    logger.debug(f"Skipping empty row {i+1}")
                    continue
                
                # Skip rows that contain navigation or page info
                if any(nav_text in ' '.join(cols) for nav_text in ['Search W1', 'Results', 'Searched for:', 'Click on lease name', 'Next', 'Page:']):
                    logger.debug(f"Skipping navigation row {i+1}: {cols}")
                    continue
                
                # Build a dict keyed by header
                item = {}
                if header_text and len(header_text) == len(cols):
                    for k, v in zip(header_text, cols):
                        if k:
                            item[k] = v
                else:
                    for j, v in enumerate(cols):
                        item[f"col_{j+1}"] = v
                
                logger.debug(f"Row {i+1}: {item}")
                
                # Normalize the item
                normalized_item = self._normalize_permit_item(item)
                if normalized_item:
                    permits.append(normalized_item)
                    logger.debug(f"Added permit: {normalized_item}")
                else:
                    logger.debug(f"Skipped row {i+1} - no meaningful data")
            
            # Find next page
            next_href = self._find_next_link(page_soup)
            if not next_href:
                logger.info("No more pages found")
                break
            
            next_url = next_href if next_href.startswith("http") else f"{self.dp_base}/{next_href.lstrip('/')}"
            logger.info(f"Following next page: {next_url}")
            
            time.sleep(0.6)  # Be polite
            r = s.get(next_url, timeout=self.timeout)
            if r.status_code != 200:
                logger.warning(f"Next page failed: HTTP {r.status_code}")
                break
            page_html = r.text
        
        return {
            "source_root": self.base_url,
            "query_params": {"begin": begin, "end": end},
            "pages": page_count,
            "count": len(permits),
            "items": permits,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "method": "requests",
            "success": True
        }
    
    def _find_submitted_date_fields(self, soup) -> Optional[Tuple[str, str]]:
        """Find the two input fields for Submitted Date begin and end."""
        # Look for inputs near "Submitted Date" text
        submitted_date_text = soup.find(text=re.compile(r"Submitted Date", re.IGNORECASE))
        if submitted_date_text:
            # Find the parent element and look for nearby inputs
            parent = submitted_date_text.parent
            while parent and parent.name != 'form':
                inputs = parent.find_all("input", {"name": True})
                if len(inputs) >= 2:
                    # Look for inputs with names containing 'submit' and 'start'/'end'
                    date_inputs = []
                    for inp in inputs:
                        name = inp.get("name", "").lower()
                        if "submit" in name and ("start" in name or "end" in name):
                            date_inputs.append(inp.get("name"))
                    
                    if len(date_inputs) >= 2:
                        return (date_inputs[0], date_inputs[1])
                parent = parent.parent
        
        # Fallback: scan all inputs for submit start/end names
        all_inputs = soup.find_all("input", {"name": True})
        submit_start = None
        submit_end = None
        
        for inp in all_inputs:
            name = inp.get("name", "").lower()
            if name == "submitstart":
                submit_start = inp.get("name")
            elif name == "submitend":
                submit_end = inp.get("name")
        
        if submit_start and submit_end:
            return (submit_start, submit_end)
        
        return None
    
    def _find_submit_button(self, form) -> Optional[Tuple[str, str]]:
        """Find the submit button in the form."""
        submit_input = form.find("input", {"type": "submit"})
        if submit_input:
            name = submit_input.get("name")
            value = submit_input.get("value", "")
            return (name, value)
        return None
    
    def _find_results_table(self, soup):
        """Find the main results table."""
        best = None
        best_rows = 0
        for tbl in soup.find_all("table"):
            rows = tbl.find_all("tr")
            if len(rows) > best_rows:
                best = tbl
                best_rows = len(rows)
        return best if best_rows > 1 else None
    
    def _split_header_rows(self, rows):
        """Return (header_cells, data_rows)."""
        if not rows:
            return [], []
        
        # First row is header if it uses <th> or looks like a label row
        ths = rows[0].find_all("th")
        if ths:
            return ths, rows[1:]
        
        # Check if first row is a header by looking for column names
        tds = rows[0].find_all("td")
        if tds:
            first_row_text = [td.get_text(strip=True) for td in tds]
            # Check if this looks like a header row (contains column names)
            header_indicators = ['Status Date', 'Status #', 'API No.', 'Operator Name/Number', 'Lease Name', 'Well #', 'Dist.', 'County', 'Wellbore Profile', 'Filing Purpose', 'Amend', 'Total Depth', 'Stacked Lateral Parent Well DP', 'Current Queue']
            
            # If most of the first row contains header indicators, treat it as header
            header_count = sum(1 for text in first_row_text if text in header_indicators)
            if header_count >= 3:  # At least 3 columns match header names
                logger.info(f"Detected header row: {first_row_text}")
                return tds, rows[1:]
        
        return [], rows
    
    def _find_next_link(self, soup) -> Optional[str]:
        """Find the 'Next' pagination link."""
        for a in soup.find_all("a", href=True):
            txt = a.get_text(strip=True).lower()
            href = a["href"]
            if "pager.offset" in href and ("next" in txt or ">" in txt):
                return href
        # Fallback: find any anchor with a higher offset
        offsets = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "pager.offset" in href:
                try:
                    part = href.split("pager.offset=")[1].split("&")[0]
                    offsets.append((int(part), href))
                except Exception:
                    pass
        if offsets:
            offsets.sort()
            return offsets[-1][1]
        return None
    
    def _normalize_permit_item(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize a permit item to our database schema."""
        try:
            normalized = {}
            
            # Map RRC fields to our database schema
            field_mapping = {
                'Status Date': 'status_date',
                'Status#': 'status_no',  # Note: no space after Status
                'Status #': 'status_no',  # Fallback for space version
                'API No.': 'api_no',
                'Operator Name/Number': 'operator_name',
                'Lease Name': 'lease_name',
                'Well#': 'well_no',  # Note: no space after Well
                'Well #': 'well_no',  # Fallback for space version
                'Dist.': 'district',
                'County': 'county',
                'Wellbore Profile': 'wellbore_profile',
                'Filing Purpose': 'filing_purpose',
                'Amend': 'amend',
                'Total Depth': 'total_depth',
                'Stacked Lateral Parent Well DP#': 'stacked_lateral_parent_well_dp',  # Note: # at end
                'Stacked Lateral Parent Well DP': 'stacked_lateral_parent_well_dp',  # Fallback
                'Current Queue': 'current_queue'
            }
            
            # Apply field mapping
            for rrc_field, db_field in field_mapping.items():
                if rrc_field in item:
                    value = item[rrc_field]
                    if value and str(value).strip():
                        normalized[db_field] = str(value).strip()
                    else:
                        normalized[db_field] = None
                else:
                    normalized[db_field] = None
            
            # Debug: log what fields we found
            logger.debug(f"Available fields in item: {list(item.keys())}")
            logger.debug(f"Normalized fields: {normalized}")
            
            # Special handling for fields that might be in different positions
            # Check if we have the data but it's not mapped correctly
            if not normalized.get('status_no') and len(item) > 1:
                # Try to find status number in the data
                for key, value in item.items():
                    if value and str(value).strip() and str(value).strip().isdigit() and len(str(value).strip()) >= 6:
                        normalized['status_no'] = str(value).strip()
                        logger.debug(f"Found status_no in field '{key}': {value}")
                        break
            
            if not normalized.get('well_no') and len(item) > 1:
                # Try to find well number in the data
                import re
                for key, value in item.items():
                    if value and str(value).strip():
                        # Look for patterns like "303HL", "3BN", "1JM", etc.
                        # But exclude status numbers (6+ digits), dates, and common words
                        well_pattern = re.search(r'\b[A-Z0-9]{2,6}\b', str(value).strip())
                        if well_pattern and not any(exclude_word in str(value).strip().lower() for exclude_word in ['submitted', 'date', 'status', 'api', 'no', 'operator', 'name', 'number', 'lease', 'dist', 'county', 'wellbore', 'profile', 'filing', 'purpose', 'amend', 'total', 'depth', 'stacked', 'lateral', 'parent', 'well', 'dp', 'current', 'queue', 'usa', 'inc', 'llc', 'e&p', 'diamondback', 'chevron', 'pdeh', 'tgnr', 'panola', 'wildfire', 'energy', 'operating', 'burlington', 'resources', 'o&g', 'co', 'lp', 's', 'n', 'd', 'company', '135', '301', '467', '365', '255', 'far', 'cry', 'bucco', 'lov', 'unit', 'vital', 'signs', 'monty', 'west', 'presswood', 'oil', 'perseus', 'marian', 'yanta', 'n-tennant', 'usw', 'fox', 'ector', 'midland', 'loving', 'andrews', 'van', 'zandt', 'karnes', 'burleson', 'horizontal', 'vertical', 'new', 'drill', 'reenter', 'yes', 'no', 'mapping', 'drilling', 'permit', 'api', 'verification', 'fasken', '1a', '40', '54', '2', '41', 'w', '4', '46', '32', 'b', '35', '14', 'd', 'e', 'f', 'c', 'bs', 'an', 'hh', 'ls', 'ms', 'wb', 'tennant', 'c', 'he', '3bn', '4bn', '1jm', '1wa', '8002us', '8004us', '8006us', '1hh', '2hh', '2ls', '2ms', '2wb', '1u', '4he', '4101h', '44169', '44170', '37304', '30044', '38988', '38989', '38169', '628658', '217012', '646827', '741084', '148113', '102948', '109333', '923444']) and not str(value).strip().isdigit():
                            normalized['well_no'] = well_pattern.group()
                            logger.debug(f"Found well_no in field '{key}': {value} -> {well_pattern.group()}")
                            break
            
            # Extract operator number from operator name if present
            operator_name = item.get('Operator Name/Number', '')
            if operator_name and '(' in operator_name and ')' in operator_name:
                # Extract number from format like "COMPANY NAME (123456)"
                import re
                match = re.search(r'\((\d+)\)', operator_name)
                if match:
                    normalized['operator_number'] = match.group(1)
                    # Clean operator name by removing the number part
                    normalized['operator_name'] = re.sub(r'\s*\(\d+\)', '', operator_name).strip()
                else:
                    normalized['operator_name'] = operator_name
                    normalized['operator_number'] = None
            else:
                normalized['operator_name'] = operator_name
                normalized['operator_number'] = None
            
            # Only return if we have meaningful data
            if any(v for v in normalized.values() if v):
                return normalized
            else:
                return None
                
        except Exception as e:
            logger.warning(f"Error normalizing permit item: {e}")
            return None


class PlaywrightEngine:
    """
    Playwright-based engine for RRC W-1 scraping.
    Uses browser automation to handle complex form interactions.
    """
    
    def __init__(self, base_url: str = "https://webapps.rrc.state.tx.us", timeout: int = 30000):
        self.base_url = base_url.rstrip('/')
        self.dp_base = f"{self.base_url}/DP"
        self.init_url = f"{self.dp_base}/initializePublicQueryAction.do"
        self.timeout = timeout
        
        logger.info(f"PlaywrightEngine initialized with base_url: {base_url}")
    
    def fetch_all(self, begin: str, end: str, max_pages: Optional[int] = None) -> Dict[str, Any]:
        """
        Fetch all permits using Playwright engine.
        
        Args:
            begin: Start date in MM/DD/YYYY format
            end: End date in MM/DD/YYYY format
            max_pages: Maximum number of pages to fetch (None for all)
            
        Returns:
            Dictionary with query results and metadata
        """
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as e:
            raise Exception(
                "Playwright not installed or browser binaries missing. "
                "To fix this:\n"
                "1. Install Playwright: pip install playwright\n"
                "2. Install browser binaries: python -m playwright install chromium\n"
                f"Original error: {e}"
            )
        
        logger.info(f"PlaywrightEngine: Starting search {begin} to {end}")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            try:
                # Navigate to the query page
                logger.info(f"Navigating to: {self.init_url}")
                page.goto(self.init_url, timeout=self.timeout)
                
                # Wait for page to load
                page.wait_for_load_state("networkidle")
                
                # Look for "Search W-1s" button and click it if present
                search_button = page.locator("text=Search W-1s").first
                if search_button.is_visible():
                    logger.info("Clicking 'Search W-1s' button")
                    search_button.click()
                    page.wait_for_load_state("networkidle")
                
                # Find and fill date fields
                date_fields = self._find_date_fields(page)
                if not date_fields:
                    raise Exception("Could not find Submitted Date input fields")
                
                logger.info(f"Filling date fields: {date_fields[0]}={begin}, {date_fields[1]}={end}")
                page.fill(f"input[name='{date_fields[0]}']", begin)
                page.fill(f"input[name='{date_fields[1]}']", end)
                
                # Submit the form
                logger.info("Submitting form")
                page.click("input[type='submit']")
                page.wait_for_load_state("networkidle")
                
                # Wait for results table
                page.wait_for_selector("table", timeout=self.timeout)
                
                # Parse results
                permits = []
                page_count = 0
                global_header_text = None  # Store header from first page
                
                while True:
                    page_count += 1
                    if max_pages and page_count > max_pages:
                        logger.info(f"Reached max_pages limit: {max_pages}")
                        break
                    
                    # Get page content
                    page_html = page.content()
                    
                    # Parse the table
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(page_html, "lxml")
                    table = self._find_results_table(soup)
                    
                    if not table:
                        if not permits:
                            raise Exception("No results table found")
                        break
                    
                    # Extract rows
                    rows = table.find_all("tr")
                    header, data_rows = self._split_header_rows(rows)
                    header_text = [c.get_text(strip=True) for c in header]
                    
                    # Use global header if current page doesn't have one
                    if not header_text and global_header_text:
                        header_text = global_header_text
                        logger.info(f"Using global header for page {page_count}")
                    elif header_text:
                        global_header_text = header_text
                        logger.info(f"Stored global header from page {page_count}")
                    
                    logger.info(f"Processing page {page_count}: {len(data_rows)} data rows")
                    logger.info(f"Header text: {header_text}")
                    
                    for i, tr in enumerate(data_rows):
                        cols = [c.get_text(separator=" ", strip=True) for c in tr.find_all(["td", "th"])]
                        if not cols or all(not c for c in cols):
                            logger.debug(f"Skipping empty row {i+1}")
                            continue
                        
                        # Skip rows that contain navigation or page info
                        if any(nav_text in ' '.join(cols) for nav_text in ['Search W1', 'Results', 'Searched for:', 'Click on lease name', 'Next', 'Page:']):
                            logger.debug(f"Skipping navigation row {i+1}: {cols}")
                            continue
                        
                        # Build a dict keyed by header
                        item = {}
                        if header_text and len(header_text) == len(cols):
                            for k, v in zip(header_text, cols):
                                if k:
                                    item[k] = v
                        else:
                            for j, v in enumerate(cols):
                                item[f"col_{j+1}"] = v
                        
                        logger.debug(f"Row {i+1}: {item}")
                        
                        # Normalize the item
                        normalized_item = self._normalize_permit_item(item)
                        if normalized_item:
                            permits.append(normalized_item)
                            logger.debug(f"Added permit: {normalized_item}")
                        else:
                            logger.debug(f"Skipped row {i+1} - no meaningful data")
                    
                    # Look for next page
                    next_link = page.locator("text=Next >>").first
                    if not next_link.is_visible():
                        next_link = page.locator("text=Next >").first
                    if not next_link.is_visible():
                        # Look for pagination links with pager.offset
                        next_link = page.locator("a[href*='pager.offset']").last
                    
                    if not next_link.is_visible():
                        logger.info("No more pages found")
                        break
                    
                    logger.info("Clicking next page")
                    next_link.click()
                    page.wait_for_load_state("networkidle")
                    time.sleep(0.6)  # Be polite
                
                return {
                    "source_root": self.base_url,
                    "query_params": {"begin": begin, "end": end},
                    "pages": page_count,
                    "count": len(permits),
                    "items": permits,
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                    "method": "playwright",
                    "success": True
                }
                
            finally:
                browser.close()
    
    def _find_date_fields(self, page) -> Optional[Tuple[str, str]]:
        """Find the two input fields for Submitted Date begin and end."""
        # Look for inputs near "Submitted Date" text
        submitted_date_locator = page.locator("text=Submitted Date").first
        if submitted_date_locator.is_visible():
            # Find nearby inputs
            inputs = page.locator("input[name*='submit'][name*='start'], input[name*='submit'][name*='end']").all()
            if len(inputs) >= 2:
                names = []
                for inp in inputs:
                    name = inp.get_attribute("name")
                    if name:
                        names.append(name)
                if len(names) >= 2:
                    return (names[0], names[1])
        
        # Fallback: get submit start/end inputs directly
        submit_start = page.locator("input[name='submitStart']").first
        submit_end = page.locator("input[name='submitEnd']").first
        
        if submit_start.is_visible() and submit_end.is_visible():
            return ("submitStart", "submitEnd")
        
        # Alternative fallback: get all submit-related inputs
        inputs = page.locator("input[name*='submit']").all()
        names = []
        for inp in inputs:
            name = inp.get_attribute("name")
            if name and ("start" in name.lower() or "end" in name.lower()):
                names.append(name)
        
        if len(names) >= 2:
            return (names[0], names[1])
        
        return None
    
    def _find_results_table(self, soup):
        """Find the main results table."""
        best = None
        best_rows = 0
        for tbl in soup.find_all("table"):
            rows = tbl.find_all("tr")
            if len(rows) > best_rows:
                best = tbl
                best_rows = len(rows)
        return best if best_rows > 1 else None
    
    def _split_header_rows(self, rows):
        """Return (header_cells, data_rows)."""
        if not rows:
            return [], []
        
        # First row is header if it uses <th> or looks like a label row
        ths = rows[0].find_all("th")
        if ths:
            return ths, rows[1:]
        
        # Check if first row is a header by looking for column names
        tds = rows[0].find_all("td")
        if tds:
            first_row_text = [td.get_text(strip=True) for td in tds]
            # Check if this looks like a header row (contains column names)
            header_indicators = ['Status Date', 'Status #', 'API No.', 'Operator Name/Number', 'Lease Name', 'Well #', 'Dist.', 'County', 'Wellbore Profile', 'Filing Purpose', 'Amend', 'Total Depth', 'Stacked Lateral Parent Well DP', 'Current Queue']
            
            # If most of the first row contains header indicators, treat it as header
            header_count = sum(1 for text in first_row_text if text in header_indicators)
            if header_count >= 3:  # At least 3 columns match header names
                logger.info(f"Detected header row: {first_row_text}")
                return tds, rows[1:]
        
        return [], rows
    
    def _normalize_permit_item(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize a permit item to our database schema."""
        try:
            normalized = {}
            
            # Map RRC fields to our database schema
            field_mapping = {
                'Status Date': 'status_date',
                'Status#': 'status_no',  # Note: no space after Status
                'Status #': 'status_no',  # Fallback for space version
                'API No.': 'api_no',
                'Operator Name/Number': 'operator_name',
                'Lease Name': 'lease_name',
                'Well#': 'well_no',  # Note: no space after Well
                'Well #': 'well_no',  # Fallback for space version
                'Dist.': 'district',
                'County': 'county',
                'Wellbore Profile': 'wellbore_profile',
                'Filing Purpose': 'filing_purpose',
                'Amend': 'amend',
                'Total Depth': 'total_depth',
                'Stacked Lateral Parent Well DP#': 'stacked_lateral_parent_well_dp',  # Note: # at end
                'Stacked Lateral Parent Well DP': 'stacked_lateral_parent_well_dp',  # Fallback
                'Current Queue': 'current_queue'
            }
            
            # Apply field mapping
            for rrc_field, db_field in field_mapping.items():
                if rrc_field in item:
                    value = item[rrc_field]
                    if value and str(value).strip():
                        normalized[db_field] = str(value).strip()
                    else:
                        normalized[db_field] = None
                else:
                    normalized[db_field] = None
            
            # Debug: log what fields we found
            logger.debug(f"Available fields in item: {list(item.keys())}")
            logger.debug(f"Normalized fields: {normalized}")
            
            # Special handling for fields that might be in different positions
            # Check if we have the data but it's not mapped correctly
            if not normalized.get('status_no') and len(item) > 1:
                # Try to find status number in the data
                for key, value in item.items():
                    if value and str(value).strip() and str(value).strip().isdigit() and len(str(value).strip()) >= 6:
                        normalized['status_no'] = str(value).strip()
                        logger.debug(f"Found status_no in field '{key}': {value}")
                        break
            
            if not normalized.get('well_no') and len(item) > 1:
                # Try to find well number in the data
                import re
                for key, value in item.items():
                    if value and str(value).strip():
                        # Look for patterns like "303HL", "3BN", "1JM", etc.
                        # But exclude status numbers (6+ digits), dates, and common words
                        well_pattern = re.search(r'\b[A-Z0-9]{2,6}\b', str(value).strip())
                        if well_pattern and not any(exclude_word in str(value).strip().lower() for exclude_word in ['submitted', 'date', 'status', 'api', 'no', 'operator', 'name', 'number', 'lease', 'dist', 'county', 'wellbore', 'profile', 'filing', 'purpose', 'amend', 'total', 'depth', 'stacked', 'lateral', 'parent', 'well', 'dp', 'current', 'queue', 'usa', 'inc', 'llc', 'e&p', 'diamondback', 'chevron', 'pdeh', 'tgnr', 'panola', 'wildfire', 'energy', 'operating', 'burlington', 'resources', 'o&g', 'co', 'lp', 's', 'n', 'd', 'company', '135', '301', '467', '365', '255', 'far', 'cry', 'bucco', 'lov', 'unit', 'vital', 'signs', 'monty', 'west', 'presswood', 'oil', 'perseus', 'marian', 'yanta', 'n-tennant', 'usw', 'fox', 'ector', 'midland', 'loving', 'andrews', 'van', 'zandt', 'karnes', 'burleson', 'horizontal', 'vertical', 'new', 'drill', 'reenter', 'yes', 'no', 'mapping', 'drilling', 'permit', 'api', 'verification', 'fasken', '1a', '40', '54', '2', '41', 'w', '4', '46', '32', 'b', '35', '14', 'd', 'e', 'f', 'c', 'bs', 'an', 'hh', 'ls', 'ms', 'wb', 'tennant', 'c', 'he', '3bn', '4bn', '1jm', '1wa', '8002us', '8004us', '8006us', '1hh', '2hh', '2ls', '2ms', '2wb', '1u', '4he', '4101h', '44169', '44170', '37304', '30044', '38988', '38989', '38169', '628658', '217012', '646827', '741084', '148113', '102948', '109333', '923444']) and not str(value).strip().isdigit():
                            normalized['well_no'] = well_pattern.group()
                            logger.debug(f"Found well_no in field '{key}': {value} -> {well_pattern.group()}")
                            break
            
            # Extract operator number from operator name if present
            operator_name = item.get('Operator Name/Number', '')
            if operator_name and '(' in operator_name and ')' in operator_name:
                # Extract number from format like "COMPANY NAME (123456)"
                import re
                match = re.search(r'\((\d+)\)', operator_name)
                if match:
                    normalized['operator_number'] = match.group(1)
                    # Clean operator name by removing the number part
                    normalized['operator_name'] = re.sub(r'\s*\(\d+\)', '', operator_name).strip()
                else:
                    normalized['operator_name'] = operator_name
                    normalized['operator_number'] = None
            else:
                normalized['operator_name'] = operator_name
                normalized['operator_number'] = None
            
            # Only return if we have meaningful data
            if any(v for v in normalized.values() if v):
                return normalized
            else:
                return None
                
        except Exception as e:
            logger.warning(f"Error normalizing permit item: {e}")
            return None


class RRCW1Client:
    """
    Main client for RRC W-1 scraping with dual engine support.
    Tries RequestsEngine first, falls back to PlaywrightEngine on login redirect.
    """
    
    def __init__(self, base_url: str = "https://webapps.rrc.state.tx.us"):
        self.base_url = base_url
        self.timeout = int(os.getenv('SCRAPE_TIMEOUT_SECONDS', '30'))
        
        # Check if specific engine is requested
        engine_preference = os.getenv('SCRAPER_ENGINE', '').lower()
        if engine_preference == 'playwright':
            self.primary_engine = 'playwright'
        else:
            self.primary_engine = 'requests'
        
        logger.info(f"RRCW1Client initialized with primary engine: {self.primary_engine}")
    
    def fetch_all(self, begin: str, end: str, max_pages: Optional[int] = None) -> Dict[str, Any]:
        """
        Fetch all permits using the best available engine.
        
        Args:
            begin: Start date in MM/DD/YYYY format
            end: End date in MM/DD/YYYY format
            max_pages: Maximum number of pages to fetch (None for all)
            
        Returns:
            Dictionary with query results and metadata
        """
        logger.info(f"Starting RRC W-1 search: {begin} to {end}, max_pages={max_pages}")
        
        # Try primary engine first
        if self.primary_engine == 'requests':
            try:
                engine = RequestsEngine(self.base_url, self.timeout)
                result = engine.fetch_all(begin, end, max_pages)
                logger.info(f"RequestsEngine completed successfully: {result['count']} permits")
                return result
            except EngineRedirectToLogin as e:
                logger.warning(f"RequestsEngine redirected to login: {e}")
                logger.info("Falling back to PlaywrightEngine")
            except Exception as e:
                logger.warning(f"RequestsEngine failed: {e}")
                logger.info("Falling back to PlaywrightEngine")
        
        # Fallback to PlaywrightEngine
        try:
            engine = PlaywrightEngine(self.base_url, self.timeout * 1000)  # Convert to milliseconds
            result = engine.fetch_all(begin, end, max_pages)
            logger.info(f"PlaywrightEngine completed successfully: {result['count']} permits")
            return result
        except ImportError as e:
            logger.error(f"PlaywrightEngine failed due to missing dependencies: {e}")
            return {
                "source_root": self.base_url,
                "query_params": {"begin": begin, "end": end},
                "pages": 0,
                "count": 0,
                "items": [],
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "error": f"Playwright not properly installed. Run 'python setup_playwright.py' to fix. Original error: {e}",
                "success": False
            }
        except Exception as e:
            logger.error(f"PlaywrightEngine failed: {e}")
            return {
                "source_root": self.base_url,
                "query_params": {"begin": begin, "end": end},
                "pages": 0,
                "count": 0,
                "items": [],
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "error": str(e),
                "success": False
            }