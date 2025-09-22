"""
Production-grade scraper for RRC W-1 search system.
Handles form discovery, submission, and pagination parsing.
"""

import os
import re
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class RRCW1Client:
    """
    Client for scraping RRC W-1 drilling permit search results.
    Handles form discovery, submission, and pagination.
    """
    
    def __init__(
        self,
        base_url: str = "https://webapps.rrc.state.tx.us/DP",
        timeout: int = 20,
        user_agent: Optional[str] = None,
    ):
        """
        Initialize the RRC W-1 client.
        
        Args:
            base_url: Base URL for RRC W-1 system
            timeout: Request timeout in seconds
            user_agent: Custom user agent (defaults to env var or default)
        """
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        
        # Set up session with headers
        self.session = requests.Session()
        
        # User agent from env or default
        if user_agent:
            self.user_agent = user_agent
        else:
            self.user_agent = os.getenv(
                "USER_AGENT", 
                "PermitTrackerBot/1.0 (+mailto:marshall@craatx.com)"
            )
        
        # Set session headers
        self.session.headers.update({
            'User-Agent': self.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        logger.info(f"RRCW1Client initialized with base_url={self.base_url}, timeout={self.timeout}")
    
    def _get(self, path_or_url: str, **kwargs) -> requests.Response:
        """
        Make a GET request, handling absolute/relative URLs.
        
        Args:
            path_or_url: URL path or full URL
            **kwargs: Additional arguments for requests.get
            
        Returns:
            Response object
            
        Raises:
            requests.HTTPError: For non-2xx status codes
        """
        # Handle absolute vs relative URLs
        if path_or_url.startswith('http'):
            url = path_or_url
        else:
            url = urljoin(self.base_url, path_or_url)
        
        # Set default timeout
        kwargs.setdefault('timeout', self.timeout)
        
        logger.info(f"GET {url}")
        response = self.session.get(url, **kwargs)
        response.raise_for_status()
        logger.info(f"GET {url} -> {response.status_code}")
        
        return response
    
    def _post(self, path_or_url: str, data: Dict[str, Any], **kwargs) -> requests.Response:
        """
        Make a POST request, handling absolute/relative URLs.
        
        Args:
            path_or_url: URL path or full URL
            data: Form data to POST
            **kwargs: Additional arguments for requests.post
            
        Returns:
            Response object
            
        Raises:
            requests.HTTPError: For non-2xx status codes
        """
        # Handle absolute vs relative URLs
        if path_or_url.startswith('http'):
            url = path_or_url
        else:
            url = urljoin(self.base_url, path_or_url)
        
        # Set default timeout
        kwargs.setdefault('timeout', self.timeout)
        
        logger.info(f"POST {url} with {len(data)} form fields")
        response = self.session.post(url, data=data, **kwargs)
        response.raise_for_status()
        logger.info(f"POST {url} -> {response.status_code}")
        
        return response
    
    def _soup(self, html: str) -> BeautifulSoup:
        """
        Parse HTML into BeautifulSoup object.
        
        Args:
            html: HTML content string
            
        Returns:
            BeautifulSoup object
        """
        return BeautifulSoup(html, 'html.parser')
    
    def load_form(self) -> Dict[str, Any]:
        """
        Load the W-1 query form and discover all input fields.
        
        Returns:
            Dictionary with form action URL, field values, and soup object
        """
        logger.info("Loading W-1 query form...")
        
        # GET the initialize page with Referer header
        response = self._get(
            "/initializePublicQueryAction.do",
            headers={'Referer': self.base_url}
        )
        
        soup = self._soup(response.text)
        
        # Find the first form (should be the W-1 query form)
        form = soup.find('form')
        if not form:
            raise ValueError("No form found on the initialize page")
        
        # Get form action URL (absolute)
        action = form.get('action', '')
        if action:
            action_url = urljoin(self.base_url, action)
        else:
            action_url = self.base_url
        
        # Collect all form fields
        fields = {}
        
        # Process input elements
        for input_elem in form.find_all('input'):
            name = input_elem.get('name')
            if name:
                value = input_elem.get('value', '')
                fields[name] = value
        
        # Process select elements
        for select_elem in form.find_all('select'):
            name = select_elem.get('name')
            if name:
                # Find selected option or first option
                selected = select_elem.find('option', selected=True)
                if selected:
                    fields[name] = selected.get('value', '')
                else:
                    first_option = select_elem.find('option')
                    fields[name] = first_option.get('value', '') if first_option else ''
        
        # Process textarea elements
        for textarea_elem in form.find_all('textarea'):
            name = textarea_elem.get('name')
            if name:
                fields[name] = textarea_elem.get_text(strip=True)
        
        logger.info(f"Discovered form with action={action_url} and {len(fields)} fields")
        logger.debug(f"Form fields: {list(fields.keys())}")
        
        return {
            "action": action_url,
            "fields": fields,
            "soup": soup
        }
    
    def _infer_submitted_date_names(self, soup: BeautifulSoup) -> Tuple[str, str]:
        """
        Robustly identify the two date inputs for 'Submitted Date: Begin' and 'End'.
        
        Args:
            soup: BeautifulSoup object of the form page
            
        Returns:
            Tuple of (begin_field_name, end_field_name)
            
        Raises:
            ValueError: If date fields cannot be identified
        """
        logger.info("Inferring submitted date field names...")
        
        # Strategy 1: Find label or text containing 'Submitted Date'
        submitted_date_elements = soup.find_all(text=re.compile(r'submitted\s+date', re.IGNORECASE))
        
        begin_name = None
        end_name = None
        
        for element in submitted_date_elements:
            # Look for nearby input elements
            parent = element.parent
            if parent:
                # Find all input elements in the same container or nearby
                inputs = parent.find_all('input', type='text')
                if len(inputs) >= 2:
                    begin_name = inputs[0].get('name')
                    end_name = inputs[1].get('name')
                    break
        
        # Strategy 2: Search all inputs for date-related names
        if not begin_name or not end_name:
            all_inputs = soup.find_all('input', type='text')
            date_inputs = []
            
            for inp in all_inputs:
                name = inp.get('name', '').lower()
                if any(keyword in name for keyword in ['submit', 'date', 'begin', 'end']):
                    date_inputs.append(inp.get('name'))
            
            if len(date_inputs) >= 2:
                begin_name = date_inputs[0]
                end_name = date_inputs[1]
        
        # Strategy 3: Look for specific patterns
        if not begin_name or not end_name:
            # Common patterns for date fields
            patterns = [
                (r'submit.*begin', r'submit.*end'),
                (r'date.*begin', r'date.*end'),
                (r'from.*date', r'to.*date'),
                (r'start.*date', r'end.*date'),
            ]
            
            all_inputs = soup.find_all('input', type='text')
            input_names = [inp.get('name', '') for inp in all_inputs]
            
            for begin_pattern, end_pattern in patterns:
                begin_matches = [name for name in input_names if re.search(begin_pattern, name, re.IGNORECASE)]
                end_matches = [name for name in input_names if re.search(end_pattern, name, re.IGNORECASE)]
                
                if begin_matches and end_matches:
                    begin_name = begin_matches[0]
                    end_name = end_matches[0]
                    break
        
        if not begin_name or not end_name:
            # Debug information
            all_inputs = soup.find_all('input')
            input_info = []
            for inp in all_inputs:
                name = inp.get('name', '')
                input_type = inp.get('type', '')
                input_info.append(f"{name} ({input_type})")
            
            raise ValueError(
                f"Could not identify submitted date field names. "
                f"Found inputs: {input_info}"
            )
        
        logger.info(f"Identified date fields: begin='{begin_name}', end='{end_name}'")
        return begin_name, end_name
    
    def query(self, begin_mmddyyyy: str, end_mmddyyyy: str) -> Dict[str, Any]:
        """
        Submit a query with date range.
        
        Args:
            begin_mmddyyyy: Start date in MM/DD/YYYY format
            end_mmddyyyy: End date in MM/DD/YYYY format
            
        Returns:
            Dictionary with first page HTML and action URL
        """
        logger.info(f"Submitting query for date range: {begin_mmddyyyy} to {end_mmddyyyy}")
        
        # Load form and discover fields
        form_data = self.load_form()
        fields = form_data["fields"].copy()
        action_url = form_data["action"]
        
        # Infer date field names
        begin_name, end_name = self._infer_submitted_date_names(form_data["soup"])
        
        # Set date fields
        fields[begin_name] = begin_mmddyyyy
        fields[end_name] = end_mmddyyyy
        
        # Submit the form
        response = self._post(action_url, fields)
        
        return {
            "first_page_html": response.text,
            "action_url": action_url
        }
    
    def _parse_table(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Parse the results table and extract normalized rows.
        
        Args:
            soup: BeautifulSoup object of results page
            
        Returns:
            List of normalized row dictionaries
        """
        logger.info("Parsing results table...")
        
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
            elif 'status' in header_lower and '#' in header:
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
    
    def _next_page_url(self, soup: BeautifulSoup, current_url: str) -> Optional[str]:
        """
        Detect pagination and return next page URL.
        
        Args:
            soup: BeautifulSoup object of current page
            current_url: Current page URL
            
        Returns:
            Next page URL or None if last page
        """
        logger.info("Looking for next page...")
        
        # Strategy 1: Look for "Next >" link
        next_links = soup.find_all('a', text=re.compile(r'next\s*>', re.IGNORECASE))
        if next_links:
            next_link = next_links[0]
            href = next_link.get('href')
            if href:
                next_url = urljoin(current_url, href)
                logger.info(f"Found 'Next >' link: {next_url}")
                return next_url
        
        # Strategy 2: Look for pager.offset pattern
        all_links = soup.find_all('a', href=True)
        for link in all_links:
            href = link.get('href', '')
            if 'pager.offset' in href:
                # Extract current offset
                match = re.search(r'pager\.offset=(\d+)', href)
                if match:
                    current_offset = int(match.group(1))
                    next_offset = current_offset + 20  # Assuming 20 items per page
                    next_href = re.sub(r'pager\.offset=\d+', f'pager.offset={next_offset}', href)
                    next_url = urljoin(current_url, next_href)
                    logger.info(f"Found pager.offset link: {next_url}")
                    return next_url
        
        logger.info("No next page found")
        return None
    
    def fetch_all(
        self,
        begin_mmddyyyy: str,
        end_mmddyyyy: str,
        max_pages: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Fetch all results across all pages.
        
        Args:
            begin_mmddyyyy: Start date in MM/DD/YYYY format
            end_mmddyyyy: End date in MM/DD/YYYY format
            max_pages: Maximum number of pages to fetch (None for all)
            
        Returns:
            Dictionary with query results and metadata
        """
        logger.info(f"Fetching all results for {begin_mmddyyyy} to {end_mmddyyyy}")
        
        # Submit initial query
        query_result = self.query(begin_mmddyyyy, end_mmddyyyy)
        
        all_items = []
        current_html = query_result["first_page_html"]
        current_url = query_result["action_url"]
        page_count = 0
        
        while current_html and (max_pages is None or page_count < max_pages):
            page_count += 1
            logger.info(f"Processing page {page_count}")
            
            # Parse current page
            soup = self._soup(current_html)
            page_items = self._parse_table(soup)
            all_items.extend(page_items)
            
            logger.info(f"Page {page_count}: found {len(page_items)} items")
            
            # Look for next page
            next_url = self._next_page_url(soup, current_url)
            if not next_url:
                logger.info("No more pages found")
                break
            
            # Fetch next page
            try:
                response = self._get(next_url)
                current_html = response.text
                current_url = next_url
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
