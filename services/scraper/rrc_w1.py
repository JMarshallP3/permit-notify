"""
RRC W-1 Drilling Permits Scraper using requests.

This module provides a robust scraper for the Texas Railroad Commission
W-1 drilling permits search system using the proven requests-based approach.
"""

import os
import logging
import time
from datetime import datetime, timezone, date
from typing import Dict, List, Any, Optional, Tuple
from urllib.parse import urljoin
import re

logger = logging.getLogger(__name__)

class RRCScrapeError(Exception):
    """Custom exception for RRC scraping errors."""
    pass

class RRCW1Client:
    """
    Client for scraping RRC W-1 drilling permits using requests.
    
    This client uses the proven requests-based approach that successfully
    bypasses anti-bot measures and gets real permit data.
    """
    
    def __init__(self, base_url: str = "https://webapps.rrc.state.tx.us"):
        """
        Initialize the RRC W-1 client.
        
        Args:
            base_url: Base URL for RRC webapps
        """
        self.base_url = base_url.rstrip('/')
        self.dp_base = f"{self.base_url}/DP"
        self.init_url = f"{self.dp_base}/initializePublicQueryAction.do"
        
        self.user_agent = os.getenv(
            'USER_AGENT', 
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        self.timeout = int(os.getenv('SCRAPE_TIMEOUT_SECONDS', '30'))
        
        # Headers that work with RRC
        self.headers = {
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Referer": f"{self.dp_base}/",
        }
        
        logger.info(f"RRCW1Client initialized with base_url: {base_url}")
    
    def fetch_all(self, begin: str, end: str, max_pages: Optional[int] = None) -> Dict[str, Any]:
        """
        Fetch all permits for the given date range using the proven requests approach.
        
        Args:
            begin: Start date in MM/DD/YYYY format
            end: End date in MM/DD/YYYY format
            max_pages: Maximum number of pages to fetch (None for all)
            
        Returns:
            Dictionary with query results and metadata
        """
        logger.info(f"Starting RRC W-1 search: {begin} to {end}, max_pages={max_pages}")
        
        try:
            # Convert string dates to date objects
            begin_date = datetime.strptime(begin, "%m/%d/%Y").date()
            end_date = datetime.strptime(end, "%m/%d/%Y").date()
            
            # Use the proven scraping method
            permits = self._get_rrc_permits(begin_date, end_date, max_pages)
            
            logger.info(f"Successfully fetched {len(permits)} permits")
            
            return {
                "source_root": self.base_url,
                "query_params": {
                    "begin": begin,
                    "end": end
                },
                "pages": 1,  # We'll update this based on actual pagination
                "count": len(permits),
                "items": permits,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "method": "requests",
                "success": True
            }
            
        except Exception as e:
            logger.error(f"RRC W-1 search failed: {e}")
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
                "error": str(e),
                "success": False
            }
    
    def _get_rrc_permits(self, submit_begin: date, submit_end: date, max_pages: Optional[int] = None, pause: float = 0.6) -> List[Dict]:
        """
        Scrape the RRC 'Drilling Permit' public query by Submitted Date range.
        Returns a list of dicts (one per row). No DB. No side effects.

        Args:
            submit_begin: date for 'Submitted Date: Begin'
            submit_end:   date for 'Submitted Date: End'
            max_pages:    maximum number of pages to fetch (None for all)
            pause:        seconds to sleep between page fetches (be polite)

        Returns:
            List of permit dictionaries
        """
        from bs4 import BeautifulSoup
        import requests
        
        s = requests.Session()
        s.headers.update(self.headers)

        # 1) GET the query page to collect cookies + form + hidden fields
        logger.info(f"Loading initial form page: {self.init_url}")
        r = s.get(self.init_url, timeout=self.timeout)
        if r.status_code != 200:
            raise RRCScrapeError(f"Init GET failed: HTTP {r.status_code}")

        soup = BeautifulSoup(r.text, "lxml")
        form = soup.find("form")
        if not form:
            raise RRCScrapeError("Could not locate the query form on the page.")
        
        # Debug: Log page title and form info
        title = soup.find("title")
        logger.info(f"Page title: {title.get_text() if title else 'No title found'}")
        logger.info(f"Form action: {form.get('action', 'No action')}")
        logger.info(f"Form method: {form.get('method', 'No method')}")

        # Build form payload from existing inputs so we include required hidden fields.
        form_data = {}
        for inp in form.find_all(["input", "select", "textarea"]):
            name = inp.get("name")
            if not name:
                continue
            value = inp.get("value", "")
            form_data[name] = value

        # Fill the date range using the *correct* names as used by RRC
        date_str = submit_begin.strftime("%m/%d/%Y")
        form_data["submittedDateFrom"] = date_str
        form_data["submittedDateTo"] = submit_end.strftime("%m/%d/%Y")
        
        logger.info(f"Set date fields: submittedDateFrom={date_str}, submittedDateTo={submit_end.strftime('%m/%d/%Y')}")

        # Some forms use a specific submit button name/value; if present, keep it.
        # Otherwise, just POST the payload to the form action.
        action = form.get("action") or ""
        action_url = action if action.startswith("http") else f"{self.dp_base}/{action.lstrip('/') or 'changeQueryPageAction.do'}"

        # 2) POST the form to get page 1 of results
        logger.info(f"Submitting form to: {action_url}")
        logger.info(f"Form data keys: {list(form_data.keys())}")
        logger.info(f"Date fields: submittedDateFrom={form_data.get('submittedDateFrom')}, submittedDateTo={form_data.get('submittedDateTo')}")
        
        r = s.post(action_url, data=form_data, timeout=self.timeout)
        if r.status_code != 200:
            raise RRCScrapeError(f"Initial POST failed: HTTP {r.status_code}")
        
        # Debug: Log response page title
        response_soup = BeautifulSoup(r.text, "lxml")
        response_title = response_soup.find("title")
        logger.info(f"Response page title: {response_title.get_text() if response_title else 'No title found'}")

        permits: List[Dict] = []
        page_html = r.text
        page_count = 0

        # 3) Iterate pages
        while True:
            page_count += 1
            if max_pages and page_count > max_pages:
                logger.info(f"Reached max_pages limit: {max_pages}")
                break
                
            page_soup = BeautifulSoup(page_html, "lxml")

            # Parse the table with the most rows (RRC sometimes nests multiple tables)
            table = self._find_results_table(page_soup)
            if not table:
                # If first page yields no table, surface helpful info
                if not permits:
                    raise RRCScrapeError("No results table found. Check date range or form fields.")
                break

            # Extract rows
            rows = table.find_all("tr")
            header, data_rows = self._split_header_rows(rows)
            header_text = [c.get_text(strip=True) for c in header]
            
            logger.info(f"Processing page {page_count}: {len(data_rows)} data rows")
            
            for tr in data_rows:
                cols = [c.get_text(separator=" ", strip=True) for c in tr.find_all(["td", "th"])]
                if not cols or all(not c for c in cols):
                    continue
                    
                # Build a dict keyed by header when possible; fallback to index
                item = {}
                if header_text and len(header_text) == len(cols):
                    for k, v in zip(header_text, cols):
                        if k:  # Only add non-empty keys
                            item[k] = v
                else:
                    for i, v in enumerate(cols):
                        item[f"col_{i+1}"] = v
                        
                # Also try to capture the drill-down link in the row, if any
                link = tr.find("a", href=True)
                if link:
                    item["detail_link"] = link["href"] if link["href"].startswith("http") else f"{self.dp_base}/{link['href'].lstrip('/')}"
                
                # Normalize the item to our database schema
                normalized_item = self._normalize_permit_item(item)
                if normalized_item:
                    permits.append(normalized_item)

            # Find and follow "Next" (pager.offset) if it exists
            next_href = self._find_next_link(page_soup)
            if not next_href:
                logger.info("No more pages found")
                break
                
            next_url = next_href if next_href.startswith("http") else f"{self.dp_base}/{next_href.lstrip('/')}"
            logger.info(f"Following next page: {next_url}")
            
            time.sleep(pause)
            r = s.get(next_url, timeout=self.timeout)
            if r.status_code != 200:
                # If "Next" fails, we just stop gracefully
                logger.warning(f"Next page failed: HTTP {r.status_code}")
                break
            page_html = r.text

        return permits
    
    def _find_results_table(self, soup) -> Optional[Any]:
        """Pick the largest-rows table as the results table."""
        from bs4 import BeautifulSoup
        
        best = None
        best_rows = 0
        for tbl in soup.find_all("table"):
            rows = tbl.find_all("tr")
            if len(rows) > best_rows:
                best = tbl
                best_rows = len(rows)
        return best if best_rows > 1 else None

    def _split_header_rows(self, rows):
        """Return (header_cells, data_rows). If no header, header_cells=[], data_rows=rows."""
        if not rows:
            return [], []
        # Heuristic: first row is header if it uses <th> or looks like a label row
        ths = rows[0].find_all("th")
        if ths:
            return ths, rows[1:]
        # Fallback: treat first row as header if all cells are bold-ish labels
        tds = rows[0].find_all("td")
        if tds and all(td.get_text(strip=True) for td in tds):
            return tds, rows[1:]
        return [], rows

    def _find_next_link(self, soup) -> Optional[str]:
        """Find the 'Next' pagination link (RRC uses pager.offset in the href)."""
        for a in soup.find_all("a", href=True):
            txt = a.get_text(strip=True).lower()
            href = a["href"]
            if "pager.offset" in href and ("next" in txt or ">" in txt):
                return href
        # Sometimes the 'Next' anchor text isn't literally 'Next'; fallback to any anchor with a higher offset
        offsets = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "pager.offset" in href:
                try:
                    # crude parse: ...pager.offset=120...
                    part = href.split("pager.offset=")[1].split("&")[0]
                    offsets.append((int(part), href))
                except Exception:
                    pass
        if offsets:
            # choose the largest offset as "next-ish"
            offsets.sort()
            return offsets[-1][1]
        return None

    def _normalize_permit_item(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Normalize a permit item to our database schema.
        
        Args:
            item: Raw permit data from RRC
            
        Returns:
            Normalized permit dictionary or None if invalid
        """
        try:
            normalized = {}
            
            # Map RRC fields to our database schema
            field_mapping = {
                'Status Date': 'status_date',
                'Status #': 'status',
                'API No.': 'api_no',
                'Operator Name/Number': 'operator',
                'Lease Name': 'lease_name',
                'Well #': 'well_id',
                'Dist.': 'district',
                'County': 'county',
                'Wellbore Profile': 'wellbore_profile',
                'Filing Purpose': 'filing_purpose',
                'Amend': 'amended',
                'Total Depth': 'total_depth',
                'Stacked Lateral Parent Well DP': 'stacked_parent',
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
            
            # Only return if we have at least some meaningful data
            if any(v for v in normalized.values() if v):
                return normalized
            else:
                return None
                
        except Exception as e:
            logger.warning(f"Error normalizing permit item: {e}")
            return None
    