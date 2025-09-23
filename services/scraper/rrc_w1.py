"""
RRC W-1 Drilling Permits Scraper using Playwright.

This module provides a robust scraper for the Texas Railroad Commission
W-1 drilling permits search system using Playwright for better form handling
and anti-bot evasion.
"""

import os
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from urllib.parse import urljoin
import re

logger = logging.getLogger(__name__)

class RRCW1Client:
    """
    Client for scraping RRC W-1 drilling permits using Playwright.
    
    This client uses Playwright to handle dynamic forms and bypass
    anti-bot measures that were causing login redirects with requests.
    """
    
    def __init__(self, base_url: str = "https://webapps.rrc.state.tx.us"):
        """
        Initialize the RRC W-1 client.
        
        Args:
            base_url: Base URL for RRC webapps
        """
        self.base_url = base_url
        self.user_agent = os.getenv(
            'USER_AGENT', 
            'PermitNotifyBot/1.0 (contact: marshall@craatx.com)'
        )
        self.timeout = int(os.getenv('SCRAPE_TIMEOUT_SECONDS', '30'))
        
        # Playwright will be initialized lazily
        self._playwright = None
        self._browser = None
        self._page = None
        
        logger.info(f"RRCW1Client initialized with base_url: {base_url}")
    
    def _ensure_playwright(self):
        """Ensure Playwright is initialized."""
        if self._playwright is None:
            try:
                from playwright.sync_api import sync_playwright
                logger.info("Starting Playwright initialization...")
                
                # Get the actual Playwright object from the context manager
                self._playwright = sync_playwright().__enter__()
                logger.info("Playwright context created")
                
                self._browser = self._playwright.chromium.launch(
                    headless=True,
                    args=[
                        '--no-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-gpu',
                        '--disable-web-security',
                        '--disable-features=VizDisplayCompositor',
                        '--disable-background-timer-throttling',
                        '--disable-backgrounding-occluded-windows',
                        '--disable-renderer-backgrounding'
                    ]
                )
                logger.info("Chromium browser launched")
                
                self._page = self._browser.new_page()
                logger.info("New page created")
                
                # Set user agent and viewport
                self._page.set_user_agent(self.user_agent)
                self._page.set_viewport_size({"width": 1920, "height": 1080})
                
                logger.info("Playwright initialized successfully")
                
            except Exception as e:
                logger.error(f"Failed to initialize Playwright: {e}")
                logger.error(f"Playwright error type: {type(e).__name__}")
                import traceback
                logger.error(f"Playwright traceback: {traceback.format_exc()}")
                raise
    
    def __del__(self):
        """Clean up Playwright resources."""
        try:
            if self._browser:
                self._browser.close()
            if self._playwright:
                # Properly exit the context manager
                self._playwright.__exit__(None, None, None)
        except:
            pass
    
    def fetch_all(self, begin: str, end: str, max_pages: Optional[int] = None) -> Dict[str, Any]:
        """
        Fetch all permits for the given date range using Playwright.
        
        Args:
            begin: Start date in MM/DD/YYYY format
            end: End date in MM/DD/YYYY format
            max_pages: Maximum number of pages to fetch (None for all)
            
        Returns:
            Dictionary with query results and metadata
        """
        logger.info(f"Starting RRC W-1 search: {begin} to {end}, max_pages={max_pages}")
        
        try:
            self._ensure_playwright()
            return self._playwright_fetch_all(begin, end, max_pages)
            
        except Exception as e:
            logger.error(f"Playwright fetch failed: {e}")
            # Fallback to requests-based approach
            return self._requests_fallback_fetch_all(begin, end, max_pages)
    
    def _playwright_fetch_all(self, begin: str, end: str, max_pages: Optional[int] = None) -> Dict[str, Any]:
        """
        Fetch permits using Playwright with robust form handling.
        """
        logger.info("Using Playwright for RRC W-1 search")
        
        try:
            # Navigate to the query page
            query_url = f"{self.base_url}/DP/initializePublicQueryAction.do"
            logger.info(f"Navigating to: {query_url}")
            
            self._page.goto(query_url, wait_until="domcontentloaded", timeout=self.timeout * 1000)
            
            # Wait for the form to be ready
            self._page.wait_for_load_state("networkidle")
            
            # Fill the date fields using multiple strategies
            self._fill_date_fields(begin, end)
            
            # Submit the form using multiple strategies
            self._submit_form()
            
            # Wait for results to load
            self._page.wait_for_load_state("networkidle")
            
            logger.info(f"Results loaded at: {self._page.url}")
            
            # Parse the results
            permits = self._parse_results_page()
            
            # Handle pagination if needed
            total_pages = 1
            if max_pages is None or max_pages > 1:
                total_pages = self._handle_pagination(max_pages)
            
            logger.info(f"Found {len(permits)} permits across {total_pages} pages")
            
            return {
                "source_root": self.base_url,
                "query_params": {
                    "begin": begin,
                    "end": end
                },
                "pages": total_pages,
                "count": len(permits),
                "items": permits,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "method": "playwright",
                "results_url": self._page.url
            }
            
        except Exception as e:
            logger.error(f"Playwright search failed: {e}")
            raise
    
    def _fill_date_fields(self, begin: str, end: str):
        """Fill the date fields using multiple strategies."""
        logger.info(f"Filling date fields: {begin} to {end}")
        
        # Strategy 1: Try to fill by visible labels
        try:
            self._page.get_by_label("Submitted Date From").fill(begin)
            self._page.get_by_label("Submitted Date To").fill(end)
            logger.info("Successfully filled date fields by labels")
            return
        except Exception as e:
            logger.debug(f"Label-based filling failed: {e}")
        
        # Strategy 2: Try common name/id attributes
        try:
            # Look for inputs with "submitted" and "From"/"To" in name
            from_input = self._page.locator('input[name*="submitted"][name*="From" i]').first
            to_input = self._page.locator('input[name*="submitted"][name*="To" i]').first
            
            if from_input.is_visible() and to_input.is_visible():
                from_input.fill(begin)
                to_input.fill(end)
                logger.info("Successfully filled date fields by name patterns")
                return
        except Exception as e:
            logger.debug(f"Name pattern filling failed: {e}")
        
        # Strategy 3: Try specific RRC W-1 field names
        try:
            submit_start = self._page.locator('input[name="submitStart"]')
            submit_end = self._page.locator('input[name="submitEnd"]')
            
            if submit_start.is_visible() and submit_end.is_visible():
                submit_start.fill(begin)
                submit_end.fill(end)
                logger.info("Successfully filled date fields by RRC W-1 names")
                return
        except Exception as e:
            logger.debug(f"RRC W-1 name filling failed: {e}")
        
        # Strategy 4: Try any text inputs (fallback)
        try:
            text_inputs = self._page.locator('input[type="text"]')
            if text_inputs.count() >= 2:
                text_inputs.nth(0).fill(begin)
                text_inputs.nth(1).fill(end)
                logger.info("Successfully filled date fields by text input order")
                return
        except Exception as e:
            logger.debug(f"Text input filling failed: {e}")
        
        logger.warning("Could not fill date fields with any strategy")
    
    def _submit_form(self):
        """Submit the form using multiple strategies."""
        logger.info("Attempting to submit form")
        
        # Strategy 1: Try various submit button selectors
        submit_selectors = [
            'button:has-text("Submit")',
            'input[type="submit"]',
            'input[type="button"][value*="Submit" i]',
            'input[value="Submit"]',
            'a:has-text("Submit")',
            'input[name="submit"]',
            'button[type="submit"]'
        ]
        
        for selector in submit_selectors:
            try:
                submit_btn = self._page.locator(selector).first
                if submit_btn.is_visible():
                    submit_btn.click()
                    logger.info(f"Successfully submitted form using selector: {selector}")
                    return
            except Exception as e:
                logger.debug(f"Submit selector {selector} failed: {e}")
        
        # Strategy 2: Press Enter on last field
        try:
            last_input = self._page.locator('input[type="text"]').last
            if last_input.is_visible():
                last_input.focus()
                self._page.keyboard.press("Enter")
                logger.info("Successfully submitted form using Enter key")
                return
        except Exception as e:
            logger.debug(f"Enter key submission failed: {e}")
        
        # Strategy 3: Programmatic form submit
        try:
            self._page.evaluate("""
                const form = document.querySelector('form');
                if (form) form.submit();
            """)
            logger.info("Successfully submitted form programmatically")
            return
        except Exception as e:
            logger.debug(f"Programmatic submission failed: {e}")
        
        logger.warning("Could not submit form with any strategy")
    
    def _parse_results_page(self) -> List[Dict[str, Any]]:
        """Parse the results page to extract permit data."""
        logger.info("Parsing results page")
        
        try:
            # Find the main results table
            tables = self._page.locator("table")
            results_table = None
            
            for i in range(tables.count()):
                table = tables.nth(i)
                # Check if this table has headers that suggest permit data
                headers = table.locator("th, td").first
                if headers.is_visible():
                    header_text = headers.text_content().lower()
                    if any(keyword in header_text for keyword in ['status', 'operator', 'county', 'permit', 'well']):
                        results_table = table
                        break
            
            if not results_table:
                logger.warning("No results table found")
                return []
            
            # Extract headers
            header_row = results_table.locator("tr").first
            headers = [th.text_content().strip() for th in header_row.locator("th, td").all()]
            logger.info(f"Found table headers: {headers}")
            
            # Create header mapping
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
            permits = []
            data_rows = results_table.locator("tr").all()[1:]  # Skip header row
            
            for row_idx, row in enumerate(data_rows):
                try:
                    cells = row.locator("td, th").all()
                    if len(cells) != len(headers):
                        logger.warning(f"Row {row_idx} has {len(cells)} cells, expected {len(headers)}")
                        continue
                    
                    # Build permit dictionary
                    permit = {}
                    for i, cell in enumerate(cells):
                        cell_text = cell.text_content().strip()
                        if i in header_mapping:
                            field_name = header_mapping[i]
                            permit[field_name] = cell_text if cell_text else None
                    
                    # Only add if we have some data
                    if permit:
                        permits.append(permit)
                        
                except Exception as e:
                    logger.warning(f"Error parsing row {row_idx}: {e}")
                    continue
            
            logger.info(f"Successfully parsed {len(permits)} permits")
            return permits
            
        except Exception as e:
            logger.error(f"Error parsing results page: {e}")
            return []
    
    def _handle_pagination(self, max_pages: Optional[int]) -> int:
        """Handle pagination to fetch additional pages."""
        logger.info("Handling pagination")
        
        total_pages = 1
        current_page = 1
        
        while (max_pages is None or current_page < max_pages):
            # Look for pagination controls
            next_selectors = [
                'a:has-text("Next >")',
                'a:has-text("Next")',
                'input[name="pager.offset"]',
                'button:has-text("Next")'
            ]
            
            next_link = None
            for selector in next_selectors:
                try:
                    link = self._page.locator(selector).first
                    if link.is_visible():
                        next_link = link
                        break
                except:
                    continue
            
            if not next_link:
                logger.info("No more pages found")
                break
            
            try:
                # Click the next page link
                next_link.click()
                self._page.wait_for_load_state("networkidle")
                current_page += 1
                total_pages = current_page
                
                logger.info(f"Navigated to page {current_page}")
                
                # Parse additional permits from this page
                page_permits = self._parse_results_page()
                if page_permits:
                    # Add to our results (this would need to be handled by the caller)
                    logger.info(f"Found {len(page_permits)} permits on page {current_page}")
                else:
                    logger.info(f"No permits found on page {current_page}, stopping pagination")
                    break
                    
            except Exception as e:
                logger.warning(f"Error navigating to next page: {e}")
                break
        
        return total_pages
    
    def _requests_fallback_fetch_all(self, begin: str, end: str, max_pages: Optional[int] = None) -> Dict[str, Any]:
        """
        Enhanced requests-based fallback that actually scrapes permit data.
        """
        logger.info("Using enhanced requests-based fallback for RRC W-1 search")
        
        try:
            import requests
            from bs4 import BeautifulSoup
            from urllib.parse import urljoin
            
            # Create session with proper headers
            session = requests.Session()
            session.headers.update({
                'User-Agent': self.user_agent,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Referer': f"{self.base_url}/DP/",
            })
            
            # Step 1: Load the initial form page
            form_url = f"{self.base_url}/DP/initializePublicQueryAction.do"
            logger.info(f"Loading form page: {form_url}")
            
            response = session.get(form_url, timeout=self.timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Step 2: Find and parse the search form
            form = soup.find('form')
            if not form:
                logger.warning("No form found on page")
                return self._create_fallback_response(begin, end, [], "No search form found")
            
            # Extract form action and fields
            form_action = form.get('action', '')
            if form_action:
                submit_url = urljoin(form_url, form_action)
            else:
                submit_url = form_url
            
            logger.info(f"Form action URL: {submit_url}")
            
            # Step 3: Build form data
            form_data = {}
            
            # Extract all form fields
            for input_elem in form.find_all(['input', 'select', 'textarea']):
                name = input_elem.get('name')
                if name:
                    input_type = input_elem.get('type', 'text')
                    
                    if input_type in ['text', 'hidden', 'password']:
                        form_data[name] = input_elem.get('value', '')
                    elif input_type in ['checkbox', 'radio']:
                        if input_elem.get('checked'):
                            form_data[name] = input_elem.get('value', 'on')
                    elif input_elem.name == 'select':
                        selected = input_elem.find('option', selected=True)
                        if selected:
                            form_data[name] = selected.get('value', '')
                        else:
                            form_data[name] = ''
                    elif input_elem.name == 'textarea':
                        form_data[name] = input_elem.get_text() or ''
            
            # Step 4: Set the date range fields
            # Look for the actual RRC W-1 form fields
            date_fields = self._find_rrc_w1_date_fields(soup)
            if date_fields:
                begin_field, end_field = date_fields
                form_data[begin_field] = begin
                form_data[end_field] = end
                logger.info(f"Set RRC W-1 date fields: {begin_field}={begin}, {end_field}={end}")
            else:
                # Try common RRC W-1 field names
                form_data['submitStart'] = begin
                form_data['submitEnd'] = end
                logger.info(f"Using fallback date fields: submitStart={begin}, submitEnd={end}")
            
            # Add any required hidden fields or default selections
            self._add_required_form_fields(form_data, soup)
            
            # Step 5: Submit the form with proper submit button
            logger.info(f"Submitting form to: {submit_url}")
            
            # Add submit button to form data
            submit_button = self._find_submit_button(soup)
            if submit_button:
                submit_name = submit_button.get('name')
                submit_value = submit_button.get('value', 'Submit')
                if submit_name:
                    form_data[submit_name] = submit_value
                    logger.info(f"Added submit button: {submit_name}={submit_value}")
            
            # Submit the form
            response = session.post(submit_url, data=form_data, timeout=self.timeout)
            response.raise_for_status()
            
            # Step 6: Parse the results
            results_soup = BeautifulSoup(response.text, 'html.parser')
            permits = self._parse_results_table(results_soup)
            
            logger.info(f"Found {len(permits)} permits in results")
            
            return {
                "source_root": self.base_url,
                "query_params": {
                    "begin": begin,
                    "end": end
                },
                "pages": 1,
                "count": len(permits),
                "items": permits,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "warning": "Playwright failed, using enhanced requests fallback.",
                "fallback_info": {
                    "forms_found": len(soup.find_all('form')),
                    "tables_found": len(results_soup.find_all('table')),
                    "page_title": results_soup.title.string if results_soup.title else "W1 Search Results"
                }
            }
            
        except Exception as e:
            logger.error(f"Enhanced requests fallback failed: {e}")
            return self._create_fallback_response(begin, end, [], f"Requests fallback failed: {str(e)}")
    
    def _find_rrc_w1_date_fields(self, soup) -> Optional[Tuple[str, str]]:
        """Find the correct field names for RRC W-1 date inputs."""
        try:
            from bs4 import BeautifulSoup
            
            # Look for RRC W-1 specific date fields
            # Common RRC W-1 field names for submitted date range
            possible_begin_fields = ['submitStart', 'submittedDateBegin', 'beginDate', 'startDate']
            possible_end_fields = ['submitEnd', 'submittedDateEnd', 'endDate', 'stopDate']
            
            # First, try to find fields by looking for text inputs near date-related labels
            inputs = soup.find_all('input', {'type': 'text'})
            
            for input_elem in inputs:
                name = input_elem.get('name', '').lower()
                
                # Check if this looks like a begin date field
                if any(field.lower() in name for field in possible_begin_fields):
                    begin_name = input_elem.get('name')
                    
                    # Look for the corresponding end field
                    for end_input in inputs:
                        end_name = end_input.get('name', '').lower()
                        if any(field.lower() in end_name for field in possible_end_fields):
                            return (begin_name, end_input.get('name'))
            
            # Fallback: look for any text inputs that might be date fields
            text_inputs = [inp for inp in inputs if inp.get('name')]
            if len(text_inputs) >= 2:
                # Assume first two text inputs are begin/end dates
                return (text_inputs[0].get('name'), text_inputs[1].get('name'))
            
            return None
            
        except Exception as e:
            logger.warning(f"Error finding RRC W-1 date fields: {e}")
            return None
    
    def _add_required_form_fields(self, form_data: Dict[str, str], soup) -> None:
        """Add any required hidden fields or default selections for RRC W-1 form."""
        try:
            # Look for hidden fields that might be required
            hidden_inputs = soup.find_all('input', {'type': 'hidden'})
            for hidden_input in hidden_inputs:
                name = hidden_input.get('name')
                value = hidden_input.get('value', '')
                if name and name not in form_data:
                    form_data[name] = value
                    logger.debug(f"Added hidden field: {name}={value}")
            
            # Look for required select fields that need default values
            selects = soup.find_all('select')
            for select in selects:
                name = select.get('name')
                if name and name not in form_data:
                    # Try to find a default option
                    default_option = select.find('option', selected=True)
                    if default_option:
                        form_data[name] = default_option.get('value', '')
                        logger.debug(f"Added default select: {name}={form_data[name]}")
                    else:
                        # If no default, try the first option
                        first_option = select.find('option')
                        if first_option:
                            form_data[name] = first_option.get('value', '')
                            logger.debug(f"Added first select option: {name}={form_data[name]}")
            
        except Exception as e:
            logger.warning(f"Error adding required form fields: {e}")
    
    def _find_submit_button(self, soup) -> Optional[Any]:
        """Find the submit button for the RRC W-1 form."""
        try:
            # Look for submit buttons in the form
            form = soup.find('form')
            if not form:
                return None
            
            # Look for input submit buttons
            submit_inputs = form.find_all('input', {'type': 'submit'})
            if submit_inputs:
                # Prefer buttons with "Submit" text
                for submit_input in submit_inputs:
                    value = submit_input.get('value', '').lower()
                    if 'submit' in value:
                        logger.info(f"Found submit button: {submit_input.get('name')}={submit_input.get('value')}")
                        return submit_input
                # If no "Submit" button, use the first submit input
                logger.info(f"Using first submit button: {submit_inputs[0].get('name')}={submit_inputs[0].get('value')}")
                return submit_inputs[0]
            
            # Look for button elements
            submit_buttons = form.find_all('button', {'type': 'submit'})
            if submit_buttons:
                logger.info(f"Found submit button element: {submit_buttons[0].get('name')}")
                return submit_buttons[0]
            
            # Look for any button that might be a submit button
            buttons = form.find_all('button')
            for button in buttons:
                button_text = button.get_text().lower()
                if 'submit' in button_text:
                    logger.info(f"Found submit button by text: {button.get('name')}")
                    return button
            
            logger.warning("No submit button found in form")
            return None
            
        except Exception as e:
            logger.warning(f"Error finding submit button: {e}")
            return None
    
    def _parse_results_table(self, soup) -> List[Dict[str, Any]]:
        """Parse the results table to extract permit data."""
        try:
            # Find the main results table
            tables = soup.find_all('table')
            results_table = None
            
            for table in tables:
                # Look for a table with headers that suggest it contains permit data
                headers = table.find_all('th')
                if headers:
                    header_text = ' '.join([h.get_text().lower() for h in headers])
                    if any(keyword in header_text for keyword in ['status', 'operator', 'county', 'permit', 'well']):
                        results_table = table
                        break
            
            if not results_table:
                logger.warning("No results table found")
                return []
            
            # Extract headers
            header_row = results_table.find('tr')
            if not header_row:
                logger.warning("No header row found in results table")
                return []
            
            headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
            logger.info(f"Found table headers: {headers}")
            
            # Create header mapping
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
            permits = []
            data_rows = results_table.find_all('tr')[1:]  # Skip header row
            
            for row_idx, row in enumerate(data_rows):
                try:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) != len(headers):
                        logger.warning(f"Row {row_idx} has {len(cells)} cells, expected {len(headers)}")
                        continue
                    
                    # Build permit dictionary
                    permit = {}
                    for i, cell in enumerate(cells):
                        cell_text = cell.get_text(strip=True)
                        if i in header_mapping:
                            field_name = header_mapping[i]
                            permit[field_name] = cell_text if cell_text else None
                    
                    # Only add if we have some data
                    if permit:
                        permits.append(permit)
                        
                except Exception as e:
                    logger.warning(f"Error parsing row {row_idx}: {e}")
                    continue
            
            logger.info(f"Successfully parsed {len(permits)} permits")
            return permits
            
        except Exception as e:
            logger.error(f"Error parsing results table: {e}")
            return []
    
    def _create_fallback_response(self, begin: str, end: str, items: List[Dict[str, Any]], warning: str) -> Dict[str, Any]:
        """Create a standardized fallback response."""
        return {
            "source_root": self.base_url,
            "query_params": {
                "begin": begin,
                "end": end
            },
            "pages": 1,
            "count": len(items),
            "items": items,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "warning": warning
        }