"""
RRC W-1 Results Parser

This module provides robust parsing of RRC W-1 search results with direct access
to the Well # column for accurate well number extraction.
"""

import re
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)

# Regex to match well column headers (case-insensitive)
WELL_HEADER_RE = re.compile(r'Well\s*#', re.I)

def parse_results_well_numbers(html: str) -> List[Dict[str, str]]:
    """
    Parse RRC W-1 search results and extract well numbers directly from the Well # column.
    
    Args:
        html: HTML content of the RRC W-1 search results page
        
    Returns:
        List of dictionaries with permit data including well numbers
    """
    soup = BeautifulSoup(html, "html.parser")

    # Find the main results table - look for the one with proper headers
    tables = soup.find_all("table")
    logger.info(f"Found {len(tables)} tables to check")
    
    target = None
    for i, t in enumerate(tables):
        rows = t.find_all("tr")
        if not rows:
            continue
            
        # Check if this table has the expected headers
        header_row = rows[0]
        headers = [th.get_text(" ", strip=True) for th in header_row.find_all(["th", "td"])]
        
        # Look for the specific header pattern we expect
        has_operator = any("Operator" in h for h in headers)
        has_well = any(WELL_HEADER_RE.search(h) for h in headers)
        has_status = any("Status" in h for h in headers)
        has_lease = any("Lease" in h for h in headers)
        
        logger.info(f"Table {i+1}: {len(headers)} columns, has_operator={has_operator}, has_well={has_well}, has_status={has_status}, has_lease={has_lease}")
        logger.info(f"  Headers: {headers[:5]}...")  # Show first 5 headers
        
        # This should be the results table if it has these key headers
        # Prefer tables with reasonable column counts (not the massive ones with all data in one cell)
        if has_operator and has_well and has_status and has_lease and 10 <= len(headers) <= 20:
            target = t
            logger.info(f"Found results table with {len(headers)} columns: {headers}")
            break
    
    if target is None:
        logger.warning("Could not find RRC W-1 results table")
        return []

    # Build header -> index map
    header_cells = target.find("tr").find_all(["th", "td"])
    headers = [hc.get_text(" ", strip=True) for hc in header_cells]
    idx = {h: i for i, h in enumerate(headers)}
    
    logger.info(f"Found table headers: {headers}")

    # Find the index of the Well # column
    well_idx = None
    for h, i in idx.items():
        if WELL_HEADER_RE.search(h):
            well_idx = i
            break
    
    if well_idx is None:
        logger.warning("Could not find Well # column")
        return []

    # Find indices of other useful columns
    lease_idx = next((i for h, i in idx.items() if "Lease Name" in h), None)
    operator_idx = next((i for h, i in idx.items() if "Operator" in h), None)
    api_idx = next((i for h, i in idx.items() if "API" in h), None)
    status_idx = next((i for h, i in idx.items() if "Status" in h and "#" in h), None)
    date_idx = next((i for h, i in idx.items() if "Date" in h), None)
    district_idx = next((i for h, i in idx.items() if "Dist" in h), None)
    county_idx = next((i for h, i in idx.items() if "County" in h), None)
    profile_idx = next((i for h, i in idx.items() if "Wellbore Profile" in h), None)
    purpose_idx = next((i for h, i in idx.items() if "Filing Purpose" in h), None)
    amend_idx = next((i for h, i in idx.items() if "Amend" in h), None)
    depth_idx = next((i for h, i in idx.items() if "Total Depth" in h), None)
    queue_idx = next((i for h, i in idx.items() if "Current Queue" in h), None)

    out = []
    rows = target.find_all("tr")[1:]  # skip header
    logger.info(f"Processing {len(rows)} data rows")
    
    for row_num, row in enumerate(rows):
        cells = row.find_all("td")
        if not cells or well_idx >= len(cells):
            continue

        # Extract well number directly from Well # column
        raw_well = cells[well_idx].get_text(" ", strip=True)
        
        # Use the well number extractor as fallback for messy values
        from well_number_extractor import extract_well_no_from_text
        well_number = extract_well_no_from_text(raw_well) or raw_well
        
        # Extract other fields
        lease_name = cells[lease_idx].get_text(" ", strip=True) if lease_idx is not None else ""
        operator_name = cells[operator_idx].get_text(" ", strip=True) if operator_idx is not None else ""
        api_number = cells[api_idx].get_text(" ", strip=True) if api_idx is not None else ""
        status_no = cells[status_idx].get_text(" ", strip=True) if status_idx is not None else ""
        status_date = cells[date_idx].get_text(" ", strip=True) if date_idx is not None else ""
        district = cells[district_idx].get_text(" ", strip=True) if district_idx is not None else ""
        county = cells[county_idx].get_text(" ", strip=True) if county_idx is not None else ""
        wellbore_profile = cells[profile_idx].get_text(" ", strip=True) if profile_idx is not None else ""
        filing_purpose = cells[purpose_idx].get_text(" ", strip=True) if purpose_idx is not None else ""
        amend = cells[amend_idx].get_text(" ", strip=True) if amend_idx is not None else ""
        total_depth = cells[depth_idx].get_text(" ", strip=True) if depth_idx is not None else ""
        current_queue = cells[queue_idx].get_text(" ", strip=True) if queue_idx is not None else ""

        # Get detail link and normalize to absolute URL
        lease_link = None
        if lease_idx is not None and cells[lease_idx].find("a"):
            href = cells[lease_idx].find("a").get("href")
            lease_link = normalize_rrc_link(href)

        # Convert amend field to boolean
        amend_bool = None
        if amend:
            amend_lower = amend.lower().strip()
            if amend_lower == 'yes':
                amend_bool = True
            elif amend_lower == 'no':
                amend_bool = False
            # else: leave as None for '-' or other values
        
        # Parse status_date to extract just the date part
        parsed_status_date = None
        if status_date:
            import re
            # Extract date from "Submitted 09/24/2025" format
            date_match = re.search(r'(\d{2}/\d{2}/\d{4})', status_date.strip())
            if date_match:
                parsed_status_date = date_match.group(1)
            else:
                parsed_status_date = status_date.strip() if status_date.strip() else None
        
        # Only include rows with meaningful data
        if status_no or api_number or operator_name:
            out.append({
                "status_date": parsed_status_date,
                "status_no": status_no,
                "api_no": api_number,
                "operator_name": operator_name,
                "lease_name": lease_name,
                "well_no": well_number if well_number else None,
                "district": district,
                "county": county,
                "wellbore_profile": wellbore_profile,
                "filing_purpose": filing_purpose,
                "amend": amend_bool,
                "total_depth": total_depth,
                "current_queue": current_queue,
                "detail_url": lease_link,
            })
            
            if well_number:
                logger.debug(f"Row {row_num + 1}: Found well_no '{well_number}' in Well # column")

    logger.info(f"Successfully parsed {len(out)} permit records")
    return out

def normalize_rrc_link(href: Optional[str], base_url: str = "https://webapps.rrc.state.tx.us") -> Optional[str]:
    """
    Normalize RRC detail link to absolute URL.
    
    Args:
        href: Relative or absolute URL
        base_url: Base URL for RRC site
        
    Returns:
        Absolute URL or None
    """
    if not href:
        return None
    
    if href.startswith('http'):
        return href
    
    return f"{base_url.rstrip('/')}/{href.lstrip('/')}"
