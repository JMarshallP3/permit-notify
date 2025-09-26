# services/enrichment/detail_parser.py
from __future__ import annotations
from decimal import Decimal
from urllib.parse import urljoin
from lxml import html
import re

WS = re.compile(r"\s+")

def norm(s: str | None) -> str:
    return WS.sub(" ", (s or "").strip()).lower()

def _xpath_first(tree, xp):
    res = tree.xpath(xp)
    return res[0] if res else None

def _text(el) -> str | None:
    if el is None: return None
    t = el.text_content().strip()
    return WS.sub(" ", t) if t else None

def _is_valid_field_name(text: str) -> bool:
    """
    Validate if text looks like a legitimate field name.
    Field names should be geological formations, not status messages or timestamps.
    """
    if not text or len(text) > 100:  # Too long
        return False
    
    text_lower = text.lower().strip()
    
    # Reject obvious non-field-name patterns
    invalid_patterns = [
        # Timestamps and dates
        r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
        r'\d{1,2}:\d{2}:\d{2}',  # HH:MM:SS
        r'\b(am|pm)\b',  # AM/PM (word boundaries to avoid matching "camp", "phantom", etc.)
        
        # Status messages and well operations
        r'please pay',
        r'exception fee',
        r'additional problems',
        r're-entry permit',
        r'this well was',
        r'never plu',
        r'revised plat',
        r'changed.*survey',
        r'allocation wells',
        r'drilled concurre',
        r'commission staff',
        r'expresses no opinion',
        r'staff expresses',
        r'no opinion',
        r'recompletion.*of.*well',
        r'completion.*of.*well',
        r'into.*shallower',
        r'into.*deeper',
        r'interval',
        
        # Administrative text
        r'suggested:',
        r'permit.*added',
        r'review now',
        r'dismiss',
        
        # Generic page headers and navigation text
        r'general.*location.*information',
        r'location.*information',
        r'general.*information',
        r'page.*header',
        r'navigation',
        r'menu',
        r'header',
        r'footer',
        r'exactly.*as.*shown.*in.*rrc.*records',
        
        # Distance and measurement descriptions (not field names)
        r'nearest.*distance.*from.*the.*first.*last.*take.*point',
        r'distance.*from.*nearest',
        r'perpendicular.*distance',
        r'basic.*information',
        
        # Company/operator patterns (not field names)
        r'.*\s+co\s*[/,]',  # "CO/" or "CO,"
        r'.*\s+llc\s*$',    # ends with "LLC"
        r'.*\s+inc\s*$',    # ends with "INC"
        r'.*\s+corp\s*$',   # ends with "CORP"
        r'h&tc\s+rr\s+co',  # H&TC RR CO
        r'railroad\s+co',   # Railroad Company
        
        # Field name patterns that are too generic
        r'^fields?\s+\d+$',  # "FIELD 21", "FIELDS 21"
        r'^field\s+\w+$',    # "FIELD ABC"
        
        # Common non-field patterns
        r'^\d+\s*(of|wells?|allocation)',
        r'primary field$',
        r'^oil\s+(or\s+)?gas',
        r'^gas\s+(or\s+)?oil',
    ]
    
    # Check for invalid patterns
    for pattern in invalid_patterns:
        if re.search(pattern, text_lower):
            return False
    
    # Valid field names typically have these characteristics:
    # 1. Contain formation names in parentheses, OR
    # 2. Are all caps geological names, OR  
    # 3. Contain common geological terms
    
    geological_terms = [
        'eagle ford', 'wolfcamp', 'spraberry', 'austin chalk', 'barnett shale',
        'bone spring', 'delaware', 'midland', 'permian', 'woodford',
        'haynesville', 'marcellus', 'utica', 'bakken', 'niobrara',
        'trend area', 'formation', 'shale', 'chalk', 'sand', 'lime',
        'granite wash', 'atoka', 'canyon', 'strawn', 'bend',
        'phantom', 'sugarkane', 'hawkville', 'emma', 'green bullet',
        'silvertip', 'skaggs', 'ratliff', 'johnson', 'bivins', 'bush',
        'courson', 'herndon', 'moy', 'reynolds', 'dorcus', 'cindy',
        # Additional common Texas formations
        'wolfcamp', 'phantom', 'bone spring', 'delaware basin',
        'peart', 'barnett', 'frio', 'jackson', 'yegua', 'wilcox',
        'cotton valley', 'travis peak', 'hosston', 'sligo', 'james lime'
    ]
    
    # Check for geological terms
    has_geo_term = any(term in text_lower for term in geological_terms)
    
    # Check for parentheses pattern (common in field names)
    has_formation_pattern = '(' in text and ')' in text and len(text.split('(')[0].strip()) > 2
    
    # Check if it looks like a proper field name (geological formation pattern)
    # Be much more strict - require either:
    # 1. Parentheses with formation name, OR
    # 2. Specific geological terms, OR
    # 3. Known field naming patterns
    
    # Require parentheses pattern for most field names
    has_proper_formation_pattern = (
        '(' in text and ')' in text and 
        len(text.split('(')[0].strip()) > 2 and  # Something before parentheses
        len(text.split('(')[1].split(')')[0].strip()) > 2  # Something inside parentheses
    )
    
    # Allow specific geological formations without parentheses
    standalone_formations = [
        'spraberry', 'wolfcamp', 'eagle ford', 'austin chalk', 'barnett',
        'bone spring', 'delaware', 'permian', 'woodford', 'haynesville',
        'marcellus', 'utica', 'bakken', 'niobrara'
    ]
    
    is_standalone_formation = any(formation in text_lower for formation in standalone_formations)
    
    # Only accept if it has proper formation pattern OR is a known standalone formation
    return has_proper_formation_pattern or is_standalone_formation

def _clean_field_name(text: str) -> str:
    """
    Clean and standardize field name text.
    """
    if not text:
        return None
    
    # Split by newlines and take the first meaningful line
    lines = text.strip().split('\n')
    clean_name = lines[0].strip()
    
    # Remove common suffixes that aren't part of the field name
    suffixes_to_remove = [
        r'\s+Primary Field$',
        r'\s+Secondary Field$', 
        r'\s+\([^)]*Field\)$',
        r'\s+Field$',
    ]
    
    for suffix in suffixes_to_remove:
        clean_name = re.sub(suffix, '', clean_name, flags=re.IGNORECASE)
    
    # Clean up extra whitespace
    clean_name = re.sub(r'\s+', ' ', clean_name).strip()
    
    # Ensure we still have parentheses for formation names
    if '(' in clean_name and ')' in clean_name:
        return clean_name
    elif clean_name and len(clean_name) >= 5:
        return clean_name
    else:
        return None

def _get_next_value(cell_texts: list, start_index: int, max_distance: int = 5) -> str:
    """Get the next non-empty value after a given index."""
    for i in range(start_index + 1, min(start_index + max_distance + 1, len(cell_texts))):
        text = cell_texts[i]
        if text and text.strip() and len(text.strip()) > 0:
            return text.strip()
    return None

def _is_valid_location_value(text: str, field_type: str) -> bool:
    """Validate if text looks like a valid location field value."""
    if not text or len(text) > 50:
        return False
    
    text_clean = text.strip()
    
    if field_type == "section":
        # Section should be a number or simple alphanumeric
        return bool(re.match(r'^[A-Z0-9\-]{1,10}$', text_clean))
    
    elif field_type == "block":
        # Block should be a number or simple alphanumeric  
        return bool(re.match(r'^[A-Z0-9\-]{1,10}$', text_clean))
    
    elif field_type == "survey":
        # Survey should be a name, not "Abstr" or other junk
        if text_clean.lower() in ['abstr', 'abstract', 'county', 'name', 'lines']:
            return False
        # Should contain letters and be reasonable length
        return bool(re.match(r'^[A-Z\s,\.\-&]{2,30}$', text_clean)) and len(text_clean) >= 2
    
    elif field_type == "abstract":
        # Abstract should be alphanumeric, often starting with A-
        return bool(re.match(r'^[A-Z]?-?[0-9]{1,6}$', text_clean)) or bool(re.match(r'^[A-Z]{1,3}-[0-9]{1,6}$', text_clean))
    
    return True

def _label_value(tree, label_texts: list[str]) -> str | None:
    """
    Find a <tr> that has a cell (th/td) whose normalized text contains one of label_texts,
    then return the text of the first following <td> in that row.
    """
    for label in label_texts:
        # Any TR that contains a TH/TD with text containing the label (case-insensitive)
        tr = _xpath_first(
            tree,
            f"//tr[.//td[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{label.lower()}')] or .//th[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{label.lower()}')]]"
        )
        if tr is not None:
            # Get all cells
            tds = tr.xpath(".//td")
            ths = tr.xpath(".//th")
            cells = ths + tds  # header cells can precede data
            
            # Find index of the label cell among row cells (normalized compare)
            idx = None
            for i, c in enumerate(cells):
                cell_text = norm(c.text_content())
                if label.lower() in cell_text:
                    idx = i
                    break
            
            # Value cell = first TD after the label cell
            if idx is not None:
                # Search forward for the next TD
                for c in cells[idx+1:]:
                    if c.tag.lower() == "td":
                        return _text(c)
            
            # Fallback: last TD in the row
            if tds:
                return _text(tds[-1])
    return None

def _header_map(table) -> dict[str, int]:
    """Map normalized header text -> column index."""
    headers = table.xpath(".//tr[1]/*[self::th or self::td]")
    m = {}
    for i, h in enumerate(headers):
        key = norm(h.text_content())
        m[key] = i
    return m

def _first_data_row_cells(table):
    rows = table.xpath(".//tr[position()>1]")
    if not rows: return []
    return rows[0].xpath("./*")

def parse_detail_page(html_text: str, detail_url: str) -> dict:
    """
    Returns:
      {
        "horizontal_wellbore": str|None,
        "field_name": str|None,
        "acres": Decimal|None,
        "section": str|None,
        "block": str|None,
        "survey": str|None,
        "abstract_no": str|None,
        "view_w1_pdf_url": str|None
      }
    """
    tree = html.fromstring(html_text)

    # This is one massive table - find the main table and extract from it
    tables = tree.xpath("//table")
    main_table = None
    
    # Find the main table that contains all the data
    for table in tables:
        table_text = table.text_content()
        if "horizontal wellbore" in table_text.lower() and "field" in table_text.lower() and "acres" in table_text.lower():
            main_table = table
            break
    
    if main_table is None:
        return {
            "horizontal_wellbore": None,
            "field_name": None,
            "acres": None,
            "section": None,
            "block": None,
            "survey": None,
            "abstract_no": None,
            "view_w1_pdf_url": None,
        }
    
    # Get all cells from the main table
    all_cells = main_table.xpath(".//*[self::th or self::td]")
    cell_texts = [cell.text_content().strip() for cell in all_cells]
    
    # Find indices of key fields
    horizontal_wellbore = None
    field_name = None
    acres = None
    section = None
    block = None
    survey = None
    abstract_no = None
    
    # Look for "Horizontal Wellbore" and get the next value
    for i, text in enumerate(cell_texts):
        if "horizontal wellbore" in text.lower() and len(text) < 50:  # Avoid long text
            # Look for "Allocation" specifically, or skip headers
            for j in range(i+1, min(i+20, len(cell_texts))):  # Look ahead up to 20 cells
                next_text = cell_texts[j]
                if (next_text and len(next_text) < 50 and 
                    next_text.lower() == "allocation"):
                    horizontal_wellbore = next_text
                    break
            if horizontal_wellbore:
                break
    
    # Look for field name in the Fields table section - TARGETED PARSING
    # First, try to find the Fields table specifically
    fields_table_found = False
    fields_table_start = None
    
    # Look for Fields table by finding "District" and "Field Name" headers nearby
    for i, text in enumerate(cell_texts):
        if text.lower().strip() == "district":
            # Look for "Field Name" in the next few cells
            for j in range(i+1, min(i+10, len(cell_texts))):
                if cell_texts[j].lower().strip() == "field name":
                    fields_table_found = True
                    fields_table_start = i
                    break
            if fields_table_found:
                break
    
    # If we found the Fields table, look for field names in the data rows
    if fields_table_found and fields_table_start is not None:
        # Look for field names in the next several cells after the header
        for j in range(fields_table_start + 10, min(fields_table_start + 50, len(cell_texts))):
            next_text = cell_texts[j]
            if next_text and _is_valid_field_name(next_text):
                cleaned = _clean_field_name(next_text)
                if cleaned:
                    field_name = cleaned
                    break
    
    # Fallback 1: Look for "Field Name" header and get the next value
    if not field_name:
        for i, text in enumerate(cell_texts):
            if "field" in text.lower() and "name" in text.lower() and len(text) < 20:
                # Look for field names that typically contain parentheses (formation names)
                for j in range(i+1, min(i+20, len(cell_texts))):
                    next_text = cell_texts[j]
                    if next_text and _is_valid_field_name(next_text):
                        field_name = _clean_field_name(next_text)
                        break
                if field_name:
                    break
    
    # Fallback 2: Look for common field name patterns anywhere in the table
    if not field_name:
        for text in cell_texts:
            if text and _is_valid_field_name(text):
                cleaned = _clean_field_name(text)
                if cleaned:
                    field_name = cleaned
                    break
    
    # Look for "Acres" in the Fields table - TARGETED PARSING
    # First, try to find acres in the Fields table context
    for i, text in enumerate(cell_texts):
        # Look for "Acres" column header in Fields table
        if text.lower() == "acres" and len(text) < 10:
            # Look for the very next non-empty cell that's a decimal number
            for j in range(i+1, min(i+10, len(cell_texts))):  # Look at next few cells
                next_text = cell_texts[j]
                if (next_text and next_text.strip() and len(next_text) < 20):
                    # Try to parse as decimal - more flexible approach
                    cleaned_text = next_text.replace(",", "").strip()
                    try:
                        acres_val = Decimal(cleaned_text)
                        # Accept reasonable acre values (between 0.1 and 100000)
                        if 0.1 <= acres_val <= 100000:
                            acres = acres_val
                            break
                    except Exception:
                        continue
            if acres:
                break
    
    # Fallback: Look for decimal numbers that could be acres in Fields table context
    if not acres and fields_table_found:
        for text in cell_texts:
            if text and text.strip() and len(text) < 20:
                cleaned_text = text.replace(",", "").strip()
                try:
                    acres_val = Decimal(cleaned_text)
                    # More specific range for acres in Fields table (typically 1-10000)
                    if 1.0 <= acres_val <= 10000 and '.' in text:  # Must have decimal point
                        acres = acres_val
                        break
                except Exception:
                    continue
    
    # FLEXIBLE: Look for section/block/survey/abstract data with multiple patterns
    pattern_found = False
    
    # Try multiple approaches to find the location data
    location_patterns = [
        # Pattern 1: "Section:", value, "Block:", value, "Survey:NAME", "Abstract #:", value
        {"section_key": "Section:", "block_key": "Block:", "survey_key": "Survey:", "abstract_key": "Abstract #:"},
        # Pattern 2: Just the labels without colons
        {"section_key": "Section", "block_key": "Block", "survey_key": "Survey", "abstract_key": "Abstract"},
        # Pattern 3: Different variations
        {"section_key": "Sec", "block_key": "Blk", "survey_key": "Survey", "abstract_key": "Abs"},
    ]
    
    for pattern in location_patterns:
        section_idx = None
        block_idx = None
        survey_idx = None
        abstract_idx = None
        
        # Find indices of each key
        for i, text in enumerate(cell_texts):
            text_clean = text.strip().replace(":", "")
            
            if pattern["section_key"].replace(":", "") in text_clean and section_idx is None:
                section_idx = i
            elif pattern["block_key"].replace(":", "") in text_clean and block_idx is None:
                block_idx = i
            elif pattern["survey_key"].replace(":", "") in text_clean and survey_idx is None:
                survey_idx = i
            elif pattern["abstract_key"].replace(":", "") in text_clean and abstract_idx is None:
                abstract_idx = i
        
        # If we found at least section and block, try to extract values
        if section_idx is not None and block_idx is not None:
            # Extract section value (next non-empty cell after section label)
            section_val = _get_next_value(cell_texts, section_idx)
            if section_val and _is_valid_location_value(section_val, "section"):
                section = section_val
            
            # Extract block value
            block_val = _get_next_value(cell_texts, block_idx)
            if block_val and _is_valid_location_value(block_val, "block"):
                block = block_val
            
            # Extract survey value
            if survey_idx is not None:
                survey_val = _get_next_value(cell_texts, survey_idx)
                if survey_val and _is_valid_location_value(survey_val, "survey"):
                    survey = survey_val
                elif "Survey:" in cell_texts[survey_idx]:
                    # Handle "Survey:MUSQUIZ, R" format
                    survey_text = cell_texts[survey_idx]
                    survey_val = survey_text.replace("Survey:", "").strip()
                    if survey_val and _is_valid_location_value(survey_val, "survey"):
                        survey = survey_val
            
            # Extract abstract value
            if abstract_idx is not None:
                abstract_val = _get_next_value(cell_texts, abstract_idx)
                if abstract_val and _is_valid_location_value(abstract_val, "abstract"):
                    abstract_no = abstract_val
            
            if section or block or survey or abstract_no:
                pattern_found = True
                break
    
    # If the pattern-based approach didn't work, fall back to the original logic for section/block
    if not pattern_found and section is None:
        for i, text in enumerate(cell_texts):
            if text.lower() == "section" and len(text) < 10:
                # Look for the next non-empty cell
                for j in range(i+1, min(i+20, len(cell_texts))):
                    next_text = cell_texts[j]
                    if (next_text and len(next_text) < 20 and 
                        "section" not in next_text.lower() and
                        "block" not in next_text.lower() and
                        "survey" not in next_text.lower() and
                        "abstract" not in next_text.lower() and
                        "county" not in next_text.lower()):
                        section = next_text
                        break
                if section:
                    break
    
    if not pattern_found and block is None:
        for i, text in enumerate(cell_texts):
            if text.lower() == "block" and len(text) < 10:
                # Look for the next non-empty cell
                for j in range(i+1, min(i+20, len(cell_texts))):
                    next_text = cell_texts[j]
                    if (next_text and len(next_text) < 20 and 
                        "block" not in next_text.lower() and
                        "survey" not in next_text.lower() and
                        "abstract" not in next_text.lower() and
                        "county" not in next_text.lower()):
                        block = next_text
                        break
                if block:
                    break
    
    # Fallback logic for survey and abstract_no if pattern didn't work
    if not pattern_found and survey is None:
        for i, text in enumerate(cell_texts):
            if text.lower() == "survey" and len(text) < 10:
                # Look for the next non-empty cell
                for j in range(i+1, min(i+20, len(cell_texts))):
                    next_text = cell_texts[j]
                    if (next_text and len(next_text) < 50 and 
                        "survey" not in next_text.lower() and
                        "abstract" not in next_text.lower() and
                        "county" not in next_text.lower() and
                        "township" not in next_text.lower() and
                        "league" not in next_text.lower() and
                        "labor" not in next_text.lower() and
                        "porcion" not in next_text.lower() and
                        "share" not in next_text.lower() and
                        "tract" not in next_text.lower() and
                        "lot" not in next_text.lower()):
                        survey = next_text
                        break
                if survey:
                    break
    
    if not pattern_found and abstract_no is None:
        for i, text in enumerate(cell_texts):
            if "abstract #" in text.lower() and len(text) < 20:
                # Look for the next non-empty cell that's a number
                for j in range(i+1, min(i+20, len(cell_texts))):
                    next_text = cell_texts[j]
                    if next_text and next_text.isdigit():
                        abstract_no = next_text
                        break
                if abstract_no:
                    break

    # D) "View Current W-1" PDF link
    href = None
    a = _xpath_first(tree,
        "//a[contains(., 'View Current W-1') or "
        "contains(@href, 'viewW1PdfFormAction.do') or "
        "contains(@href, 'viewW1FormAction.do') or "
        "contains(@href, 'viewW1Pdf') or "
        "contains(@href, 'downloadDocumentAction.do')]"
    )
    if a is not None:
        h = a.get("href")
        if h:
            href = urljoin(detail_url, h)

    return {
        "horizontal_wellbore": horizontal_wellbore,
        "field_name": field_name,
        "acres": acres,
        "section": section,
        "block": block,
        "survey": survey,
        "abstract_no": abstract_no,
        "view_w1_pdf_url": href,
    }