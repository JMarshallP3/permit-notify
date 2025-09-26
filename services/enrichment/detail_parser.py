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
        r'(am|pm)',  # AM/PM
        
        # Status messages
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
        
        # Administrative text
        r'suggested:',
        r'permit.*added',
        r'review now',
        r'dismiss',
        
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
        'courson', 'herndon', 'moy', 'reynolds', 'dorcus', 'cindy'
    ]
    
    # Check for geological terms
    has_geo_term = any(term in text_lower for term in geological_terms)
    
    # Check for parentheses pattern (common in field names)
    has_formation_pattern = '(' in text and ')' in text and len(text.split('(')[0].strip()) > 2
    
    # Check if it looks like a proper field name (mostly uppercase, reasonable length)
    looks_like_field_name = (
        text.isupper() and 
        5 <= len(text) <= 50 and
        not text.isdigit() and
        ' ' in text  # Multi-word
    )
    
    return has_geo_term or has_formation_pattern or looks_like_field_name

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
    
    # Look for "Field Name" and get the next value - ROBUST PARSING
    for i, text in enumerate(cell_texts):
        if "field" in text.lower() and "name" in text.lower():
            # Look for field names that typically contain parentheses (formation names)
            for j in range(i+1, min(i+20, len(cell_texts))):
                next_text = cell_texts[j]
                if next_text and _is_valid_field_name(next_text):
                    field_name = _clean_field_name(next_text)
                    break
            if field_name:
                break
    
    # Fallback: Look for common field name patterns anywhere in the table
    if not field_name:
        for text in cell_texts:
            if text and _is_valid_field_name(text):
                cleaned = _clean_field_name(text)
                if cleaned:
                    field_name = cleaned
                    break
    
    # Look for "Acres" and get the next value - IMPROVED GENERIC PARSING
    for i, text in enumerate(cell_texts):
        if text.lower() == "acres" and len(text) < 10:
            # Look for the very next non-empty cell that's a decimal number
            for j in range(i+1, min(i+5, len(cell_texts))):  # Look at next few cells only
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
    
    # IMPROVED: Look for the specific pattern: "Section:", "", "Block:", "", "Survey:MUSQUIZ, R", "Abstract #:", "7"
    # This pattern appears reliably in the HTML structure
    pattern_found = False
    for i, text in enumerate(cell_texts):
        if text == "Section:" and i + 8 < len(cell_texts):
            # Check if we have the expected pattern
            pattern_cells = cell_texts[i:i+9]  # Get next 9 cells
            
            if (len(pattern_cells) >= 9 and 
                pattern_cells[0] == "Section:" and
                pattern_cells[2] == "Block:" and
                "Survey:" in pattern_cells[4] and
                pattern_cells[5] == "Abstract #:"):
                
                # Extract the values
                section_val = pattern_cells[1] if pattern_cells[1] else None
                block_val = pattern_cells[3] if pattern_cells[3] else None
                
                # Extract survey from "Survey:MUSQUIZ, R" format
                survey_text = pattern_cells[4]
                if "Survey:" in survey_text:
                    survey_val = survey_text.replace("Survey:", "").strip()
                    survey_val = survey_val if survey_val else None
                else:
                    survey_val = None
                
                abstract_val = pattern_cells[6] if pattern_cells[6] else None
                
                # Clean empty values (convert empty strings to None)
                section = section_val if section_val and section_val.strip() and section_val.strip() != '' else None
                block = block_val if block_val and block_val.strip() and block_val.strip() != '' else None
                survey = survey_val if survey_val and survey_val.strip() and survey_val.strip() != '' else None
                abstract_no = abstract_val if abstract_val and abstract_val.strip() and abstract_val.strip() != '' else None
                
                # Found the pattern, no need to continue
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