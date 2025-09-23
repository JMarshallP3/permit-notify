"""
PDF parsing utilities for extracting reservoir well count information.
"""

import re
from typing import Tuple, Optional
from pdfminer.high_level import extract_text
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from io import StringIO, BytesIO
import logging

logger = logging.getLogger(__name__)

def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """
    Extract text from PDF bytes using pdfminer.six.
    
    Args:
        pdf_bytes: Raw PDF file bytes
        
    Returns:
        Extracted text as string
    """
    try:
        # Use high-level API for simplicity
        text = extract_text(BytesIO(pdf_bytes))
        return text
    except Exception as e:
        logger.warning(f"High-level PDF extraction failed: {e}, trying low-level approach")
        
        # Fallback to low-level API
        try:
            resource_manager = PDFResourceManager()
            output_string = StringIO()
            laparams = LAParams()
            device = TextConverter(resource_manager, output_string, laparams=laparams)
            
            # Create a file-like object from bytes
            pdf_file = BytesIO(pdf_bytes)
            
            interpreter = PDFPageInterpreter(resource_manager, device)
            
            for page in PDFPage.get_pages(pdf_file, check_extractable=True):
                interpreter.process_page(page)
            
            text = output_string.getvalue()
            device.close()
            output_string.close()
            
            return text
        except Exception as e2:
            logger.error(f"PDF text extraction failed: {e2}")
            return ""

def parse_reservoir_well_count(text: str) -> Tuple[Optional[int], float, str]:
    """
    Parse reservoir well count from PDF text.
    
    Args:
        text: Extracted PDF text
        
    Returns:
        Tuple of (well_count, confidence, snippet)
    """
    if not text or not text.strip():
        return None, 0.0, ""
    
    try:
        # Clean and normalize text
        text_clean = re.sub(r'\s+', ' ', text.strip())
        
        # Use specified patterns with flags
        flags = re.IGNORECASE | re.DOTALL
        patterns = [
            r'Number\s+of\s+Wells\s+on\s+this\s+lease\s+in\s+this\s+Reservoir[^0-9]{0,40}(\d{1,5})',
            r'\b32\.\s*Number\s+of\s+Wells.*?(\d{1,5})',
            r'\bNumber\s+of\s+Wells\b[^0-9]{0,40}(\d{1,5})'
        ]
        
        # First match wins
        for pattern in patterns:
            match = re.search(pattern, text_clean, flags)
            
            if match:
                try:
                    well_count = int(match.group(1))
                    confidence = 0.85
                    
                    # Create context snippet around the match
                    start = max(0, match.start() - 200)
                    end = min(len(text_clean), match.end() + 200)
                    context_snippet = text_clean[start:end]
                    
                    logger.debug(f"Found reservoir well count: {well_count} (confidence: {confidence})")
                    return well_count, confidence, context_snippet
                    
                except ValueError:
                    logger.warning(f"Could not parse well count as integer: {match.group(1)}")
                    continue
        
        # If none: return (None, 0.0, text[:600])
        snippet = text_clean[:600] if len(text_clean) > 600 else text_clean
        logger.debug("No reservoir well count pattern found")
        return None, 0.0, snippet
        
    except Exception as e:
        logger.error(f"Error parsing reservoir well count: {e}")
        return None, 0.0, ""

def parse_w1_content(text: str) -> Tuple[Optional[str], Optional[int], float, str]:
    """
    Parse W-1 content to extract field name and well count.
    
    Args:
        text: Extracted PDF text
        
    Returns:
        Tuple of (field_name, well_count, confidence, text_snippet)
    """
    if not text or not text.strip():
        return None, None, 0.0, ""
    
    # Clean and normalize text
    text_clean = re.sub(r'\s+', ' ', text.strip())
    text_snippet = text_clean[:800] if len(text_clean) > 800 else text_clean
    
    confidence = 0.0
    field_name = None
    well_count = None
    
    # Extract field name using regex patterns (in order of preference)
    field_patterns = [
        r'\bFIELD\s*NAME\s*[:\-]\s*(.+?)(?:\s{2,}|\s+OPERATOR|\s+LEASE|\s+WELL|\s+TOTAL|\s+NUMBER|\s+API|\s+DIST|\s+COUNTY|\s+AMEND|\s+STACKED|\s+CURRENT|\n|$)',  # Pattern 1: FIELD NAME: value
        r'\bFIELD\s*[:\-]\s*(.+?)(?:\s{2,}|\s+OPERATOR|\s+LEASE|\s+WELL|\s+TOTAL|\s+NUMBER|\s+API|\s+DIST|\s+COUNTY|\s+AMEND|\s+STACKED|\s+CURRENT|\n|$)',         # Pattern 2: FIELD: value
    ]
    
    for pattern in field_patterns:
        match = re.search(pattern, text_clean, re.IGNORECASE)
        if match:
            field_name = match.group(1).strip()
            # Clean up common artifacts
            field_name = re.sub(r'\s+', ' ', field_name)
            field_name = field_name.strip('.,;:')
            if field_name:
                confidence += 0.1
                break
    
    # Extract well count using regex patterns (in order of preference)
    well_count_patterns = [
        r'\bTOTAL\s+NUMBER\s+OF\s+WELLS\s*[:\-]?\s*(\d+)\b',
        r'\bNUMBER\s+OF\s+WELLS\s*[:\-]?\s*(\d+)\b',
    ]
    
    explicit_count_found = False
    for pattern in well_count_patterns:
        match = re.search(pattern, text_clean, re.IGNORECASE)
        if match:
            try:
                well_count = int(match.group(1))
                confidence += 0.6
                explicit_count_found = True
                break
            except ValueError:
                continue
    
    # Fallback: count distinct well ID tokens
    if not explicit_count_found:
        well_id_pattern = r'\bWELL\s*NO\.?\s*[A-Za-z0-9\-]+'
        well_matches = re.findall(well_id_pattern, text_clean, re.IGNORECASE)
        
        # Extract and deduplicate well numbers
        well_numbers = set()
        for match in well_matches:
            # Extract the actual well number part
            well_num_match = re.search(r'[A-Za-z0-9\-]+$', match)
            if well_num_match:
                well_numbers.add(well_num_match.group(0))
        
        if well_numbers:
            well_count = len(well_numbers)
            confidence += 0.3
    
    # Clip confidence to [0, 1]
    confidence = max(0.0, min(1.0, confidence))
    
    return field_name, well_count, confidence, text_snippet

def calculate_pdf_sha256(pdf_bytes: bytes) -> str:
    """
    Calculate SHA256 hash of PDF bytes.
    
    Args:
        pdf_bytes: Raw PDF file bytes
        
    Returns:
        SHA256 hash as hex string
    """
    import hashlib
    return hashlib.sha256(pdf_bytes).hexdigest()

def parse_w1_pdf(pdf_bytes: bytes) -> Tuple[Optional[str], Optional[int], float, str, str]:
    """
    Complete W-1 PDF parsing pipeline.
    
    Args:
        pdf_bytes: Raw PDF file bytes
        
    Returns:
        Tuple of (field_name, well_count, confidence, text_snippet, pdf_sha256)
    """
    try:
        # Extract text from PDF
        text = extract_text_from_pdf(pdf_bytes)
        
        if not text:
            logger.warning("No text extracted from PDF")
            return None, None, 0.0, "", calculate_pdf_sha256(pdf_bytes)
        
        # Parse W-1 content
        field_name, well_count, confidence, text_snippet = parse_w1_content(text)
        
        # Calculate PDF hash
        pdf_sha256 = calculate_pdf_sha256(pdf_bytes)
        
        logger.info(f"Parsed W-1 PDF: field='{field_name}', wells={well_count}, confidence={confidence:.2f}")
        
        return field_name, well_count, confidence, text_snippet, pdf_sha256
        
    except Exception as e:
        logger.error(f"Error parsing W-1 PDF: {e}")
        return None, None, 0.0, "", calculate_pdf_sha256(pdf_bytes)