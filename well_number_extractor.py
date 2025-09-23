#!/usr/bin/env python3
"""
Standalone Well Number Extraction Script

This script contains the enhanced well number extraction logic that was added to the scraper.
It can be used independently to extract well numbers from text data.
"""

import re
from typing import Optional, List, Tuple

def extract_well_no_from_text(text: str) -> Optional[str]:
    """
    Extract well number from text using enhanced pattern matching.
    
    Args:
        text: Text to search for well numbers
        
    Returns:
        Well number if found, None otherwise
    """
    if not text or not str(text).strip():
        return None
    
    # Look for well number patterns like "303HL", "3BN", "1JM", etc.
    # These are typically 2-6 characters with letters and numbers
    # Prioritize longer patterns first
    well_patterns = [
        r'\b\d{2,4}[A-Z]{2,3}\b',  # Pattern like "303HL", "305HJ" (2-4 digits + 2-3 letters)
        r'\b\d+[A-Z]{1,3}\b',      # Pattern like "3BN", "1JM" (digits + 1-3 letters)
        r'\b[A-Z]\d+[A-Z]*\b',     # Pattern like "H1", "A2B"
        r'\b\d+[A-Z]\d*\b',        # Pattern like "3H", "1A2"
    ]
    
    # Collect all potential well numbers and pick the best one
    all_matches = []
    for pattern in well_patterns:
        matches = re.findall(pattern, str(text).strip())
        all_matches.extend(matches)
    
    # Sort by length (longer is better) and then by pattern priority
    all_matches.sort(key=lambda x: (-len(x), x))
    
    for match in all_matches:
        # Check if this looks like a well number (not a common word or number)
        if (len(match) >= 2 and len(match) <= 6 and 
            not match.isdigit() and 
            not match.lower() in ['usa', 'inc', 'llc', 'e&p', 'co', 'lp', 'api', 'no', 'dp'] and
            not any(exclude_word in match.lower() for exclude_word in [
                'submitted', 'date', 'status', 'operator', 'name', 'number', 'lease', 'dist', 'county', 
                'wellbore', 'profile', 'filing', 'purpose', 'amend', 'total', 'depth', 'stacked', 'lateral', 
                'parent', 'well', 'current', 'queue', 'diamondback', 'chevron', 'pdeh', 'tgnr', 'panola', 
                'wildfire', 'energy', 'operating', 'burlington', 'resources', 'company', 'far', 'cry', 
                'bucco', 'lov', 'unit', 'vital', 'signs', 'monty', 'west', 'presswood', 'oil', 'perseus', 
                'marian', 'yanta', 'tennant', 'usw', 'fox', 'ector', 'midland', 'loving', 'andrews', 'van', 
                'zandt', 'karnes', 'burleson', 'horizontal', 'vertical', 'new', 'drill', 'reenter', 'yes', 
                'no', 'mapping', 'drilling', 'permit', 'verification', 'fasken'
            ])):
            return match
    
    return None

def extract_well_no_from_data(data: dict) -> Optional[str]:
    """
    Extract well number from a data dictionary by checking multiple fields.
    
    Args:
        data: Dictionary containing permit data
        
    Returns:
        Well number if found, None otherwise
    """
    # Fields to check for well numbers (in order of priority)
    fields_to_check = [
        'lease_name',
        'operator_name', 
        'api_no',
        'stacked_lateral_parent_well_dp'
    ]
    
    for field in fields_to_check:
        if field in data and data[field]:
            well_no = extract_well_no_from_text(data[field])
            if well_no:
                return well_no
    
    return None

def test_well_number_extraction():
    """Test the well number extraction with sample data."""
    
    test_cases = [
        # Test cases that should extract well numbers
        {
            'name': 'Well number in lease name',
            'data': {'lease_name': 'FASKEN 1A 303HL', 'operator_name': 'OVINTIV USA INC.'},
            'expected': '303HL'
        },
        {
            'name': 'Well number in operator field',
            'data': {'lease_name': 'FASKEN 1B', 'operator_name': 'OVINTIV USA INC. 305HJ'},
            'expected': '305HJ'
        },
        {
            'name': 'Well number in API field',
            'data': {'lease_name': 'FAR CRY 40', 'api_no': '135-44169 3BN'},
            'expected': '3BN'
        },
        {
            'name': 'Complex lease name',
            'data': {'lease_name': 'F14C MARIAN AN', 'operator_name': 'TGNR PANOLA LLC'},
            'expected': 'F14C'
        },
        # Test cases that should NOT extract well numbers
        {
            'name': 'No well number',
            'data': {'lease_name': 'FAR CRY 40', 'operator_name': 'DIAMONDBACK E&P LLC'},
            'expected': None
        },
        {
            'name': 'Common words only',
            'data': {'lease_name': 'DIAMONDBACK E&P LLC', 'operator_name': 'USA INC.'},
            'expected': None
        }
    ]
    
    print("Testing Well Number Extraction:")
    print("=" * 50)
    
    for test_case in test_cases:
        print(f"\nTest: {test_case['name']}")
        print(f"Input: {test_case['data']}")
        
        result = extract_well_no_from_data(test_case['data'])
        print(f"Extracted: {result}")
        print(f"Expected: {test_case['expected']}")
        
        if result == test_case['expected']:
            print("✅ PASS")
        else:
            print("❌ FAIL")
    
    print("\n" + "=" * 50)
    print("Test completed!")

if __name__ == "__main__":
    test_well_number_extraction()
