#!/usr/bin/env python3
"""Debug specific field extraction issues."""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def debug_field_extraction():
    """Debug specific field extraction issues."""
    
    print("🔍 Debugging specific field extraction:")
    
    try:
        from db.session import get_session
        from db.models import Permit
        from services.enrichment.worker import EnrichmentWorker
        from lxml import html
        
        # Get a permit with detail_url
        with get_session() as session:
            permit = session.query(Permit).filter(
                Permit.detail_url.isnot(None)
            ).first()
            
            if not permit:
                print("❌ No permits with detail_url found")
                return
            
            # Fetch detail page
            worker = EnrichmentWorker()
            detail_response = worker._make_request(permit.detail_url)
            
            if not detail_response:
                print("❌ Failed to fetch detail page")
                return
            
            # Parse HTML
            tree = html.fromstring(detail_response.text)
            
            # Find the main table
            tables = tree.xpath("//table")
            main_table = None
            for table in tables:
                table_text = table.text_content()
                if "horizontal wellbore" in table_text.lower() and "field" in table_text.lower() and "acres" in table_text.lower():
                    main_table = table
                    break
            
            if not main_table:
                print("❌ Main table not found")
                return
            
            # Get all cells from the main table
            all_cells = main_table.xpath(".//*[self::th or self::td]")
            cell_texts = [cell.text_content().strip() for cell in all_cells]
            
            print(f"\n🔍 Looking for 'Horizontal Wellbore' context:")
            for i, text in enumerate(cell_texts):
                if "horizontal wellbore" in text.lower() and len(text) < 50:
                    print(f"   Found at index {i}: '{text}'")
                    # Show next 10 cells
                    for j in range(i+1, min(i+11, len(cell_texts))):
                        next_text = cell_texts[j]
                        print(f"     {j}: '{next_text}'")
                    break
            
            print(f"\n🔍 Looking for 'Field Name' context:")
            for i, text in enumerate(cell_texts):
                if "field name" in text.lower() and len(text) < 50:
                    print(f"   Found at index {i}: '{text}'")
                    # Show next 10 cells
                    for j in range(i+1, min(i+11, len(cell_texts))):
                        next_text = cell_texts[j]
                        print(f"     {j}: '{next_text}'")
                    break
            
            print(f"\n🔍 Looking for 'Acres' context:")
            for i, text in enumerate(cell_texts):
                if text.lower() == "acres" and len(text) < 10:
                    print(f"   Found at index {i}: '{text}'")
                    # Show next 10 cells
                    for j in range(i+1, min(i+11, len(cell_texts))):
                        next_text = cell_texts[j]
                        print(f"     {j}: '{next_text}'")
                    break
                
    except Exception as e:
        print(f"❌ Error debugging field extraction: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_field_extraction()
