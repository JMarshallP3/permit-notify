#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def verify_changes():
    """Verify the schema changes were applied."""
    
    print("🔍 VERIFYING SCHEMA CHANGES")
    print("=" * 50)
    
    try:
        from db.session import get_session
        from db.models import Permit
        import sqlalchemy as sa
        
        with get_session() as session:
            # Check table structure
            inspector = sa.inspect(session.bind)
            columns = inspector.get_columns('permits')
            
            print("📋 Current table structure:")
            for i, col in enumerate(columns, 1):
                print(f"   {i:2}. {col['name']:25} {str(col['type']):20} {'NULL' if col['nullable'] else 'NOT NULL'}")
            
            # Check if removed columns are gone
            column_names = [col['name'] for col in columns]
            
            removed_columns = ['submission_date', 'w1_field_name', 'w1_well_count']
            print(f"\n✅ REMOVED COLUMNS CHECK:")
            for col in removed_columns:
                if col not in column_names:
                    print(f"   ✅ Column '{col}' successfully removed")
                else:
                    print(f"   ❌ Column '{col}' still exists")
            
            # Check column order (created_at should be last)
            print(f"\n✅ COLUMN ORDER CHECK:")
            if column_names[-1] == 'created_at':
                print("   ✅ created_at is now the last column")
            else:
                print(f"   ❌ created_at is not last (last column: {column_names[-1]})")
            
            # Check updated_at position (should be second to last)
            if len(column_names) >= 2 and column_names[-2] == 'updated_at':
                print("   ✅ updated_at is second to last column")
            else:
                print(f"   ❌ updated_at position issue")
            
            # Test operator name/number splitting on sample records
            print(f"\n📊 OPERATOR NAME/NUMBER SPLITTING CHECK:")
            samples = session.query(Permit).limit(5).all()
            for sample in samples:
                print(f"   {sample.status_no}: '{sample.operator_name}' | Number: {sample.operator_number}")
            
            # Test status_date formatting
            print(f"\n📅 STATUS DATE FORMATTING CHECK:")
            sample_with_date = session.query(Permit).filter(Permit.status_date.isnot(None)).first()
            if sample_with_date:
                # Test the to_dict method which should format as MM-DD-YYYY
                dict_data = sample_with_date.to_dict()
                print(f"   Raw date: {sample_with_date.status_date}")
                print(f"   Formatted (MM-DD-YYYY): {dict_data['status_date']}")
            
            print(f"\n🎉 SCHEMA VERIFICATION COMPLETE!")
        
        return True
        
    except Exception as e:
        print(f"❌ Verification failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    verify_changes()
