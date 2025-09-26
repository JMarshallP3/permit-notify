#!/usr/bin/env python3
"""Test the field name learning system."""

import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.field_learning import field_learning
from db.session import get_session
from db.models import Permit

async def test_field_learning():
    """Test the intelligent field name learning system."""
    
    print("🤖 TESTING FIELD NAME LEARNING SYSTEM")
    print("=" * 60)
    
    try:
        # Get a permit with a potentially wrong field name
        with get_session() as session:
            permit = session.query(Permit).filter(
                Permit.field_name.isnot(None)
            ).first()
            
            if not permit:
                print("❌ No permits found to test with")
                return
            
            print(f"📋 Testing with permit: {permit.status_no}")
            print(f"   Lease: {permit.lease_name}")
            print(f"   Current Field: {permit.field_name}")
            print(f"   Operator: {permit.operator_name}")
            print()
            
            # Test 1: Record a correction
            print("🎯 Test 1: Recording a field name correction")
            test_correction = "SPRABERRY (TREND AREA)"
            
            success = field_learning.record_correction(
                permit_id=permit.id,
                status_no=permit.status_no,
                wrong_field=permit.field_name,
                correct_field=test_correction,
                detail_url=permit.detail_url
            )
            
            if success:
                print(f"   ✅ Successfully recorded correction: '{permit.field_name}' → '{test_correction}'")
            else:
                print(f"   ❌ Failed to record correction")
            
            print()
            
            # Test 2: Get suggestion
            print("🤖 Test 2: Getting AI suggestion")
            suggestion = field_learning.suggest_field_name(
                permit.field_name,
                permit.lease_name,
                permit.operator_name
            )
            
            if suggestion:
                print(f"   ✅ AI Suggestion: '{suggestion}'")
            else:
                print(f"   ℹ️  No suggestion available")
            
            print()
            
            # Test 3: Get stats
            print("📊 Test 3: Learning system statistics")
            stats = field_learning.get_correction_stats()
            
            print(f"   📈 Total corrections: {stats.get('total_corrections', 0)}")
            print(f"   🧠 Learned patterns: {stats.get('learned_patterns', 0)}")
            
            if stats.get('most_common_errors'):
                print(f"   🔍 Most common errors:")
                for error, count in stats['most_common_errors'][:3]:
                    print(f"      • '{error}': {count} times")
            
            print()
            
            # Test 4: Apply learned corrections
            print("🔄 Test 4: Applying learned corrections")
            result = field_learning.apply_learned_corrections(limit=5)
            
            if 'corrected' in result:
                print(f"   ✅ Applied corrections to {result['corrected']} permits")
                print(f"   ⏭️  Skipped {result.get('skipped', 0)} permits")
            else:
                print(f"   ❌ Error: {result.get('error', 'Unknown error')}")
            
            print()
            print("🎉 Field name learning system test completed!")
            print()
            print("💡 HOW TO USE:")
            print("   1. Open PermitTracker in your browser")
            print("   2. Click '🎯 Correct Field' on any permit with wrong field name")
            print("   3. Enter the correct geological field name")
            print("   4. System learns and applies to similar permits automatically!")
            print("   5. Use '🤖 AI Suggest' to get suggestions based on learned patterns")
            
    except Exception as e:
        print(f"❌ Error testing field learning: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_field_learning())
