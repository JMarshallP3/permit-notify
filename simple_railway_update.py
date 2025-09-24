# simple_railway_update.py
import os
import psycopg
import argparse

def mask_url(u: str) -> str:
    if not u:
        return "None"
    try:
        prefix, rest = u.split("://", 1)
        if "@" in rest and ":" in rest.split("@")[0]:
            user_pass, host_part = rest.split("@", 1)
            user, _ = user_pass.split(":", 1)
            return f"{prefix}://{user}:***@{host_part}"
        return u
    except Exception:
        return u

def update_railway_directly(verify_only=False) -> bool:
    """Update Railway database with the GREEN BULLET enhanced data."""
    railway_db_url = os.getenv("DATABASE_URL")

    if verify_only:
        print("🔍 VERIFYING RAILWAY DATABASE")
        print("=" * 40)
    else:
        print("🚀 UPDATING RAILWAY DATABASE")
        print("=" * 40)
    print(f"[debug] DATABASE_URL (masked) = {mask_url(railway_db_url)}")

    if not railway_db_url:
        print("❌ Error: DATABASE_URL is not set in the environment.")
        print('   Set it, then rerun:')
        print('   $env:DATABASE_URL = "postgresql://postgres:<PASSWORD>@ballast.proxy.rlwy.net:57963/railway?sslmode=require"')
        return False

    try:
        print("🔄 Connecting to Railway database...")
        with psycopg.connect(railway_db_url, sslmode="require") as conn:
            with conn.cursor() as cur:
                print("✅ Connected successfully!")

                updates = [
                    {
                        'status_no': '910678',
                        'section': '15',
                        'block': '28',
                        'survey': 'PSL',
                        'abstract_no': 'A-980',
                        'acres': 1284.37,
                        'field_name': 'PHANTOM (WOLFCAMP)',
                        'reservoir_well_count': 2
                    },
                    {
                        'status_no': '910679',
                        'section': '15',
                        'block': '28',
                        'survey': 'PSL',
                        'abstract_no': 'A-980',
                        'acres': 1284.37,
                        'field_name': 'PHANTOM (WOLFCAMP)',
                        'reservoir_well_count': 3
                    },
                    {
                        'status_no': '910681',
                        'section': '15',
                        'block': '28',
                        'survey': 'PSL',
                        'abstract_no': 'A-980',
                        'acres': 1284.37,
                        'field_name': 'PHANTOM (WOLFCAMP)',
                        'reservoir_well_count': 4
                    }
                ]

                if not verify_only:
                    print("📝 Updating GREEN BULLET permits with enhanced data...")
                    for upd in updates:
                        try:
                            cur.execute(
                                """
                                UPDATE public.permits SET 
                                    section = %s,
                                    block = %s,
                                    survey = %s,
                                    abstract_no = %s,
                                    acres = %s,
                                    field_name = %s,
                                    reservoir_well_count = %s,
                                    updated_at = NOW()
                                WHERE status_no = %s
                                """,
                                (
                                    upd['section'],
                                    upd['block'],
                                    upd['survey'],
                                    upd['abstract_no'],
                                    upd['acres'],
                                    upd['field_name'],
                                    upd['reservoir_well_count'],
                                    upd['status_no'],
                                ),
                            )
                            if cur.rowcount == 0:
                                print(f"   ⚠️  No rows matched status_no {upd['status_no']} (check value).")
                            else:
                                print(f"   ✅ Updated permit {upd['status_no']} → wells: {upd['reservoir_well_count']}")
                        except Exception as e:
                            print(f"   ❌ Error updating permit {upd['status_no']}: {e}")

                    conn.commit()
                    print("\n🎉 All updates committed to Railway!")
                else:
                    print("🔍 Skipping updates (verify-only mode)...")

                # Verify the updates
                if verify_only:
                    print("\n🔍 Checking current data...")
                else:
                    print("\n🔍 Verifying updates...")
                cur.execute(
                    """
                    SELECT status_no, lease_name, section, block, survey, abstract_no, 
                           acres, field_name, reservoir_well_count
                    FROM public.permits
                    WHERE status_no IN ('910678', '910679', '910681')
                    ORDER BY status_no
                    """
                )
                rows = cur.fetchall()
                if rows:
                    print("\n📊 UPDATED DATA IN RAILWAY:")
                    print("Status   | Lease Name                | Sec | Blk | Survey | Abstract | Acres   | Field Name         | Wells")
                    print("---------|---------------------------|-----|-----|--------|----------|---------|--------------------|------")
                    for r in rows:
                        status_no   = r[0]
                        lease_name  = (r[1] or 'N/A')[:25]
                        section     = r[2] or 'N/A'
                        block       = r[3] or 'N/A'
                        survey      = r[4] or 'N/A'
                        abstract_no = r[5] or 'N/A'
                        acres       = f"{r[6]:.1f}" if r[6] is not None else 'N/A'
                        field_name  = (r[7] or 'N/A')[:18]
                        wells       = r[8] or 'N/A'
                        print(f"{status_no:<8} | {lease_name:<25} | {section:<3} | {block:<3} | {survey:<6} | {abstract_no:<8} | {acres:<7} | {field_name:<18} | {wells}")
                else:
                    print("⚠️  No matching rows found for verification (check status_no values).")

                return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update Railway database with enhanced permit data")
    parser.add_argument("--verify-only", action="store_true", 
                        help="Only verify current data without making updates")
    args = parser.parse_args()
    
    ok = update_railway_directly(verify_only=args.verify_only)
    if ok:
        if args.verify_only:
            print("\n🎉 SUCCESS! Railway database verification complete!")
            print("   Data shown above reflects current state in Railway.")
        else:
            print("\n🎉 SUCCESS! Railway database updated with enhanced parsing data!")
            print("   • Section: 15, Block: 28, Survey: PSL")
            print("   • Abstract: A-980, Acres: 1284.37")
            print("   • Field: PHANTOM (WOLFCAMP)")
            print("   • Reservoir Well Count: 2, 3, 4 (FIXED!)")
    else:
        if args.verify_only:
            print("\n❌ Verification failed. Please check the errors above.")
        else:
            print("\n❌ Update failed. Please check the errors above.")


