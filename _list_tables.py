import os, psycopg
with psycopg.connect(os.getenv("DATABASE_URL"), sslmode="require") as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type='BASE TABLE'
              AND table_schema NOT IN ('pg_catalog','information_schema')
            ORDER BY table_schema, table_name;
        """)
        for s,t in cur.fetchall():
            print(f"{s}.{t}")
