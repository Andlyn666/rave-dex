import os
import psycopg2
from dotenv import load_dotenv
load_dotenv()

def get_conn():
    return psycopg2.connect(
        dbname=os.environ.get('PG_DATABASE', 'your_db'),
        user=os.environ.get('PG_USER', 'your_user'),
        password=os.environ.get('PG_PASSWORD', 'your_password'),
        host=os.environ.get('PG_HOST', 'localhost'),
        port=os.environ.get('PG_PORT', '5432')
    )

def insert_historical(dex_type, price, created_at):
    conn = get_conn()
    cur = conn.cursor()
    sql = """
        INSERT INTO rave_dex_historical (dex_type, price, created_at)
        VALUES (%s, %s, %s)
    """
    cur.execute(sql, (dex_type, price, created_at))
    conn.commit()
    cur.close()
    conn.close()

def upsert_latest(dex_type, price, created_at):
    conn = get_conn()
    cur = conn.cursor()
    sql = """
        INSERT INTO rave_dex_latest (dex_type, price, created_at)
        VALUES (%s, %s, %s)
        ON CONFLICT (dex_type)
        DO UPDATE SET price = EXCLUDED.price, created_at = EXCLUDED.created_at
    """
    cur.execute(sql, (dex_type, price, created_at))
    conn.commit()
    cur.close()
    conn.close()
