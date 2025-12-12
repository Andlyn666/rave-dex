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

def upsert_penrose_cex_latest(cex, symbol, spot_price, index_price, mark_price, funding_rate, timestamp):
    conn = get_conn()
    cur = conn.cursor()
    sql = """
        INSERT INTO penrose_cex_latest (
            cex, symbol, spot_price, index_price, mark_price, funding_rate, timestamp
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (cex, symbol)
        DO UPDATE SET
            spot_price = EXCLUDED.spot_price,
            index_price = EXCLUDED.index_price,
            mark_price = EXCLUDED.mark_price,
            funding_rate = EXCLUDED.funding_rate,
            timestamp = EXCLUDED.timestamp;
    """
    cur.execute(sql, (cex, symbol, spot_price, index_price, mark_price, funding_rate, timestamp))
    conn.commit()
    cur.close()
    conn.close()

def insert_rave_cex_history(cex, spot_price, index_price, mark_price, funding_rate, timestamp):
    conn = get_conn()
    cur = conn.cursor()
    sql = """
        INSERT INTO rave_cex_history (
            cex, spot_price, index_price, mark_price, funding_rate, timestamp
        ) VALUES (
            %s, %s, %s, %s, %s, %s
        )
    """
    cur.execute(sql, (cex, spot_price, index_price, mark_price, funding_rate, timestamp))
    conn.commit()
    cur.close()
    conn.close()

