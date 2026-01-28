"""Create database tables for DEX price tracking."""
import logging
import os
import dotenv
import psycopg2
dotenv.load_dotenv()

conn = psycopg2.connect(
    dbname=os.environ.get('PG_DATABASE', 'your_db'),
    user=os.environ.get('PG_USER', 'your_user'),
    password=os.environ.get('PG_PASSWORD', 'your_password'),
    host=os.environ.get('PG_HOST', 'localhost'),
    port=os.environ.get('PG_PORT', '5432')
)
cur = conn.cursor()

create_sql = '''
CREATE TABLE IF NOT EXISTS rave_dex_latest (
    id SERIAL PRIMARY KEY,
    dex_type SMALLINT NOT NULL,
    price NUMERIC NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS rave_dex_historical (
    id SERIAL PRIMARY KEY,
    dex_type SMALLINT NOT NULL,
    price NUMERIC NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rave_dex_historical_created_at ON rave_dex_historical USING BTREE (created_at);
CREATE INDEX IF NOT EXISTS idx_rave_dex_historical_dex_type ON rave_dex_historical USING BTREE (dex_type);

CREATE TABLE IF NOT EXISTS token_pair_volume_hourly (
    token_pair VARCHAR(128) NOT NULL,
    type VARCHAR(32) NOT NULL,
    volume NUMERIC NOT NULL,
    open_time TIMESTAMPTZ NOT NULL,
    close_time TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (token_pair, type, open_time)
);

CREATE INDEX IF NOT EXISTS idx_token_pair_volume_hourly_token_pair ON token_pair_volume_hourly USING BTREE (token_pair);
CREATE INDEX IF NOT EXISTS idx_token_pair_volume_hourly_type ON token_pair_volume_hourly USING BTREE (type);
CREATE INDEX IF NOT EXISTS idx_token_pair_volume_hourly_open_time ON token_pair_volume_hourly USING BTREE (open_time);
CREATE INDEX IF NOT EXISTS idx_token_pair_volume_hourly_close_time ON token_pair_volume_hourly USING BTREE (close_time);
CREATE INDEX IF NOT EXISTS idx_token_pair_volume_hourly_pair_type ON token_pair_volume_hourly USING BTREE (token_pair, type);
'''

cur.execute(create_sql)
conn.commit()
cur.close()
conn.close()
logging.info('Tables and indexes created successfully.')

