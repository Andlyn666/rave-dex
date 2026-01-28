"""Fetch K-line volume data from Aster API and insert into database."""
import logging
import os
from datetime import datetime, timedelta
from typing import List, Optional
import dotenv
import psycopg2
import requests


# API base url
API_HOST = 'https://sapi.asterdex.com'
FUTURES_API_HOST = 'https://fapi.asterdex.com'
ALPHA_API_HOST = 'https://www.binance.com'

dotenv.load_dotenv()

MAX_KLINE_LIMIT = 1500


def get_db_connection():
    """Create and return database connection."""
    return psycopg2.connect(
        dbname=os.environ.get('PG_DATABASE', 'your_db'),
        user=os.environ.get('PG_USER', 'your_user'),
        password=os.environ.get('PG_PASSWORD', 'your_password'),
        host=os.environ.get('PG_HOST', 'localhost'),
        port=os.environ.get('PG_PORT', '5432')
    )


def get_klines(symbol: str, interval: str = '1d', start_time: Optional[int] = None, 
               end_time: Optional[int] = None, limit: int = 500) -> List[List]:
    """
    Fetch K-line data from Aster Spot API.
    
    Args:
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        interval: K-line interval (e.g., '1d', '1h', '1m')
        start_time: Start time in milliseconds (optional)
        end_time: End time in milliseconds (optional)
        limit: Number of klines to return (default 500, max 1500)
    
    Returns:
        List of kline data arrays
    """
    url = f'{API_HOST}/api/v1/klines'
    params = {
        'symbol': symbol,
        'interval': interval,
        'limit': limit
    }
    
    if start_time:
        params['startTime'] = start_time
    if end_time:
        params['endTime'] = end_time
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        logging.info("Fetched %d klines for %s", len(data), symbol)
        return data
    except requests.exceptions.RequestException as e:
        logging.error("Error fetching klines for %s: %s", symbol, e)
        raise


def get_klines_futures(symbol: str, interval: str = '1d', start_time: Optional[int] = None, 
                       end_time: Optional[int] = None, limit: int = 500) -> List[List]:
    """
    Fetch K-line data from Aster Futures API.
    
    Args:
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        interval: K-line interval (e.g., '1d', '1h', '1m')
        start_time: Start time in milliseconds (optional)
        end_time: End time in milliseconds (optional)
        limit: Number of klines to return (default 500, max 1500)
    
    Returns:
        List of kline data arrays
    """
    url = f'{FUTURES_API_HOST}/fapi/v1/klines'
    params = {
        'symbol': symbol,
        'interval': interval,
        'limit': limit
    }
    
    if start_time:
        params['startTime'] = start_time
    if end_time:
        params['endTime'] = end_time
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        logging.info("Fetched %d futures klines for %s", len(data), symbol)
        return data
    except requests.exceptions.RequestException as e:
        logging.error("Error fetching futures klines for %s: %s", symbol, e)
        raise


def get_klines_alpha(symbol: str, interval: str = '1d', start_time: Optional[int] = None, 
                     end_time: Optional[int] = None, limit: int = 500) -> List[List]:
    """
    Fetch K-line data from Binance Alpha API.
    
    Args:
        symbol: Trading pair symbol (e.g., 'ALPHA_175USDT')
        interval: K-line interval (e.g., '1d', '1h', '1m', '1s', '15s')
        start_time: Start time in milliseconds (optional)
        end_time: End time in milliseconds (optional)
        limit: Number of klines to return (default 500, max 1500)
    
    Returns:
        List of kline data arrays
    """
    url = f'{ALPHA_API_HOST}/bapi/defi/v1/public/alpha-trade/klines'
    params = {
        'symbol': symbol,
        'interval': interval,
        'limit': limit
    }
    
    if start_time:
        params['startTime'] = start_time
    if end_time:
        params['endTime'] = end_time
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        result = response.json()
        
        # Alpha API returns {code, message, success, data}
        if result.get('success') and result.get('code') == '000000':
            data = result.get('data', [])
            logging.info("Fetched %d alpha klines for %s", len(data), symbol)
            return data
        else:
            error_msg = result.get('message', 'Unknown error')
            logging.error("Alpha API error for %s: %s", symbol, error_msg)
            raise requests.exceptions.RequestException(f"Alpha API error: {error_msg}")
    except requests.exceptions.RequestException as e:
        logging.error("Error fetching alpha klines for %s: %s", symbol, e)
        raise


def insert_kline_volume(conn, token_pair: str, volume: str, open_time: int, close_time: int, data_type: str):
    """
    Insert K-line volume data into database.
    
    Args:
        conn: Database connection
        token_pair: Trading pair symbol
        volume: Volume value as string
        open_time: Open time in milliseconds
        close_time: Close time in milliseconds
        data_type: Data source type ('aster_spot', 'aster_future', 'alpha')
    """
    # Convert milliseconds to PostgreSQL TIMESTAMPTZ
    open_time_ts = datetime.fromtimestamp(open_time / 1000)
    close_time_ts = datetime.fromtimestamp(close_time / 1000)
    
    cur = conn.cursor()
    try:
        insert_sql = """
            INSERT INTO token_pair_volume_hourly (token_pair, type, volume, open_time, close_time)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (token_pair, type, open_time) DO UPDATE
            SET volume = EXCLUDED.volume, close_time = EXCLUDED.close_time
        """
        cur.execute(insert_sql, (token_pair, data_type, volume, open_time_ts, close_time_ts))
        conn.commit()
        logging.debug("Inserted/Updated: %s (%s) at %s", token_pair, data_type, open_time_ts)
    except psycopg2.Error as e:
        conn.rollback()
        logging.error("Error inserting data for %s (%s): %s", token_pair, data_type, e)
        raise
    finally:
        cur.close()


def _normalize_kline_fields(kline: List) -> tuple[int, int, str]:
    """
    Normalize kline fields across providers.

    Expected formats:
    - Aster spot/future: [open_time(ms:int), ..., volume(str), close_time(ms:int), ...]
    - Alpha:            [open_time(ms:str), ..., volume(str), close_time(ms:str), ...]
    """
    open_time_raw = kline[0]
    close_time_raw = kline[6]
    volume = kline[5]

    open_time_ms = int(open_time_raw) if isinstance(open_time_raw, str) else open_time_raw
    close_time_ms = int(close_time_raw) if isinstance(close_time_raw, str) else close_time_raw
    return open_time_ms, close_time_ms, volume


def _fetch_and_store_volume_range(
    fetch_fn,
    symbol: str,
    interval: str,
    start_time_ms: int,
    end_time_ms: int,
    data_type: str,
):
    """
    Fetch klines for [start_time_ms, end_time_ms] and upsert into DB.
    Uses pagination to handle ranges that exceed API limit.
    """
    conn = get_db_connection()
    try:
        cursor_start = start_time_ms
        while cursor_start < end_time_ms:
            klines = fetch_fn(symbol, interval, cursor_start, end_time_ms, limit=MAX_KLINE_LIMIT)
            if not klines:
                break

            # Ensure ascending by open_time
            klines_sorted = sorted(klines, key=lambda k: int(k[0]) if isinstance(k[0], str) else k[0])

            last_open_time_ms = None
            for kline in klines_sorted:
                open_time_ms, close_time_ms, volume = _normalize_kline_fields(kline)
                insert_kline_volume(conn, symbol, volume, open_time_ms, close_time_ms, data_type)
                last_open_time_ms = open_time_ms

            if last_open_time_ms is None:
                break

            # Advance cursor; +1ms avoids repeating the last candle
            next_cursor = last_open_time_ms + 1
            if next_cursor <= cursor_start:
                # Safety against non-advancing responses
                break
            cursor_start = next_cursor

            # If we didn't hit the limit, we likely exhausted the range
            if len(klines_sorted) < MAX_KLINE_LIMIT:
                break
    finally:
        conn.close()


def fetch_and_store_volume_aster_spot(symbol: str, interval: str = '1h', days_back: int = 1):
    """
    Fetch K-line volume data for a symbol and store in database.
    
    Args:
        symbol: Trading pair symbol
        interval: K-line interval (default '1d' for daily)
        days_back: Number of days back to fetch (default 1 for yesterday)
    """
    # Calculate time range for yesterday
    end_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(days=days_back)
    
    start_time_ms = int(start_time.timestamp() * 1000)
    end_time_ms = int(end_time.timestamp() * 1000)
    
    logging.info("Fetching %s klines for %s from %s to %s", interval, symbol, start_time, end_time)
    
    try:
        # Fetch klines
        klines = get_klines(symbol, interval, start_time_ms, end_time_ms)
        
        if not klines:
            logging.warning("No kline data returned for %s", symbol)
            return
        
        # Connect to database
        conn = get_db_connection()
        
        try:
            # Insert each kline
            for kline in klines:
                # Kline format: [open_time, open, high, low, close, volume, close_time, ...]
                open_time_ms = kline[0]
                volume = kline[5]  # Volume at index 5
                close_time_ms = kline[6]  # Close time at index 6
                
                insert_kline_volume(conn, symbol, volume, open_time_ms, close_time_ms, 'aster_spot')
            
            logging.info("Successfully stored %d klines for %s", len(klines), symbol)
        finally:
            conn.close()
            
    except (requests.exceptions.RequestException, psycopg2.Error) as e:
        logging.error("Error processing %s: %s", symbol, e)
        raise


def fetch_and_store_volume_aster_future(symbol: str, interval: str = '1d', days_back: int = 1):
    """
    Fetch K-line volume data for a futures symbol and store in database.
    
    Args:
        symbol: Trading pair symbol
        interval: K-line interval (default '1d' for daily)
        days_back: Number of days back to fetch (default 1 for yesterday)
    """
    # Calculate time range for yesterday
    end_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(days=days_back)
    
    start_time_ms = int(start_time.timestamp() * 1000)
    end_time_ms = int(end_time.timestamp() * 1000)
    
    logging.info("Fetching %s futures klines for %s from %s to %s", interval, symbol, start_time, end_time)
    
    try:
        # Fetch klines
        klines = get_klines_futures(symbol, interval, start_time_ms, end_time_ms)
        
        if not klines:
            logging.warning("No futures kline data returned for %s", symbol)
            return
        
        # Connect to database
        conn = get_db_connection()
        
        try:
            # Insert each kline
            for kline in klines:
                # Kline format: [open_time, open, high, low, close, volume, close_time, ...]
                open_time_ms = kline[0]
                volume = kline[5]  # Volume at index 5
                close_time_ms = kline[6]  # Close time at index 6
                
                insert_kline_volume(conn, symbol, volume, open_time_ms, close_time_ms, 'aster_future')
            
            logging.info("Successfully stored %d futures klines for %s", len(klines), symbol)
        finally:
            conn.close()
            
    except (requests.exceptions.RequestException, psycopg2.Error) as e:
        logging.error("Error processing futures %s: %s", symbol, e)
        raise


def fetch_and_store_volume_alpha(symbol: str, interval: str = '1d', days_back: int = 1):
    """
    Fetch K-line volume data for an Alpha symbol and store in database.
    
    Args:
        symbol: Trading pair symbol (e.g., 'ALPHA_175USDT')
        interval: K-line interval (default '1d' for daily)
        days_back: Number of days back to fetch (default 1 for yesterday)
    """
    # Calculate time range for yesterday
    end_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(days=days_back)
    
    start_time_ms = int(start_time.timestamp() * 1000)
    end_time_ms = int(end_time.timestamp() * 1000)
    
    logging.info("Fetching %s alpha klines for %s from %s to %s", interval, symbol, start_time, end_time)
    
    try:
        # Fetch klines
        klines = get_klines_alpha(symbol, interval, start_time_ms, end_time_ms)
        
        if not klines:
            logging.warning("No alpha kline data returned for %s", symbol)
            return
        
        # Connect to database
        conn = get_db_connection()
        
        try:
            # Insert each kline
            for kline in klines:
                # Kline format: [open_time, open, high, low, close, volume, close_time, ...]
                # Note: Alpha API returns strings, need to convert open_time and close_time
                open_time_ms = int(kline[0]) if isinstance(kline[0], str) else kline[0]
                volume = kline[5]  # Volume at index 5
                close_time_ms = int(kline[6]) if isinstance(kline[6], str) else kline[6]
                
                insert_kline_volume(conn, symbol, volume, open_time_ms, close_time_ms, 'alpha')
            
            logging.info("Successfully stored %d alpha klines for %s", len(klines), symbol)
        finally:
            conn.close()
            
    except (requests.exceptions.RequestException, psycopg2.Error) as e:
        logging.error("Error processing alpha %s: %s", symbol, e)
        raise


def fill_history_kline_volume(interval: str = '1h', days: int = 7):
    """
    Backfill kline volume for the last `days` days (default: 7).

    This upserts into `token_pair_volume_hourly` using the `(token_pair, type, open_time)` key,
    so it is safe to re-run.
    """
    end_time = datetime.now()
    start_time = end_time - timedelta(days=days)
    start_time_ms = int(start_time.timestamp() * 1000)
    end_time_ms = int(end_time.timestamp() * 1000)

    # Keep defaults aligned with the daily fetch function.
    aster_spot_symbols = ['SPACEUSD1']
    aster_future_symbols = ['SPACEUSDT']
    alpha_symbols = ['ALPHA_606USDT']

    logging.info("Backfilling klines from %s to %s", start_time, end_time)

    for symbol in aster_spot_symbols:
        try:
            _fetch_and_store_volume_range(get_klines, symbol, interval, start_time_ms, end_time_ms, 'aster_spot')
        except (requests.exceptions.RequestException, psycopg2.Error) as e:
            logging.error("Backfill failed for %s (aster_spot): %s", symbol, e)

    for symbol in aster_future_symbols:
        try:
            _fetch_and_store_volume_range(get_klines_futures, symbol, interval, start_time_ms, end_time_ms, 'aster_future')
        except (requests.exceptions.RequestException, psycopg2.Error) as e:
            logging.error("Backfill failed for %s (aster_future): %s", symbol, e)

    for symbol in alpha_symbols:
        try:
            _fetch_and_store_volume_range(get_klines_alpha, symbol, interval, start_time_ms, end_time_ms, 'alpha')
        except (requests.exceptions.RequestException, psycopg2.Error) as e:
            logging.error("Backfill failed for %s (alpha): %s", symbol, e)


def run_daily_kline_volume_fetch():
    """Fetch and store K-line volume data (callable by other modules)."""
    # You can specify symbols to fetch, or fetch all active symbols
    # Option 1: Fetch specific symbols
    
    interval = '1h'  # Daily klines
    days_back = 1    # Fetch yesterday's data
    aster_spot_symbols = ['SPACEUSD1']
    logging.info("Starting to fetch volume data for %d symbols", len(aster_spot_symbols))
    for symbol in aster_spot_symbols:
        try:
            fetch_and_store_volume_aster_spot(symbol, interval, days_back)
        except (requests.exceptions.RequestException, psycopg2.Error) as e:
            logging.error("Failed to process %s: %s", symbol, e)
            continue
    
    aster_future_symbols = ['SPACEUSDT']
    logging.info("Starting to fetch volume data for %d symbols", len(aster_future_symbols))
    for symbol in aster_future_symbols:
        try:
            fetch_and_store_volume_aster_future(symbol, interval, days_back)
        except (requests.exceptions.RequestException, psycopg2.Error) as e:
            logging.error("Failed to process %s: %s", symbol, e)
            continue
    
    alpha_symbols = ['ALPHA_606USDT']
    logging.info("Starting to fetch volume data for %d symbols", len(alpha_symbols))
    for symbol in alpha_symbols:
        try:
            fetch_and_store_volume_alpha(symbol, interval, days_back)
        except (requests.exceptions.RequestException, psycopg2.Error) as e:
            logging.error("Failed to process %s: %s", symbol, e)
            continue
    logging.info("Finished fetching volume data")


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    fill_history_kline_volume()
