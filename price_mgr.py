import time
import datetime
import logging
from web3 import Web3
from pancake_v4 import PancakeV4Dex
from uniswap_v4 import UniswapV4Dex
from aerodrome_v3 import AerodromeV3Dex
from data import insert_historical, upsert_latest, insert_rave_cex_history, upsert_penrose_cex_latest
from aster_future import get_latest_funding_rate
from aster_spot import get_latest_price_spot

logging.basicConfig(filename='log', level=logging.INFO)
# 实际主网合约地址请替换
PANCAKE_ID = '0x101552cfd9d16f17db7d11fde6082e4671e9fe39cb21679bb3fad5be9e5ec2c9'
PANCAKE_MGR = Web3.to_checksum_address('0xa0FfB9c1CE1Fe56963B0321B32E7A0302114058b')

UNISWAP_ID = '0x72331fcb696b0151904c03584b66dc8365bc63f8a144d89a773384e3a579ca73'
UNISWAP_STATE_VIEW = Web3.to_checksum_address('0x7ffe42c4a5deea5b0fec41c94c136cf115597227')  # USDT on Ethereum

AERO_PAIR = Web3.to_checksum_address('0x9785ef59e2b499fb741674ecf6faf912df7b3c1b')
QUOTE_TOKEN_AERODROME = Web3.to_checksum_address('0xfde4C96c8593536E31F229EA8f37b2ADa2699bb2')  # USDT on Base

def main():
    pancake = PancakeV4Dex(PANCAKE_ID, PANCAKE_MGR)
    uniswap = UniswapV4Dex(UNISWAP_ID, UNISWAP_STATE_VIEW)
    aerodrome = AerodromeV3Dex(AERO_PAIR, quote_token_address=QUOTE_TOKEN_AERODROME)

    while True:
        now = datetime.datetime.now()
        try:
            price_pancake = pancake.get_price()
            insert_historical(0, price_pancake, now)
            upsert_latest(0, price_pancake, now)
        except Exception as e:
            logging.info("PancakeV3Dex error: %s", e)
        try:
            price_uniswap = uniswap.get_price()
            insert_historical(1, price_uniswap, now)
            upsert_latest(1, price_uniswap, now)
        except Exception as e:
            logging.info("UniswapV3Dex error: %s", e)
        try:
            price_aero = aerodrome.get_price()
            insert_historical(2, price_aero, now)
            upsert_latest(2, price_aero, now)
        except Exception as e:
            logging.info("AerodromeV3Dex error: %s", e)
        try:
            mark_price, index_price, funding_rate = get_latest_funding_rate('RAVEUSDT')
            spot_price = get_latest_price_spot('RAVEUSDT')
            upsert_penrose_cex_latest(
                6, 'RAVE', spot_price, index_price, mark_price, funding_rate, now
            )
            insert_rave_cex_history(
                6, spot_price, index_price, mark_price, funding_rate, now
            )
        except Exception as e:
            logging.info("Error fetching funding rate or spot price: %s", e)

        time.sleep(60)

if __name__ == "__main__":
    main()
