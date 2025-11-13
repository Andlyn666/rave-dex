import time
import datetime
import logging
from web3 import Web3
from pancake_v3 import PancakeV3Dex
from uniswap_v3 import UniswapV3Dex
from aerodrome_v3 import AerodromeV3Dex
from data import insert_historical, upsert_latest

logging.basicConfig(filename='log', level=logging.INFO)
# 实际主网合约地址请替换
PANCAKE_PAIR = Web3.to_checksum_address('0x84354592cb82EAc7fac65df4478ED1eEbBa0252c')
QUOTE_TOKEN_PANCAKE = Web3.to_checksum_address('0x55d398326f99059fF775485246999027B3197955')  # USDT on BSC

UNISWAP_PAIR = Web3.to_checksum_address('0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8')
QUOTE_TOKEN_UNISWAP = Web3.to_checksum_address('0xdAC17F958D2ee523a2206206994597C13D831ec7')  # USDT on Ethereum

AERO_PAIR = Web3.to_checksum_address('0xE6C694f8B9EE84353a10de59c9b4cDEFa0F5b4Ad')
QUOTE_TOKEN_AERODROME = Web3.to_checksum_address('0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913')  # USDT on Base

def main():
    pancake = PancakeV3Dex(PANCAKE_PAIR, quote_token_address=QUOTE_TOKEN_PANCAKE)
    uniswap = UniswapV3Dex(UNISWAP_PAIR, quote_token_address=QUOTE_TOKEN_UNISWAP)
    aerodrome = AerodromeV3Dex(AERO_PAIR, quote_token_address=QUOTE_TOKEN_AERODROME)

    while True:
        now = datetime.datetime.now()
        try:
            price_pancake = pancake.get_price()
            insert_historical(0, price_pancake, now)
            upsert_latest(0, price_pancake, now)
        except Exception as e:
            logging.info(f"PancakeV3Dex error: {e}")
        try:
            price_uniswap = uniswap.get_price()
            insert_historical(1, price_uniswap, now)
            upsert_latest(1, price_uniswap, now)
        except Exception as e:
            logging.info(f"UniswapV3Dex error: {e}")
        try:
            price_aero = aerodrome.get_price()
            insert_historical(2, price_aero, now)
            upsert_latest(2, price_aero, now)
        except Exception as e:
            logging.info(f"AerodromeV3Dex error: {e}")
        time.sleep(5)

if __name__ == "__main__":
    main()
