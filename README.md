# DEX核心方法用法说明

## UniswapV3Dex

### 初始化
```python
from uniswap_v3 import UniswapV3Dex
from web3 import Web3
pair_address = Web3.to_checksum_address('0x65F53f9edF81B6b4b2a7d40C3Ca56054D4c93b9A')
dex = UniswapV3Dex(pair_address)
```

### 获取价格
```python
price = dex.get_price()
logging.info(f"Current price: {price}")
```

### Swap交易
```python
amount_in = 10 ** 5
amount_out_min = 0
token_in_is0 = False
sqrt_price_limit_x96 = 0
receipt = dex.swap(amount_in, token_in_is0, amount_out_min, sqrt_price_limit_x96)
logging.info(f"Swap receipt: {receipt}")
```

---

## PancakeV3Dex

### 初始化
```python
from pancake_v3 import PancakeV3Dex
from web3 import Web3
pair_address = Web3.to_checksum_address('0x84354592cb82EAc7fac65df4478ED1eEbBa0252c')
dex = PancakeV3Dex(pair_address)
```

### 获取价格
```python
price = dex.get_price()
logging.info(f"Current price: {price}")
```

### Swap交易
```python
amount_in = 10 ** 13
token_in_is0 = True
amount_out_min = 0
sqrt_price_limit_x96 = 0
receipt = dex.swap(amount_in, token_in_is0, amount_out_min, sqrt_price_limit_x96)
logging.info(f"Swap receipt: {receipt}")
```

---

## AerodromeV3Dex

### 初始化
```python
from aerodrome_v3 import AerodromeV3Dex
from web3 import Web3
pair_address = Web3.to_checksum_address('0xE6C694f8B9EE84353a10de59c9b4cDEFa0F5b4Ad')
dex = AerodromeV3Dex(pair_address)
```

### 获取价格
```python
price = dex.get_price()
logging.info(f"Current price: {price}")
```

### Swap交易
```python
amount_in = 10 ** 5
token_in_is0 = True
amount_out_min = 0
sqrt_price_limit_x96 = 0
receipt = dex.swap(amount_in, token_in_is0, amount_out_min, sqrt_price_limit_x96)
logging.info(f"Swap receipt: {receipt}")
```
