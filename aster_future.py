import json
import math
import time
import requests
import logging
from eth_abi import encode
from eth_account import Account
from eth_account.messages import encode_defunct
from web3 import Web3
import dotenv

logging.basicConfig(level=logging.INFO)


dotenv.load_dotenv()
#your main wallet address (eoa)
#你的登陆钱包地址(erc20)
user = dotenv.get_key(".env", "USER_ADDRESS")

#please get these parameters from  https://www.asterdex.com/en/api-wallet
#下面这些参数在这里生成配置  https://www.asterdex.com/zh-CN/api-wallet
signer = dotenv.get_key(".env", "SIGNER_ADDRESS")
priKey = dotenv.get_key(".env", "PRIVATE_KEY")

host = 'https://fapi.asterdex.com'

def place_order(symbol, side, quantity, price, position_side='BOTH', order_type='LIMIT', time_in_force='GTC'):
    api = {
        'url': '/fapi/v3/order',
        'method': 'POST',
        'params': {
            'symbol': symbol,
            'positionSide': position_side,
            'type': order_type,
            'side': side,
            'timeInForce': time_in_force,
            'quantity': quantity,
            'price': price,
        }
    }
    resp = call(api)
    # 解析orderId和status
    try:
        data = json.loads(resp) if isinstance(resp, str) else resp
        order_id = data.get('orderId')
        status = data.get('status')
        if order_id is None or status is None:
            logging.error(f"place_order failed: orderId or status missing, response: {data}")
            raise ValueError("place_order failed: orderId or status missing")
        return order_id, status
    except Exception as e:
        logging.error(f"Error parsing response: {e}")
        raise ValueError("Error parsing response")
def get_order(symbol, side, order_id, order_type='LIMIT'):
    api = {
        'url': '/fapi/v3/order',
        'method': 'GET',
        'params': {
            'symbol': symbol,
            'side': side,
            'type': order_type,
            'orderId': order_id
        }
    }
    return call(api)

# 查询所有合约挂单
def get_open_orders(symbol=None):
    api = {
        'url': '/fapi/v3/openOrders',
        'method': 'GET',
        'params': {
            'symbol': symbol
        } if symbol else {}
    }
    resp = call(api)
    return resp

def cancel_order(symbol, order_id):
    api = {
        'url': '/fapi/v3/order',
        'method': 'DELETE',
        'params': {
            'symbol': symbol,
            'orderId': order_id
        }
    }
    return call(api)

# 获取最新价格
def get_latest_price(symbol):
    url = host + '/fapi/v1/ticker/price'
    params = {'symbol': symbol}
    res = requests.get(url, params=params)
    try:
        data = res.json()
        return data.get('price')
    except Exception as e:
        print(f"Error parsing price response: {e}")
        return None
def get_latest_funding_rate(symbol):
    """
    获取最新资金费率
    :param symbol: 合约交易对，如 'NEIROUSDT'
    :return: 资金费率(float) 或 None
    """
    url = host + '/fapi/v1/premiumIndex'
    params = {'symbol': symbol}
    try:
        res = requests.get(url, params=params)
        data = res.json()
        market_price = round(float(data.get('markPrice', 0)), 6)
        index_price = round(float(data.get('indexPrice', 0)), 6)
        founding_rate = round(float(data.get('lastFundingRate', 0)), 6)
        return market_price, index_price, founding_rate
    except Exception as e:
        logging.error(f"Error fetching funding rate: {e}")
        return None

def call(api):
    nonce = math.trunc(time.time() * 1000000)
    my_dict = api['params']
    return send(api['url'], api['method'], sign(my_dict, nonce))

def sign(my_dict,nonce):
    my_dict = {key: value for key, value in my_dict.items() if  value is not None}
    my_dict['recvWindow'] = 50000
    my_dict['timestamp'] = int(round(time.time()*1000))
    msg = trim_param(my_dict,nonce)
    signable_msg = encode_defunct(hexstr=msg)
    signed_message = Account.sign_message(signable_message=signable_msg, private_key=priKey)
    my_dict['nonce'] = nonce
    my_dict['user'] = user
    my_dict['signer'] = signer
    my_dict['signature'] = '0x'+signed_message.signature.hex()
    return  my_dict

def trim_param(my_dict,nonce) -> str:
    _trim_dict(my_dict)
    json_str = json.dumps(my_dict, sort_keys=True).replace(' ', '').replace('\'','\"')
    encoded = encode(['string', 'address', 'address', 'uint256'], [json_str, user, signer, nonce])
    keccak_hex =Web3.keccak(encoded).hex()
    return keccak_hex

def _trim_dict(my_dict) :
    for key in my_dict:
        value = my_dict[key]
        if isinstance(value, list):
            new_value = []
            for item in value:
                if isinstance(item, dict):
                    new_value.append(json.dumps(_trim_dict(item)))
                else:
                    new_value.append(str(item))
            my_dict[key] = json.dumps(new_value)
            continue
        if isinstance(value, dict):
            my_dict[key] = json.dumps(_trim_dict(value))
            continue
        my_dict[key] = str(value)

    return my_dict

def send(url, method, my_dict):
    url = host + url
    if method == 'POST':
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'PythonApp/1.0'
        }
        res = requests.post(url, data=my_dict, headers=headers)
        return res.text
    if method == 'GET':
        res = requests.get(url, params=my_dict)
        return res.text
    if method == 'DELETE':
        res = requests.delete(url, data=my_dict)
        return res.text



# if __name__ == '__main__':
#     price = get_latest_price('NEIROUSDT')
#     print(f"Latest price for NEIROUSDT: {price}")
#     # 示例调用
#     order_id, status = place_order('NEIROUSDT', 'SELL', '200000', 0.00014, 'SHORT')
#     logger.info(f"Placed order with ID: {order_id}, Status: {status}")
#     order_id2, status2 = place_order('NEIROUSDT', 'BUY', '200000', 0.00014, 'LONG')
#     logger.info(f"Placed order with ID: {order_id2}, Status: {status2}")
#     res = get_open_orders('NEIROUSDT')
#     logger.info(f"Open Orders: {res}")
#     cancel_order('NEIROUSDT', order_id)
#     cancel_order('NEIROUSDT', order_id2)
    