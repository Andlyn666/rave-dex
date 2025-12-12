import time
import hmac
import hashlib
import requests
import dotenv
import logging

logging.basicConfig(level=logging.INFO)

# API base url for spot trading
host = 'https://sapi.asterdex.com'

dotenv.load_dotenv()
signer = dotenv.get_key(".env", "SPOT_API_KEY")
priKey = dotenv.get_key(".env", "SPOT_API_SECRET")

# 签名方法（HMAC SHA256），保持参数顺序与实际请求一致
def sign(params, secret):
    # 只签名未带signature的参数，且顺序与params一致
    query_string = '&'.join([f"{k}={params[k]}" for k in params if k != 'signature'])
    signature = hmac.new(secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
    return signature

# 下单
# type: LIMIT/MARKET/STOP/TAKE_PROFIT/STOP_MARKET/TAKE_PROFIT_MARKET
# timeInForce: GTC/IOC/FOK/GTX
def place_order(symbol, side, quantity=None, quoteOrderQty=None, price=None, newClientOrderId=None, stopPrice=None, type='LIMIT', time_in_force='GTC'):
    params = {
        'symbol': symbol,
        'side': side,
        'type': type,
        'quantity': quantity,
        'quoteOrderQty': quoteOrderQty,
        'price': price,
        'timeInForce': time_in_force,
        'newClientOrderId': newClientOrderId,
        'stopPrice': stopPrice,
        'recvWindow': 5000,
        'timestamp': int(round(time.time()*1000))
    }
    # 移除None参数
    params = {k: v for k, v in params.items() if v is not None}
    params['signature'] = sign(params, priKey)
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'X-MBX-APIKEY': signer,
        'User-Agent': 'PythonApp/1.0'
    }
    url = host + '/api/v1/order'
    res = requests.post(url, data=params, headers=headers)
    try:
        print(res.text)
        data = res.json()
        order_id = data.get('orderId')
        status = data.get('status')
        if order_id is None or status is None:
            logging.error(f"place_order failed: orderId or status missing, response: {data}")
            raise ValueError("place_order failed: orderId or status missing")
        return order_id, status
    except Exception as e:
        logging.error(f"Error parsing response: {e}")
        raise ValueError("Error parsing response")

# 撤销订单
# symbol 必填, orderId 或 origClientOrderId 必填
def cancel_order(symbol, order_id=None, origClientOrderId=None):
    params = {
        'symbol': symbol,
        'orderId': order_id,
        'origClientOrderId': origClientOrderId,
        'recvWindow': 5000,
        'timestamp': int(round(time.time()*1000))
    }
    params = {k: v for k, v in params.items() if v is not None}
    params['signature'] = sign(params, priKey)
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'X-MBX-APIKEY': signer,
        'User-Agent': 'PythonApp/1.0'
    }
    url = host + '/api/v1/order'
    res = requests.delete(url, data=params, headers=headers)
    return res.text

# 查询订单
# symbol 必填, orderId 或 origClientOrderId 必填
def get_order(symbol, order_id=None, origClientOrderId=None):
    params = {
        'symbol': symbol,
        'orderId': order_id,
        'origClientOrderId': origClientOrderId,
        'recvWindow': 5000,
        'timestamp': int(round(time.time()*1000))
    }
    params = {k: v for k, v in params.items() if v is not None}
    # 保持签名参数顺序与实际请求一致
    signature = sign(params, priKey)
    params['signature'] = signature
    headers = {
        'X-MBX-APIKEY': signer,
        'User-Agent': 'PythonApp/1.0'
    }
    url = host + '/api/v1/order'
    res = requests.get(url, params=params, headers=headers)
    return res.text

# 查询所有挂单
def get_open_orders(symbol=None):
    params = {
        'symbol': symbol,
        'recvWindow': 5000,
        'timestamp': int(round(time.time()*1000))
    }
    params = {k: v for k, v in params.items() if v is not None}
    # 保持签名参数顺序与实际请求一致
    query_string = '&'.join([f"{k}={params[k]}" for k in params])
    signature = hmac.new(priKey.encode(), query_string.encode(), hashlib.sha256).hexdigest()
    params['signature'] = signature
    headers = {
        'X-MBX-APIKEY': signer,
        'User-Agent': 'PythonApp/1.0'
    }
    url = host + '/api/v1/openOrders'
    res = requests.get(url, params=params, headers=headers)
    return res.text

def get_latest_price_spot(symbol):
    url = host + '/api/v1/ticker/price'
    params = {'symbol': symbol}
    res = requests.get(url, params=params)
    try:
        data = res.json()
        return round(float(data.get('price', 0)), 6)
    except Exception as e:
        logging.error("Error parsing spot price response: %s", e)
        return None


#     # 下单 LIMIT
#     order_id, status = place_order('CDLUSDT', 'BUY', quantity='200', price='0.04')
#     logger.info(f"Placed order with ID: {order_id}, Status: {status}")
#     order_id2, status2 = place_order('CDLUSDT', 'BUY', quantity='200', price='0.03')
#     logger.info(f"Placed order with ID: {order_id2}, Status: {status2}")
#     # 查询订单
#     orders = get_open_orders()
#     logger.info(f"Open Orders: {orders}")
#     # 撤销订单
#     cancel_order('CDLUSDT', order_id=order_id)
#     cancel_order('CDLUSDT', order_id=order_id2)

