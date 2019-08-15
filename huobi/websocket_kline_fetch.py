import json
import zlib
import time
import websocket
import glob
import configparser
from urllib import parse
import datetime
from com.huobi.time_tools import TimeTools
from com.huobi.sql_connection import SqlConnection


# 从配置文件中获取初始化参数
__iniFilePath = glob.glob("websocket_fetch_kline_params.ini")
cfg = configparser.ConfigParser()
cfg.read(__iniFilePath, encoding='utf-8')

protocol = cfg.get('ws', 'protocol')
_host = cfg.get('ws', '_host')
path = cfg.get('ws', 'path')

kline_period = cfg.get('kline', 'kline_period')
start_time = cfg.get('kline', 'start_time')
to_time = cfg.get('kline', 'to_time')

# 组建获取请求的url
url = protocol + _host + path


class Message:

    # 发送sub请求
    def sub_padding(self, ws, message, data=None, totalcount=None):
        # 接收服务器的数据  进行解压操作
        ws_result = str(zlib.decompressobj(31).decompress(message), encoding="utf-8")
        js_result = json.loads(ws_result)
        ss = (int(round(time.time() * 1000)) - js_result['ts'])
        # TODO 自定义打印服务器传回的信息
        print('接收服务器数据为 ：%s' % ws_result)

        if totalcount < 1:
            for k in data:
                print('向服务器发送订阅 :%s' % k)
                ws.send(json.dumps(k))

        # 维持ping pong
        ping_id = json.loads(ws_result).get('ts')
        if 'ping' in ws_result:
            pong_data = '{"op":"pong","ts": %s}' % ping_id
            ws.send(pong_data)
            print('向服务器发送pong :%s' % pong_data)

    # 发送req请求
    def req(self, ws, message, data, totalcount):
        # 接收服务器的数据  进行解压操作
        ws_result = str(zlib.decompressobj(31).decompress(message), encoding="utf-8")
        # TODO 自定义打印服务器传回的信息
        print('服务器响应数据%s' % ws_result)
        # print(time.time())
        if totalcount < 1:
            print('向服务器发送数据1%s' % data)
            ws.send(json.dumps(data))
        # 维持ping pong
        ping_id = json.loads(ws_result).get('ts')
        if 'ping' in ws_result:
            pong_data = '{"op":"pong","ts":%s}' % ping_id
            ws.send(pong_data)
            print('向服务器发送pong :%s' % pong_data)


# websocket
class websockClient():

    def __init__(self, instance_id=''):
        self.req_ws = None
        self.instance_id = instance_id
        self.count = 0
        self.totalcount = 0
        self.func = None


    # 接收消息
    def on_message(self, ws, message):
        self.req_ws = ws
        # TODO 接收消息自定义处理
        MSG = Message()
        # 初始化传入的sub 获取 req函数
        if self.func:
            hasattr(self, self.func)
            func = getattr(MSG, self.func)
            # 调用传入的sub_padding或者是req函数
            func(ws, message, data=self.data, totalcount=self.totalcount)
            self.totalcount += 1

    # 发生错误
    def on_error(self, ws, error):
        # TODO 填写因发送异常，连接断开的处理操作
        print(ws.on_error.__dict__)

    # 连接断开
    def on_close(self, ws):
        # TODO 填写连接断开的处理操作
        print("### closed ###")

    # 建立连接
    def on_open(self, ws):
        def run(*args):
            time.sleep(1)
        run()

    def start_websocket(self, func, data):
        # 调用的函数
        self.func = func
        # 发送服务器的数据
        self.data = data
        websocket.enableTrace(True)
        ws = websocket.WebSocketApp(url,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)
        ws.on_open = self.on_open
        ws.run_forever()

    def get_ws(self):
        return self.req_ws


# TODO sub订阅参数 可填写多个请求以数组形式添加
datasymbols = [{
    'req': 'market.btcusdt.kline.60min',
    'id': 'id1',
    'from': '1563893210',
    'to': '1564498010'}
]

#
# TODO req查询参数数组 accounts.list , orders.detail , orders.list
datareq = [
    {
        "op": "req",
        "topic": "accounts.list",
        "cid": 'sfdsfsfdsf'
    },
    {
        "op": "req",
        "topic": "orders.detail",
        'cid': '40sdfkajs',
        "order-id": '1543924'
    },
    {
        "op": "req",
        "topic": "orders.list",
        # "cid":'32sdfsawa',
        "account-id": 1698070,
        "symbol": 'eosusdt',
        "types": "buy-market,sell-market,buy-limit,sell-limit,buy-ioc,sell-ioc",
        # "start-date" :1563893210,
        # "end-date":1564498010,
        "states": "submitted,partial-filled,partial-canceled,filled",
        # "from": "1543875",
        # "direct": "next",
        # "size": '100'
    }
]

# TODO 发送req请求
# websockClient().start_websocket(func='req' ,data=datareq[2],authdata=authdata)
# websockClient().start_websocket(func='req',data=datareq[1],authdata=authdata)
# websockClient().start_websocket(func='req',data=datareq[0],authdata=authdata)


# TODO 发送sub 订阅或取消订阅
# websockClient().start_websocket(func='req', data=datareq[2], authdata=authdata)

