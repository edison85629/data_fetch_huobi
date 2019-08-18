import websocket
import time
import logging.config
import yaml
import glob
import configparser
from huobi.sql_connection import SqlConnection
from huobi.MessageFormat import Message


class FetchClient:

    def __init__(self, instance_id='',
                 exchange: str='',
                 symbol: str='btcusdt',
                 period: int=30,
                 size: int=240
                 ):

        self.sql_client = SqlConnection(symbol=symbol, period=period)
        self.sql_client.create_tables()
        records_time = self.sql_client.get_time_range()
        records_begin = records_time.begin
        records_end = records_time.end
        __iniFilePath = glob.glob('../config/exchanges_config.ini')
        cfg = configparser.ConfigParser()
        cfg.read(__iniFilePath, encoding='utf-8')
        self.url = cfg.get(exchange, 'url')
        if records_begin is None:
            self.start_time = 1501174800
        else:
            self.start_time = records_end+1
        self.to_time = int(time.time())
        self.req_ws = None
        self.func = None
        self.symbol = symbol
        self.period = period
        self.instance_id = instance_id
        self.time_unit = 1*60*self.period*size
        self.req_count = 0
        self.sub_dict = {}
        self.totaldata = {}
        self.records_indexes = []
        _start = self.to_time - self.time_unit + 1
        _to = self.to_time
        _step = self.time_unit
        while _to > self.start_time:
            self.records_indexes.append((max(_start, self.start_time), _to))
            _to = _to - _step
            _start = _start - _step
        print(len(self.records_indexes))
        self.fetch_count = 0
        self.records_index = 0

        with open(r'../config/log_config.yaml') as f:
            log_config = yaml.safe_load(f)
            logging.config.dictConfig(log_config)
        self.logger = logging.getLogger('%s' % (self.symbol + '_'+str(self.period)+"min"))


    # 接收消息
    def on_message(self, ws, message):
        self.req_ws = ws
        msg = Message(logger=self.logger)
        # 初始化传入的sub 获取 req函数
        if self.func == 'req':
            if self.fetch_count < len(self.records_indexes):
                if hasattr(msg, self.func):
                    func = getattr(msg, self.func)
                    # 调用传入的sub或者是req函数
                    data = '{"req":"market.%s.kline.%smin","id":%s,"from":%s,"to":%s}' % (self.symbol,
                                                                                          self.period,
                                                                                          self.fetch_count,
                                                                                          self.records_indexes[self.fetch_count][0],
                                                                                          self.records_indexes[self.fetch_count][1])
                    func(ws, message, data=data, totalcount=self.req_count)
                    self.req_count = 1
                    data = msg.get_req_msg()

                    if data:
                        # self.logger.info(data)  # logger测试
                        if data[0] == self.fetch_count:
                            self.fetch_count += 1
                            self.req_count = 0
                        self.totaldata[data[0]] = data[1]
                        self.records_index = data[0]
            if self.records_index == len(self.records_indexes)-1:
                if self.totaldata:
                    save_msg = self.sql_client.save_req_records(data=self.get_data())
                    self.logger.info("save message"+":"+save_msg)
                    self.totaldata = None
                sub = "market.%s.kline.%s" % (self.symbol, str(self.period) + 'min')
                if sub not in self.sub_dict.keys():
                    sub_id = str(len(self.sub_dict))
                    sub_data = '{"sub":"%s","id":%s}' % (sub, sub_id)
                    self.sub_dict[sub] = sub_id
                    msg.sub_padding(ws, message, data=sub_data, totalcount=0)
                else:
                    msg.sub_padding(ws, message, data='', totalcount=1)
                values = msg.get_sub_msg()
                if values:
                    self.sql_client.update_records(values=values)
        elif self.func == 'sub':
            pass  # todo
        else:
            pass
    # 发生错误
    def on_error(self, ws, error):
        # TODO 填写因发送异常，连接断开的处理操作
        self.logger.error(ws.on_error.__dict__)
        # print(ws.on_error.__dict__)

    # 连接断开
    def on_close(self, ws):
        # TODO 填写连接断开的处理操作
        self.logger.info("### closed ###")
        # print("### closed ###")

    # 建立连接
    def on_open(self, ws):
        self.logger.info('Connection Successful')
        # print("success")

    def start_fetch(self, func: str='req'):
        # 调用的函数
        self.func = func
        websocket.enableTrace(True)
        ws1 = websocket.WebSocketApp(self.url,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)
        ws1.on_open = self.on_open
        ws1.run_forever()

    def get_ws(self):
        return self.req_ws

    def get_data(self):
        return self.totaldata

    def get_sub_dict(self):
        return self.sub_dict

