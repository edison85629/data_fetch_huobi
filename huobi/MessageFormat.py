import json
import zlib
import time


class Message:
    def __init__(self):
        self.req_msg = {}
        self.sub_msg = None

    # 发送sub请求
    def sub_padding(self, ws, message, data=None, totalcount=None):
        # 接收服务器的数据  进行解压操作
        ws_result = str(zlib.decompressobj(31).decompress(message), encoding="utf-8")
        # TODO 自定义打印服务器传回的信息
        print('接收服务器数据为 ：%s' % ws_result)

        if totalcount < 1:
            print('向服务器发送订阅 :%s' % data)
            ws.send(data)

        # 维持ping pong
        if 'ping' in ws_result:
            ping_id = json.loads(ws_result).get('ping')
            pong_data = '{"pong": %d}' % ping_id
            ws.send(pong_data)
            print('向服务器发送pong :%s' % pong_data)

        if 'tick' in ws_result:
            data = json.loads(ws_result).get('tick')
            self.sub_msg = data

    # 发送req请求
    def req(self, ws, message, data, totalcount):
        # 接收服务器的数据  进行解压操作
        ws_result = str(zlib.decompressobj(31).decompress(message), encoding="utf-8")
        # TODO 自定义打印服务器传回的信息
        print('服务器响应数据:%s' % ws_result)
        if totalcount < 1:
            print('向服务器发送数据:%s' % data)
            ws.send(data)
        # 维持ping pong
        result_js = json.loads(ws_result)
        ping_id = result_js.get('ping')
        if 'ping' in ws_result:
            pong_data = '{"pong": %d}' % ping_id
            ws.send(pong_data)
            print('向服务器发送pong :%s' % pong_data)
        if 'id' in ws_result and 'status' in ws_result:
            index = result_js.get('id')
            data = result_js.get('data')
            if data:
                self.req_msg = (index, data)
            else:
                self.req_msg = None

    def get_req_msg(self):
        return self.req_msg

    def get_sub_msg(self):
        return self.sub_msg

