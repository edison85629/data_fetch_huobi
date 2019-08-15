import json
import configparser
import hmac
import base64
import hashlib
import glob

from datetime import datetime
from urllib import parse


class Auth:

    def __init__(self, auth_name: str=None):
        __iniFilePath = glob.glob("authcfg.ini")
        cfg = configparser.ConfigParser()
        cfg.read(__iniFilePath, encoding='utf-8')
        self.accessKey = cfg.get(auth_name, 'accessKey')
        self.secretKey = cfg.get(auth_name, 'secretKey')
        self._host = cfg.get('ws','_host')
        self.path = cfg.get('ws','path')

    def _auth(self, auth):
        # 获取需要签名的信息
        authentication_data = auth[1]
        # 获取 secretkey
        _accessKeySecret = auth[0]
        # 计算签名Signature

        authentication_data['Signature'] = self._sign(authentication_data, _accessKeySecret)
        print(authentication_data)
        return json.dumps(authentication_data)

    # 计算鉴权签名
    def _sign(self, param=None, _access_key_secret=None):
        # 签名参数:
        params = {}
        params['SignatureMethod'] = param.get('SignatureMethod') if type(param.get('SignatureMethod')) == type(
            'a') else '' if param.get('SignatureMethod') else ''
        params['SignatureVersion'] = param.get('SignatureVersion') if type(param.get('SignatureVersion')) == type(
            'a') else '' if param.get('SignatureVersion') else ''
        params['AccessKeyId'] = param.get('AccessKeyId') if type(param.get('AccessKeyId')) == type(
            'a') else '' if param.get('AccessKeyId') else ''
        params['Timestamp'] = param.get('Timestamp') if type(param.get('Timestamp')) == type('a') else '' if param.get(
            'Timestamp') else ''
        print(params)
        # 对参数进行排序:
        keys = sorted(params.keys())
        # 加入&
        qs = '&'.join(['%s=%s' % (key, self._encode(params[key])) for key in keys])
        # 请求方法，域名，路径，参数 后加入`\n`
        payload = '%s\n%s\n%s\n%s' % ('GET', self._host, self.path, qs)
        dig = hmac.new(_access_key_secret, msg=payload.encode('utf-8'), digestmod=hashlib.sha256).digest()
        # 进行base64编码
        return base64.b64encode(dig).decode()

    # 获取UTC时间
    @staticmethod
    def _utc(self):
        return datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')

    # 进行编码
    @staticmethod
    def _encode(self, s):
        # return urllib.pathname2url(s)
        return parse.quote(s, safe='')

    # 发送的authData数据
    def get_authdata(self):
        authdata = [
            self.secretKey.encode('utf-8'),
            {
                "op": "auth",
                # "cid": "",  # 选填；Client 请求唯一 ID
                "AccessKeyId": self.accessKey,
                "SignatureMethod": "HmacSHA256",
                "SignatureVersion": "2",
                "Timestamp": self._utc()
            }
        ]
        return authdata

    def send_auth_data(self):
        #发送信息
        _auth = self._auth(self.get_authdata())

