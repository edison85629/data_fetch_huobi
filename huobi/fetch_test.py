from huobi.FetchClient import FetchClient


# 定义url地址
url = "wss://api.huobi.br.com/ws"
# 定义操作交易队
symbol = 'btcusdt'
# 定义周期值 单位为min
period = 60
# 开始时间
fc = FetchClient(url=url, symbol=symbol, period=period)
fc.start_fetch()
