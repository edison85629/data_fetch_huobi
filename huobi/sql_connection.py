import pymysql
import glob
import configparser
import time
import warnings
from pandas import Series, DataFrame


class SqlConnection:
    def __init__(self, symbol: str='btcusdt', period: int=60):
        warnings.filterwarnings('error', category=pymysql.Warning)
        self.symbol = symbol
        self.period = period
        # 从配置文件中获取初始化参数
        __iniFilePath = glob.glob("mysql_params.ini")
        cfg = configparser.ConfigParser()
        cfg.read(__iniFilePath, encoding='utf-8')
        self.host = cfg.get('mysql', 'host')
        self.port = cfg.getint('mysql', 'port')
        self.user = cfg.get('mysql','user')
        self.password = cfg.get('mysql', 'password')
        self.db= 'huobi_' + symbol
        self.table_name = symbol+'_'+str(period)+'min'
        self.con = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.db)
        self.cursor = self.con.cursor()

    def close_connect(self):
        self.con.close()

    def get_connect(self):
        return self.con

    def get_cursor(self):
        return self.get_connect().cursor()

    def create_tables(self, table_name: str=None):
        if table_name is None:
            if self.table_name == '':
                print("table_name is wrong")
                raise RuntimeError
            table_name = self.table_name

        try:
            effect_row = self.cursor.execute('''
              CREATE TABLE IF NOT EXISTS %s(
              `id` INT UNSIGNED AUTO_INCREMENT,
              `timestamp` INT(13) NOT NULL DEFAULT '0',
              `open` FLOAT NOT NULL, 
              `close` FLOAT NOT NULL,
              `high` FLOAT NOT NULL,
              `low` FLOAT NOT NULL,
              `vol` FLOAT NOT NULL,
              `amount` FLOAT NOT NULL,
              `count` INT(10) NOT NULL,
              PRIMARY KEY (`id`)
              )ENGINE=InnoDB DEFAULT CHARSET=utf8
            ''' % table_name)
        except pymysql.Warning as w:
            sqlWarning = 'Warning: %s' % str(w)
            print(sqlWarning)
        else:
            print("create table %s, success" % table_name)

    def records_insert(self,table_name:str=None, values={}):
        if table_name is None:
            if self.table_name == '':
                print("table_name is wrong")
                raise RuntimeError
            table_name = self.table_name

        if not self.check_table_exist(table_name=table_name):
            self.create_tables(table_name=table_name)
        result = self.update_kline(table_name=table_name,values=values)
        if not result:
            result = self.insert_kline(table_name=table_name, values=values)
        return result

    def check_table_exist(self, table_name=None):
        if table_name is None:
            table_name = self.table_name
        effect_row = self.cursor.execute('''
            SHOW TABLES LIKE '%s' 
        ''' % table_name)
        result = self.cursor.fetchone()
        return result

    def get_time_range(self, table_name: str=None):
        if table_name is None:
            table_name = self.table_name
        # if self.check_table_exist():
        if self.check_table_exist() is None:
            self.create_tables()
        try:
            effect_row = self.cursor.execute('''
              select MAX(timestamp),MIN(timestamp) FROM %s;
            ''' % table_name)
            result = self.cursor.fetchone()
            result_s = Series({'begin':result[1], 'end': result[0]})
        except pymysql.Warning as w:
            print(str(w))
        return result_s

    def get_records(self,
                    start_time: int=0,
                    to_time: int=int(time.time()),
                    period: int=0,
                    symbol: str=None
                    ):
        if symbol is None:
            if self.symbol is None:
                raise RuntimeError
            symbol = self.symbol
        if period == 0:
            if self.period == 0:
                raise RuntimeError
            period = self.period

        table_name = symbol+'_'+str(period)+'min'
        if start_time is 0:
            start_time = self.show_time_range(table_name=table_name)[1]
        effect_row = self.cursor.execute('''
            select * from %s WHERE TIMESTAMP BETWEEN %s AND %s
        ''' % (table_name, start_time, to_time))
        results = self.cursor.fetchall()
        records = []
        for result in results:
            record = {'ts': result[1],
                      'open': result[2],
                      'close': result[3],
                      'high': result[4],
                      'low': result[5],
                      'vol': result[6],
                      'amount': result[7],
                      'count': result[8]}
            records.append(record)
        return DataFrame(records)

    def save_req_records(self, data):
        if data:
            keys = list(data.keys())
            list.sort(keys, reverse=True)
            for i in keys:
                for j in data[i]:
                    self.records_insert(values=j)

    def update_records(self, table_name: str=None, values: dict={}):
        if table_name is None:
            if self.table_name == '':
                print("table_name is wrong")
                raise RuntimeError
            table_name = self.table_name

        try:
            effect_row = self.update_kline(table_name=table_name, values=values)
            if not effect_row:
                effect_row = self.insert_kline(table_name=table_name, values=values)
            self.con.commit()
        except pymysql.Warning as w:
            print('Warning: %s' % str(w))
            self.con.rollback()
        except pymysql.Error as e:
            print('Error: %s' % str(e))
            self.con.rollback()
        else:
            return "success"

    def insert_kline(self, table_name, values):
        self.con.begin()
        effect_row = 0
        try:
            effect_row = self.cursor.execute('''
                INSERT INTO %s (`timestamp`, `open`, `close`, `high`, `low`, `vol`, `amount`, `count`) VALUES (
                %d,%f,%f,%f,%f,%f,%f,%d
                )
            ''' % (table_name, values['id'], values['open'], values['close'], values['high'], values['low'], values['vol'], values['amount'], values['count']))
            self.con.commit()
        except pymysql.Warning as w:
            print('Warning: %s' % str(w))
            self.con.rollback()
        except pymysql.Error as e:
            print('Error: %s' % str(e))
            self.con.rollback()
        else:
            pass

        return effect_row

    def update_kline(self, table_name, values: dict={}):
        effect_row = 0
        try:
            update_sql = '''
                UPDATE %s SET `open`=%f,`close`=%f,`high`=%f,`low`=%f,`vol`=%f,`amount`=%f,`count`=%d WHERE `timestamp`=%d;
            ''' % (table_name, values['open'], values['close'], values['high'], values['low'], values['vol'], values['amount'], values['count'], values['id'])

            effect_row = self.cursor.execute(update_sql)
            self.con.commit()
        except pymysql.Warning as w:
            print('Warning: %s' % str(w))
            self.con.rollback()
        except pymysql.Error as e:
            print('Error: %s' % str(e))
            self.con.rollback()
        else:
            pass

        return effect_row

