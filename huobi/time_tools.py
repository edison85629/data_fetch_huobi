import datetime
import time


class TimeTools:

    @staticmethod
    def str2timestamp(str_value):
        try:
            d = datetime.datetime.strptime(str_value, "%Y-%m-%d %H:%M:%S")
            t = d.timetuple()
            time_stamp = int(time.mktime(t))
            time_stamp = float(str(time_stamp) + str("%06d" % d.microsecond)) / 1000000
            return int(time_stamp)
        except ValueError as e:
            print(e)
            d = datetime.datetime.strptime(str_value, "%Y-%m-%d %H:%M:%S")
            t = d.timetuple()
            time_stamp = int(time.mktime(t))
            time_stamp = float(str(time_stamp) + str("%06d" % d.microsecond)) / 1000000
            return int(time_stamp)

    @staticmethod
    def timestamp_to_timestamp10(time_stamp):
        time_stamp = int(time_stamp * (10 ** (10 - len(str(time_stamp)))))
        return time_stamp

    @staticmethod
    def timestamp2str(timestamp):
        timestamp_int = int(timestamp)
        if len(str(timestamp_int)) > 10:
            timestamp_int = TimeTools.timestamp_to_timestamp10(timestamp_int)
        str_time = time.strftime("%Y-%m-%d_%H:%M:%S", time.localtime(timestamp_int))
        return str_time

    @staticmethod
    def timestamp2date(timestamp):
        pass  # TODO

    @staticmethod
    def get_now_time():
        timestamp = int(time.time())
        time_local = time.localtime(timestamp)
        now_time = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
        return now_time