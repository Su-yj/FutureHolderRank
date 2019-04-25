import os
import sys
import logging

from logging import FileHandler
from logging import handlers


class MPFileLogHandler(logging.Handler):
    """重构logging的Handler类，兼容多进程"""
    def __init__(self, file_path):
        self._fd = os.open(file_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND)
        logging.Handler.__init__(self)

    def emit(self, record):
        msg = "{}\n".format(self.format(record))
        os.write(self._fd, msg.encode('utf-8'))



class Logger(object):
    level_relations = {
        'debug':logging.DEBUG,
        'info':logging.INFO,
        'warning':logging.WARNING,
        'error':logging.ERROR,
        'crit':logging.CRITICAL
    }#日志级别关系映射

    def __init__(self, filename, filepath=None, level='debug', when='D', backCount=3, fmt='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'):
        # 设置日志默认路径
        if not filepath:
            filepath=os.path.join(sys.path[0], filename)

        self.logger = logging.getLogger(filename)
        if not self.logger.handlers:
            # 设置日志格式
            format_str = logging.Formatter(fmt)
            # 设置日志级别
            self.logger.setLevel(self.level_relations.get(level))
            # 往屏幕上输出
            sh = logging.StreamHandler()
            # 设置屏幕上显示的格式
            sh.setFormatter(format_str)
            # 设置等级
            sh.setLevel(self.level_relations.get('debug'))
            # 多进程处理
            wh = MPFileLogHandler(filepath)
            # 设置输出格式
            wh.setFormatter(format_str)
            wh.setLevel(self.level_relations.get('info'))
            # 往文件里写入
            # 指定间隔时间自动生成文件的处理器
            # th = handlers.TimedRotatingFileHandler(filename=filename,when=when,backupCount=backCount,encoding='utf-8')
            # 实例化TimedRotatingFileHandler
            # interval是时间间隔，backupCount是备份文件的个数，如果超过这个个数，就会自动删除，when是间隔的时间单位，单位有以下几种：
            # S 秒
            # M 分
            # H 小时、
            # D 天、
            # W 每星期（interval==0时代表星期一）
            # midnight 每天凌晨
            # 设置文件里写入的格式
            # th.setFormatter(format_str)
            # 把对象加到logger里
            self.logger.addHandler(sh)
            self.logger.addHandler(wh)
            # self.logger.addHandler(th)


# log = Logger('check_future_missing_data.log',level='debug')
