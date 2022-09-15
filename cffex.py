# -*- coding:utf-8 -*-
# 中金所
# IF 2010-04-16
# IC 2015-04-16
# IH 2015-04-16
import os
import time
import datetime
import re
import requests
import pymongo
import xmltodict
import settings

from threading import Thread
from queue import Queue, Empty
from log import Logger

log = Logger('logs/cffex.log')


class CrawlData(Thread):
    """数据爬取类"""
    def __init__(self, q):
        super(CrawlData, self).__init__()
        self.url = settings.API['cffex']
        self.q = q
        self.goods = ['IF', 'IH', 'IC']
        self.retry = 3

    def run(self):
        # 出错异常停止信号
        exit_signal = False
        # 查询最后一天的时间
        last_date = self.get_last_date()
        today = datetime.datetime.today()
        while last_date <= today:
            # 排除周六日情况
            if last_date.isoweekday() != 6 and last_date.isoweekday() != 7:
                year_month = last_date.strftime('%Y%m')
                day = last_date.strftime('%d')
                # 分别对三个品种进行查询
                for goods in self.goods:
                    format_dict = {
                        'year_month': year_month,
                        'day': day,
                        'goods': goods,
                    }
                    url = self.url.format(**format_dict)
                    timeout = 0
                    while timeout < self.retry:
                        try:
                            # log.logger.debug('正在爬取 %s' % url)
                            response = requests.get(url)
                            break
                        except Exception as e:
                            log.logger.warning('获取数据超时 %s, 错误：%s' % (url, e))
                            timeout += 1
                    # 如果获取超时了，则退出本次爬取，并且通知线程退出
                    if timeout == self.retry:
                        log.logger.error('爬取严重超时 %s' % url)
                        exit_signal = True
                        break
                    # 如果返回的数据大小大于3000，说明是有数据的
                    if len(response.content) > 3000 and response.status_code == 200:
                        # 对内容进行转码
                        xml = response.content.decode('utf-8')
                        # 把xml转成字典格式
                        xml_dict = xmltodict.parse(xml)
                        # 放进队列
                        self.q.put(xml_dict)
                # 如果有退出信号，则退出整个线程
                if exit_signal:
                    break
            last_date += datetime.timedelta(days=1)

    def get_last_date(self):
        """获取最后一天的日期"""
        # 连接数据库
        if not settings.MONGODB['AUTHMECHANISM']:
            client = pymongo.MongoClient(
                    host=settings.MONGODB['HOST'], 
                    port=settings.MONGODB['PORT'], 
                    username=settings.MONGODB['USERNAME'], 
                    password=settings.MONGODB['PASSWORD'], 
                    authSource=settings.MONGODB['AUTHSOURCE']
                )
        else:
            client = pymongo.MongoClient(
                    host=settings.MONGODB['HOST'], 
                    port=settings.MONGODB['PORT'], 
                    username=settings.MONGODB['USERNAME'], 
                    password=settings.MONGODB['PASSWORD'], 
                    authSource=settings.MONGODB['AUTHSOURCE'], 
                    authMechanism=settings.MONGODB['AUTHMECHANISM']
                )
        db = client[settings.DB_NAME]
        date_list = []
        # 对每个表进行查询最后一条的日期
        for collection_name in settings.COLLECTION_NAMES.values():
            collection = db[collection_name]
            data = collection.find_one({'exchange': 'cffex'}, sort=[('date', -1)])
            # 郑商所期货大户持仓数据最早时间是2005年5月9日
            if data:
                date_list.append(data['date'])
            else:
                date_list.append(datetime.datetime(2010, 4, 16))
        # 返回最小的那天
        return min(date_list)


class ParseData(Thread):
    """数据处理类"""
    def __init__(self, q, trade_q, short_q, long_q):
        super(ParseData, self).__init__()
        # 数据队列
        self.q = q
        self.trade_q = trade_q
        self.short_q = short_q
        self.long_q = long_q

    def run(self):
        global EXIT_FLAG_PARSER
        while not EXIT_FLAG_PARSER:
            try:
                xml_dict = self.q.get(timeout=1)
                self.parse_data(xml_dict)
                self.q.task_done()
            except Empty:
                pass
            except Exception as e:
                log.logger.error('数据处理线程出错, 时间：%s，错误信息：%s' % (xml_dict['positionRank']['data'][0]['tradingday'], e), exc_info=True)
                self.q.task_done()
    
    def parse_data(self, xml_dict):
        """处理数据"""
        # 时间
        date = xml_dict['positionRank']['data'][0]['tradingday']
        date = datetime.datetime.strptime(date, '%Y%m%d')
        # log.logger.debug('正在处理 %s' % date)
        # 数据
        trade_data = {}
        long_data = {}
        short_data = {}
        data = xml_dict['positionRank']['data']
        for item in data:
            # 当日的变化量
            volumeDiff = int(item.get('varVolume', item.get('varvolume')))
            # 合约代码
            instrumentId = item.get('instrumentId', item.get('instrumentid')).strip()
            temp_dict = {
                'rank': int(item['rank']),
                'name': item['shortname'],
                'volume': int(item['volume']),
                'volumeDiff': volumeDiff,
            }
            if item['@Value'] == '0':
                temp = trade_data.setdefault(instrumentId, [])
                temp.append(temp_dict)
            elif item['@Value'] == '1':
                temp = long_data.setdefault(instrumentId, [])
                temp.append(temp_dict)
            elif item['@Value'] == '2':
                temp = short_data.setdefault(instrumentId, [])
                temp.append(temp_dict)
        if trade_data:
            self.parse2(trade_data, date, self.trade_q)
        if long_data:
            self.parse2(long_data, date, self.long_q)
        if short_data:
            self.parse2(short_data, date, self.short_q)

    def parse2(self, data_dict, date, q):
        """转成需要的格式并放进队列"""
        for contract, data in data_dict.items():
            volume = sum([i['volume'] for i in data])
            volumeDiff = sum([i['volumeDiff'] for i in data])
            goods = re.findall(r'[a-zA-Z]+', contract)[0]
            doc = {
                'exchange': 'cffex',
                'goods': goods,
                'symbol': 'cffex_%s' % contract.lower(),
                'date': date,
                'volume': volume,
                'volumeDiff': volumeDiff,
                'data': data,
            }
            q.put(doc)
        

class InsertData(Thread):
    """插入数据类"""
    def __init__(self, q, collection_name):
        super(InsertData, self).__init__()
        self.q = q
        if not settings.MONGODB['AUTHMECHANISM']:
            self.client = pymongo.MongoClient(
                    host=settings.MONGODB['HOST'], 
                    port=settings.MONGODB['PORT'], 
                    username=settings.MONGODB['USERNAME'], 
                    password=settings.MONGODB['PASSWORD'], 
                    authSource=settings.MONGODB['AUTHSOURCE']
                )
        else:
            self.client = pymongo.MongoClient(
                    host=settings.MONGODB['HOST'], 
                    port=settings.MONGODB['PORT'], 
                    username=settings.MONGODB['USERNAME'], 
                    password=settings.MONGODB['PASSWORD'], 
                    authSource=settings.MONGODB['AUTHSOURCE'], 
                    authMechanism=settings.MONGODB['AUTHMECHANISM']
                )
        self.db = self.client[settings.DB_NAME]
        self.collection = self.db[collection_name]
        self.collection.create_index([('date', 1), ('symbol', 1)])
    
    def run(self):
        global EXIT_FLAG_INSERTER
        while not EXIT_FLAG_INSERTER:
            # 采用非堵塞获取队列数据
            try:
                data = self.q.get(timeout=1)
                self.insert_data(data)
                self.q.task_done()
            except Empty:
                pass
            except Exception as e:
                log.logger.error('插入数据线程出错, 时间：%s，错误内容：%s' % (data['date'], e), exc_info=True)
                self.q.task_done()

    def insert_data(self, data):
        date = data['date']
        symbol = data['symbol']
        try:
            # log.logger.debug('正在插入 %s %s' % (symbol, date))
            self.collection.replace_one({'date': date, 'symbol': symbol}, data, True)
        except Exception as e:
            log.logger.error('插入数据出错 %s' % data)


def main():
    start = time.time()
    log.logger.info('-'*50+' start '+'-'*50)
    log.logger.info('开始上期所大户持仓爬虫程序')
    # 数据处理线程退出信号
    global EXIT_FLAG_PARSER
    EXIT_FLAG_PARSER = False
    # 数据插入线程退出信号
    global EXIT_FLAG_INSERTER
    EXIT_FLAG_INSERTER = False
    # 爬虫数据队列
    q = Queue()
    # 成交量排名数据队列
    trade_q = Queue()
    # 持卖单量排名数据队列
    short_q = Queue()
    # 持买单量排名数据队列
    long_q = Queue()
    # 开启爬虫
    crawler = CrawlData(q)
    crawler.start()
    # 开启数据处理
    parser = ParseData(q, trade_q, short_q, long_q)
    parser.start()
    # 开启数据插入
    insert1 = InsertData(trade_q, settings.COLLECTION_NAMES['TRADE'])
    insert2 = InsertData(short_q, settings.COLLECTION_NAMES['SHORT'])
    insert3 = InsertData(long_q, settings.COLLECTION_NAMES['LONG'])
    insert1.start()
    insert2.start()
    insert3.start()
    # 等待爬虫线程结束
    crawler.join()
    # 等待数据处理完成
    q.join()
    # 通知数据处理线程可以结束了
    EXIT_FLAG_PARSER = True
    # 等待处理线程结束
    parser.join()
    # 等待其他数据队列完成
    trade_q.join()
    short_q.join()
    long_q.join()
    # 通知数据插入线程已经没有其他数据了
    EXIT_FLAG_INSERTER = True
    # 等待数据插入线程结束
    insert1.join()
    insert2.join()
    insert3.join()
    log.logger.info('上期所大户持仓数据已更新完成')
    log.logger.info('共耗时%ss' % (time.time()-start))
    log.logger.info('-'*50+'  end  '+'-'*50)


if __name__ == "__main__":
    main()
    
