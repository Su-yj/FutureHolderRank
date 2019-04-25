# -*- encode:utf-8 -*-
# 上期所
import time
import re
import datetime
import requests
import pymongo
import settings
import threading

import pandas as pd

from  queue import Queue, Empty
from log import Logger

log = Logger('logs/shfe.log')


class CrawlData(threading.Thread):
    """爬取数据类"""
    def __init__(self, q):
        super(CrawlData, self).__init__()
        # 数据存储队列
        self.q = q
        # url
        self.url = settings.API['shfe']
        # 请求头
        self.headers = {
            'Host': 'www.shfe.com.cn',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36',
            'Referer': 'http://www.shfe.com.cn/statements/dataview.html?paramid=delaymarket_all',
        }
        # 连接失败重试次数
        self.retry = 3

    def run(self):
        # 获取数据库最新的一条时间
        last_time = self.get_last_time()
        last_time = last_time.date()
        today = datetime.date.today()
        while last_time <= today:
            # 排除周六日情况
            if last_time.isoweekday != 6 and last_time.isoweekday != 7:
                # 请求url
                url = self.url % last_time.strftime('%Y%m%d')
                # 超时次数
                time_out = 0
                while time_out < self.retry:
                    try:
                        # log.logger.debug('开始爬取 %s' % url)
                        response = requests.get(url)
                    except Exception as e:
                        log.logger.warning('连接失败, 错误内容: %s, url: %s' % (e, url), exc_info=True)
                        time_out += 1
                        continue
                    # 如果是404，说明当天没有数据
                    if response.status_code == 404:
                        break
                    try:
                        # 把数据转换成json
                        data = response.json()
                    except Exception as e:
                        log.logger.error('数据转成json失败, 错误内容: %s, url: %s' % (e, url), exc_info=True)
                        time_out += 1
                        continue
                    # 如果数据没有report_date，则添加一个时间
                    data.setdefault('report_date', last_time.strftime('%Y%m%d'))
                    # 如果是有数据的
                    if data['o_cursor']:
                        # 把数据放进队列里
                        self.q.put(data)
                    break
                # 如果超时次数等于设定的重试次数，说明没有成功爬取到数据
                if time_out == self.retry:
                    log.logger.error('获取数据失败 %s' % url)
                    # 停止爬取
                    break
            # 日期加1天
            last_time += datetime.timedelta(days=1)
                
    def get_last_time(self):
        """查询数据库最后一条的时间"""
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
        # 查询每个表中最小的日期
        for collection_name in settings.COLLECTION_NAMES.values():
            collection = db[collection_name]
            data = collection.find_one({'exchange': 'shfe'}, sort=[('date', -1)])
            # 上期所期货最早时间是2002年1月7日
            if data:
                date_list.append(data['date'])
            else:
                date_list.append(datetime.datetime(2002, 1, 7))
        return min(date_list)


class ParseData(threading.Thread):
    """处理数据类"""
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
            # 采用非堵塞获取队列数据
            try:
                data = self.q.get(timeout=1)
                self.parse_data(data)
                self.q.task_done()
            except Empty:
                pass
            except Exception as e:
                log.logger.error('数据处理线程出错，时间：%s，错误信息：%s' % (data['report_date'], e), exc_info=True)
                self.q.task_done()

    def parse_data(self, data):
        """处理数据"""
        # 日期
        date = datetime.datetime.strptime(data['report_date'], '%Y%m%d')
        # log.logger.debug('正在处理 %s' % date)
        # 把数据转换成DataFrame
        df = pd.DataFrame(data['o_cursor'])
        # 如果排名是-1或0，是品种的一个总情况，忽略跳过
        df = df[(df['RANK']!=-1) & (df['RANK']!=0)]
        # 把数据中的空格去掉
        df['INSTRUMENTID'] = df['INSTRUMENTID'].str.strip()
        df['PARTICIPANTABBR1'] = df['PARTICIPANTABBR1'].str.strip()
        df['PARTICIPANTABBR2'] = df['PARTICIPANTABBR2'].str.strip()
        df['PARTICIPANTABBR3'] = df['PARTICIPANTABBR3'].str.strip()
        # 数据类型转换
        df['RANK'] = df['RANK'].astype('int32')
        # 按照合约进行分组
        for contract, son_df in df.groupby(df['INSTRUMENTID']):
            # RANK=999的是对合约的小结
            new_df = son_df[son_df['RANK']!=999]
            for i in range(1, 4):
                # 如果合约小结没有数据，则跳过
                if int(son_df.loc[son_df['RANK']==999, 'CJ%s' % i] == ''):
                    continue
                # 构造新的DataFrame
                temp_df = pd.DataFrame({'rank': new_df['RANK'], 'name': new_df['PARTICIPANTABBR%s' % i], 'volume': new_df['CJ%s' % i], 'volumeDiff': new_df['CJ%s_CHG' % i]}).reset_index(drop=True).sort_values('rank')
                # 把空的数据删除，并转换数据类型
                temp_df = temp_df[temp_df['name'] != '']
                temp_df[['volume', 'volumeDiff']] = temp_df[['volume', 'volumeDiff']].astype('int32')
                # 整理全部数据
                temp_dict = {
                    'exchange': 'shfe',
                    'goods': re.match(r'[^\d]+', contract).group(),
                    'symbol': 'shfe_%s' % contract.lower(),
                    'date': date,
                    'volume': int(son_df.loc[son_df['RANK']==999, 'CJ%s' % i]),
                    'volumeDiff': int(son_df.loc[son_df['RANK']==999, 'CJ%s_CHG' % i]),
                    'data': temp_df.to_dict('records'),
                }
                # 把数据放进队列
                if i == 1:
                    self.trade_q.put(temp_dict)
                elif i == 3:
                    self.short_q.put(temp_dict)
                elif i == 2:
                    self.long_q.put(temp_dict)


class InsertData(threading.Thread):
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
        self.client.close()

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
    parser1 = ParseData(q, trade_q, short_q, long_q)
    parser2 = ParseData(q, trade_q, short_q, long_q)
    parser1.start()
    parser2.start()
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
    parser1.join()
    parser2.join()
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
