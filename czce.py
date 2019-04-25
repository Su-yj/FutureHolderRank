# -*- encode:utf-8 -*-
# 郑商所
import time
import datetime
import re
import requests
import pymongo
import settings

import pandas as pd

from threading import Thread
from queue import Queue, Empty
from lxml import etree
from log import Logger

log = Logger('logs/czce.log')


class CrawlData(Thread):
    """数据爬取类"""
    def __init__(self, q):
        super(CrawlData, self).__init__()
        # 数据队列
        self.q = q
        # url
        self.url = settings.API['czce']
        # 请求form-data
        self.form = {
            'channelCode': '',
            'pubDate': '2005-05-09',
            'curpath': '/cn/jysj/ccpm/H770304index_1.htm',
            'curpath1': '',
            'radio': 'future',
            'sub': '查询'
        }
        # 重试次数
        self.retry = 3

    def run(self):
        # 查询三个表中日期最小的那天
        last_date = self.get_last_date()
        today = datetime.datetime.today()
        # 如果日期小于今天
        while last_date <= today:
            # 如果不是周六日的情况，则往下进行
            if last_date.isoweekday() != 6 and last_date.isoweekday() != 7:
                # 对form-data进行更新
                pubDate = last_date.strftime('%Y-%m-%d')
                self.form.update({'pubDate': pubDate})
                # 超时次数
                time_out = 0
                while time_out < self.retry:
                    try:
                        # log.logger.debug('正在爬取 %s' % pubDate)
                        response = requests.post(self.url, data=self.form)
                        break
                    except Exception as e:
                        log.logger.warning('获取数据超时 %s, 错误: %s' % (pubDate, e))
                        time_out += 1
                # 如果重试多次仍然失败，则停止爬取
                if time_out == self.retry:
                    log.logger.error('获取数据严重超时，停止爬取 %s' % pubDate)
                    break
                # 如果是能获取得到数据的
                if response.status_code == 200:
                    self.q.put((response.content.decode('utf-8'), pubDate))
            # 加1天
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
            data = collection.find_one({'exchange': 'czce'}, sort=[('date', -1)])
            # 郑商所期货大户持仓数据最早时间是2005年5月9日
            if data:
                date_list.append(data['date'])
            else:
                date_list.append(datetime.datetime(2005, 5, 9))
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
                html, pubDate = self.q.get(timeout=1)
                self.parse_data(html, pubDate)
                self.q.task_done()
            except Empty:
                pass
            except Exception as e:
                log.logger.error('数据处理线程出错,日期：%s, 错误信息: %s' % (pubDate, e), exc_info=True)
                self.q.task_done()

    def parse_data(self, html, pubDate):
        """数据处理"""
        # 把数据转成xml
        html = etree.HTML(html)
        # 把字符串的时间转成时间格式
        pubDate = datetime.datetime.strptime(pubDate, '%Y-%m-%d')
        # 由于不同时间段的数据形式不同，因此分类处理
        if pubDate <= datetime.datetime(2010, 8, 24):
            self.method1(html)
        elif pubDate <= datetime.datetime(2017, 12, 27):
            self.method2(html)
        else:
            self.method3(html)

    def method1(self, html):
        """处理方法1"""
        # 获取所有表格标题
        titles = html.xpath("//div[@align='left']/b/font/text()")
        # 逐个遍历
        for title in titles:
            data = []
            trade_data = []
            long_data = []
            short_data = []
            # 把所有空格去除掉
            title_str = ''.join(title.split())
            # 如果有品种在，说明该条是品种的总情况，而不是合约的情况，跳过
            if '品种' in title_str:
                continue
            # 提取合约和日期
            contract, date = re.findall(r'合约代码(.+)日期:(\d+)', title_str)[0]
            goods, num = re.findall(r'([a-zA-Z]+)(\d+)', contract)[0]
            # 对郑商所的合约年份进行修改，添加一位数字区分
            num = self.full_year(num, int(date[:4]))
            contract = '%s%s' % (goods, num)

            # 对表格数据进行处理
            table = title.getparent().getparent().getparent().getnext()
            # 获取每一行数据，由于第一行是每列的名字，最后一行没有数据，因此跳过
            # 并且合约合计一行的tr是有问题的，合计一行都写成 </tr>...</tr>
            rows = table.xpath(".//tr")[1: -1]
            for row in rows:
                infos = [''.join(''.join(i.strip()).split(',')) for i in row.xpath("./td/text()")]
                data.append(infos)

            # 合计部分
            total_list = [''.join(''.join(i.strip()).split(',')) for i in table.xpath('./td/text()')]
            data.append(total_list)
            # 日期
            date = datetime.datetime.strptime(date, '%Y%m%d')
            # 合约
            symbol = 'czce_%s' % contract.lower()
            # 构造信息
            info_dict = {
                'exchange': 'czce',
                'goods': goods,
                'symbol': symbol,
                'date': date,
            }
            if data:
                self.parse2(data, info_dict)
                                
    def method2(self, html):
        # 定位每一行
        rows = html.xpath("//table[@class='table']//tr")
        # 数据存储
        data = []
        for row in rows:
            # 如果是表信息行，则每次把数据清空
            if row.xpath('.//b/text()'):
                title = ''.join(row.xpath('.//b/text()')[0].split())
                # 如果是品种的信息，则跳过
                if '品种' in title:
                    # 记录开关，用于判断是否需要往下记录处理数据
                    switch = False
                    continue
                else:
                    switch = True
                # 如果有数据，把数据转换并存储到队列中
                if data:
                    self.parse2(data, info_dict)

                contract, date = re.findall(r'合约：(.+?)日期：(.+)', title)[0]
                goods, num = re.findall(r'([a-zA-Z]+)(\d+)', contract)[0]
                # 对郑商所的合约年份进行修改，添加一位数字区分
                num = self.full_year(num, int(date[:4]))
                contract = '%s%s' % (goods, num)
                symbol = 'czce_%s' % contract.lower()
                # 构造信息
                info_dict = {
                    'exchange': 'czce',
                    'goods': goods,
                    'symbol': symbol,
                    'date': datetime.datetime.strptime(date, '%Y-%m-%d'),
                }
                # 清空之前的数据
                data = []
                continue
            # 如果不是合约信息，则跳过
            if not switch:
                continue
            # 如果是表的列明信息，则跳过
            if row.xpath('./@class'):
                continue
            # 把数字中的 , 去除
            infos = [''.join(i.split(',')) for i in row.xpath('./td/text()')]
            # 把空格去掉
            infos = [''.join(i.strip()) for i in infos]
            # 把该条数据添加到列表中
            data.append(infos)
        if data:
            self.parse2(data, info_dict)

    def method3(self, html):
        # 定位每一行
        rows = html.xpath('//table//tr')
        data = []
        for row in rows:
            if row.xpath('.//b/text()'):
                title = ''.join(row.xpath('.//b/text()')[0].split())
                # 如果是品种的信息，则跳过
                if '品种' in title:
                    # 记录开关，用于判断是否需要往下记录处理数据
                    switch = False
                    continue
                else:
                    switch = True
                # 如果有数据，把数据转换并存储到队列中
                if data:
                    self.parse2(data, info_dict)

                contract, date = re.findall(r'合约：(.+?)日期：(.+)', title)[0]
                goods, num = re.findall(r'([a-zA-Z]+)(\d+)', contract)[0]
                # 对郑商所的合约年份进行修改，添加一位数字区分
                num = self.full_year(num, int(date[:4]))
                contract = '%s%s' % (goods, num)
                symbol = 'czce_%s' % contract.lower()
                # 构造信息
                info_dict = {
                    'exchange': 'czce',
                    'goods': goods,
                    'symbol': symbol,
                    'date': datetime.datetime.strptime(date, '%Y-%m-%d'),
                }
                # 清空之前的数据
                data = []
                continue
            # 如果不是合约信息，则跳过
            if not switch:
                continue
            # 把数字中的 , 去除
            infos = [''.join(i.split(',')) for i in row.xpath('./td/text()')]
            # 把空格去掉
            infos = [''.join(i.strip()) for i in infos]
            # 跳过列名的那行
            if '名次' in infos:
                continue
            # 把该条数据添加到列表中
            data.append(infos)
        if data:
            self.parse2(data, info_dict)
    
    def parse2(self, data, info_dict):
        """处理方法2的数据"""
        # 列名
        columns = ['rank', 'name1', 'trade', 'tradeDiff', 'name2', 'long', 'longDiff', 'name3', 'short', 'shortDiff']
        # 转成DataFrame类型，其中跳过最后一行合计部分
        df = pd.DataFrame(data[:-1], columns=columns)
        trade_df = df[['rank', 'name1', 'trade', 'tradeDiff']]
        long_df = df[['rank', 'name2', 'long', 'longDiff']]
        short_df = df[['rank', 'name3', 'short', 'shortDiff']]
        # 对列名重命名
        trade_df.rename(columns={'rank': 'rank', 'name1': 'name', 'trade': 'volume', 'tradeDiff': 'volumeDiff'}, inplace=True)
        long_df.rename(columns={'rank': 'rank', 'name2': 'name', 'long': 'volume', 'longDiff': 'volumeDiff'}, inplace=True)
        short_df.rename(columns={'rank': 'rank', 'name3': 'name', 'short': 'volume', 'shortDiff': 'volumeDiff'}, inplace=True)            
        # 去掉空的值
        trade_df = trade_df[trade_df['name'] != '-']
        long_df = long_df[long_df['name'] != '-']
        short_df = short_df[short_df['name'] != '-']
        # 转换成int32类型
        trade_df[['rank', 'volume', 'volumeDiff']] = trade_df[['rank', 'volume', 'volumeDiff']].astype('int32')
        long_df[['rank', 'volume', 'volumeDiff']] = long_df[['rank', 'volume', 'volumeDiff']].astype('int32')
        short_df[['rank', 'volume', 'volumeDiff']] = short_df[['rank', 'volume', 'volumeDiff']].astype('int32')
        # 合计信息
        total_list = data[-1]
        # 由于有些数据开头有空格，有些没有，所有需要把total_list个数是9的统一索引0位置增加一个
        if len(total_list) == 9:
            total_list.insert(0, '')
        # 2006-01-16 的合计数据没有空格，因此只有6个，也需要把数据补齐成10个
        if len(total_list) == 6:
            total_list.insert(0, '')
            total_list.insert(0, '')
            total_list.insert(4, '')
            total_list.insert(7, '')
        # 构造存储的数据
        trade_dict = info_dict.copy()
        trade_dict.update({
            'volume': int(total_list[2]),
            'volumeDiff': int(total_list[3]),
            'data': trade_df.to_dict('records'),
        })
        long_dict = info_dict.copy()
        long_dict.update({
            'volume': int(total_list[5]),
            'volumeDiff': int(total_list[6]),
            'data': long_df.to_dict('records'),
        })
        short_dict = info_dict.copy()
        short_dict.update({
            'volume': int(total_list[8]),
            'volumeDiff': int(total_list[9]),
            'data': short_df.to_dict('records'),
        })
        # 把数据放到队列中
        if trade_dict['data']:
            self.trade_q.put(trade_dict)
        if long_dict['data']:
            self.long_q.put(long_dict)
        if short_dict['data']:
            self.short_q.put(short_dict)

    def full_year(self, number, year):
        """
        大商所年份补齐
        :param number: 商品期货的交割日期
        :return : 
        """
        year1 = int(number[0])
        for i in range(10):
            year_i = 2000 + i*10 + year1
            # 顺序生成的某个年份和实际的年份相差不超过3年
            if abs(year_i - year) <= 3:
                return str(year_i)[-2:] + number[-2:]


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
            log.logger.error('插入数据出错 %s' % data, exc_info=True)


def main():
    start = time.time()
    log.logger.info('-'*50+' start '+'-'*50)
    log.logger.info('开始郑商所大户持仓爬虫程序')
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
    log.logger.info('郑商所大户持仓数据已更新完成')
    log.logger.info('共耗时%ss' % (time.time()-start))
    log.logger.info('-'*50+'  end  '+'-'*50)


if __name__ == "__main__":
    main()    
