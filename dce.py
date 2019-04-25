# -*- coding:utf-8 -*-
# 大商所
import os
import time
import datetime
import re
import zipfile
import shutil
import requests
import pymongo
import settings

import pandas as pd

from threading import Thread
from queue import Queue, Empty
from lxml import etree
from log import Logger

log = Logger('logs/dce.log')


class CrawlData(Thread):
    """数据爬取类"""
    def __init__(self, q):
        super(CrawlData, self).__init__()
        self.q = q
        # url
        self.url = settings.API['dce']
        # form-data
        self.form = {
            "memberDealPosiQuotes.trade_type": "0",
            "contract.contract_id": "all",
            "year": "2019",
            "month": "0",
            "day": "1",
            "batchExportFlag": "batch",
        }
        # 重试次数
        self.retry = 3

    def run(self):
        # 创建下载文件目录
        self.make_dir()
        # 获取最后更新的一天日期
        last_date = self.get_last_date()
        today = datetime.datetime.today()
        while last_date <= today:
            # 排除周六日情况
            if last_date.isoweekday() != 6 and last_date.isoweekday() != 7:
                self.form.update({
                    'year': str(last_date.year),
                    'month': str(last_date.month - 1),
                    'day': str(last_date.day),
                })
                # 多次重试防止连接失败
                timeout = 0
                while timeout < self.retry:
                    try:
                        # log.logger.debug('正在爬取 %s' % last_date)
                        response = requests.post(self.url, self.form)
                        break
                    except Exception as e:
                        log.logger.warning('爬取超时 %s' % last_date)
                        timeout += 1
                # 如果超时了则停止继续爬取
                if timeout == self.retry:
                    log.logger.error('爬取严重超时，停止爬取 %s' % last_date)
                    break
                # 如果返回数据大小大于800的，说明当天是有数据的
                if response.status_code == 200 and len(response.content) > 800:
                    # 保存的文件名
                    file_name = '%s%s' % (last_date.strftime('%Y%m%d'), '_DCE_DPL.zip')
                    # 保存的文件路径
                    file_path = os.path.join(settings.TEMP_DOWNLOAD_DIR, file_name)
                    with open(file_path, 'wb') as f:
                        f.write(response.content)
                    # 把文件路径放进队列，让数据处理线程处理
                    self.q.put(file_path)
            # 对时间进行加1天
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
            data = collection.find_one({'exchange': 'dce'}, sort=[('date', -1)])
            # 郑商所期货大户持仓数据最早时间是2005年5月9日
            if data:
                date_list.append(data['date'])
            else:
                date_list.append(datetime.datetime(settings.DCE_TIME[0], settings.DCE_TIME[1], settings.DCE_TIME[2]))
        # 返回最小的那天
        return min(date_list)

    def make_dir(self):
        """创建下载目录"""
        if not os.path.isdir(settings.TEMP_DOWNLOAD_DIR):
            os.makedirs(settings.TEMP_DOWNLOAD_DIR)


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
                file_path = self.q.get(timeout=1)
                # log.logger.debug('正在处理 %s' % file_path)
                self.parse_data(file_path)
                self.q.task_done()
            except Empty:
                pass
            except Exception as e:
                log.logger.error('数据处理线程出错, 文件地址：%s，错误信息：%s' % (file_path, e), exc_info=True)
                self.q.task_done()
    
    def parse_data(self, file_path):
        """处理数据"""
        # 解压数据
        extract_path = self.extract_file(file_path)
        # 遍历目录读取文件
        file_paths = [os.path.join(extract_path, i) for i in os.listdir(extract_path)]
        for path in file_paths:
            self.parse2(path)
        # 删除解压目录及所有文件
        shutil.rmtree(extract_path)

    def get_data(self, path):
        """获取文件数据"""
        # 由于可能使用utf-8读取文件会失败，因此另外使用gbk读取文件
        try:
            with open(path, 'r', encoding='utf-8') as f:
                # 读取文件所有数据
                data = f.readlines()
                # 把数字中的 , 删除，并把空格换行符删除，把制表符替换成 ,
                data = [re.sub(r'\t+', ',', i.replace(',', '').strip()) for i in data]
        except UnicodeDecodeError:
            with open(path, 'r', encoding='gbk') as f:
                # 读取文件所有数据
                data = f.readlines()
                # 把数字中的 , 删除，并把空格换行符删除，把制表符替换成 ,
                data = [re.sub(r' +', ',', i.replace(',', '').strip()) for i in data]
        except Exception:
            log.logger.error('读取文件失败 %s' % path, exc_info=True)
        # 删除没有数据的行，并且不需要第一行
        data = [i for i in data if i][1:]
        return data
    
    def get_contract_goods(self, path):
        """从地址中提取合约信息和时间"""
        file_name = os.path.split(path)[-1]
        date, contract = re.findall(r'(\d+)_([a-zA-Z]+\d+)', file_name)[0]
        date = datetime.datetime.strptime(date, '%Y%m%d')
        goods = re.match(r'[a-zA-Z]+', contract).group()
        return contract, goods, date

    def parse2(self, path):
        """处理文件数据"""
        # 数据临时保存列表
        temp_data = []
        # 数据类型
        data_type = ''
        # 由于有某几天的数据没有合约信息，需要从文件名获取
        contract, goods, date = self.get_contract_goods(path)
        # 获取文件数据
        data = self.get_data(path)
        # 对数据进行处理
        for item in data:
            if '合约代码' in item:
                contract, date = re.findall(r'合约代码：(.+),Date：(.+)', item)[0]
                goods = re.findall(r'[a-zA-Z]+', contract)[0]
                # 把字符串的时间转成时间格式
                date = datetime.datetime.strptime(date, '%Y-%m-%d')
                continue
            if '期货公司会员' in item or '会员类别' in item:
                continue
            if '名次' in item:
                temp_data = []
                if '成交量' in item:
                    data_type = 'trade'
                elif '持买单量' in item:
                    data_type = 'long'
                elif '持卖单量' in item:
                    data_type = 'short'
                continue
            # 如果是总计并且有数据，则对数据进行整体处理
            if '总计' in item and temp_data:
                df = pd.DataFrame(temp_data, columns=['rank', 'name', 'volume', 'volumeDiff'], dtype='int32')
                data_dict = {
                    'exchange': 'dce',
                    'goods': goods,
                    'symbol': 'dce_%s' % contract.lower(),
                    'date': date,
                    'volume': df['volume'].sum(),
                    'volumeDiff': df['volumeDiff'].sum(),
                    'data': df.to_dict('record'),
                }
                if data_type == 'trade':
                    self.trade_q.put(data_dict)
                elif data_type == 'long':
                    self.long_q.put(data_dict)
                elif data_type == 'short':
                    self.short_q.put(data_dict)
            # 把数据处理并放到数据列表里
            if len(item.split(',')) == 4:
                temp_data.append(item.split(','))

    def extract_file(self, file_path):
        """解压文件"""
        # 压缩包的文件名
        file_name = os.path.split(file_path)[-1]
        # 加压路径
        extract_path = os.path.join(settings.TEMP_EXTRACT_DIR, file_name)
        # 打开压缩文件
        z = zipfile.ZipFile(file_path, 'r')
        # 加压文件
        z.extractall(extract_path)
        # 关闭压缩文件
        z.close()
        # 删除压缩文件
        os.remove(file_path)
        # 返回解压路径
        return extract_path


class InsertData(Thread):
    """插入数据类"""
    def __init__(self, q, collection_name):
        super(InsertData, self).__init__()
        self.q = q
        self.collection_name = collection_name
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
            # log.logger.debug('正在插入 %s %s %s' % (self.collection_name, symbol, date))
            self.collection.replace_one({'date': date, 'symbol': symbol}, data, True)
        except Exception as e:
            log.logger.error('插入数据出错 %s' % data, exc_info=True)


def main():
    start = time.time()
    log.logger.info('-'*50+' start '+'-'*50)
    log.logger.info('开始大商所大户持仓爬虫程序')
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
    parser3 = ParseData(q, trade_q, short_q, long_q)
    parser1.start()
    parser2.start()
    parser3.start()
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
    parser3.join()
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
    log.logger.info('大商所大户持仓数据已更新完成')
    log.logger.info('共耗时%ss' % (time.time()-start))
    log.logger.info('-'*50+'  end  '+'-'*50)


if __name__ == "__main__":
    main()
