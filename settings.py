# MongoDB配置
MONGODB = {
    'HOST': '127.0.0.1',
    'PORT': 27017,
    'USERNAME': '',
    'PASSWORD': '',
    'AUTHSOURCE': '',
    'AUTHMECHANISM': '',
}

# 数据库名
DB_NAME = 'future_rank'
# 表名
COLLECTION_NAMES = {
    # 成交量排名
    "TRADE": 'future_trade_rank',
    # 持买单量排名
    "LONG": 'future_long_rank',
    # 持卖单量排名
    "SHORT": 'future_short_rank',
}

# 各交易所排名接口
API = {
    # 上海期货交易所
    # 网页地址: http://www.shfe.com.cn/statements/dataview.html?paramid=pm&paramdate=20190412
    'shfe': 'http://www.shfe.com.cn/data/dailydata/kx/pm%s.dat',
    # 郑州商品交易所
    # 网页地址: http://www.czce.com.cn/cn/jysj/ccpm/H770304index_1.htm
    'czce': 'http://app.czce.com.cn/cms/cmsface/czce/newcms/calendarnewAll.jsp',
    # 大连商品交易所
    # 网页地址: http://www.dce.com.cn/dalianshangpin/xqsj/tjsj26/rtj/rcjccpm/index.html
    'dce': 'http://www.dce.com.cn/publicweb/quotesdata/exportMemberDealPosiQuotesBatchData.html',
    # 中国金融期货交易所
    # 网页地址: http://www.cffex.com.cn/ccpm/
    'cffex': 'http://www.cffex.com.cn/sj/ccpm/{year_month}/{day}/{goods}.xml',
}

# 临时下载目录
TEMP_DOWNLOAD_DIR = './temp/download'
# 临时解压目录
TEMP_EXTRACT_DIR = './temp/extract'

DCE_TIME = (2004, 1, 5)