# 国内四大期货交易所大户持仓排名数据爬取

本项目基于国内四大交易所（中金所、上期所、郑商所、大商所）的网站获取数据，其中包括持仓排名、持买单量排名和持卖单量排名，由于每个交易所能获取的数据开始时间各不相同，所以每个交易所数据开始时间需要视情况而定

### 使用说明

* 环境:

1. 本项目基于python3开发，python2未经测试
2. Linux 系统测试没问题，但Windows 系统在大商所插入数据到数据库时发生类型错误，目前具体原因未找到
3. 使用Mongodb4.0数据库，其他版本Mongodb未测试

* 下载代码:

```shell
git clone git@github.com:Su-yj/FutureHolderRank.git
或者直接到 https://github.com/Su-yj/FutureHolderRank 下载zip文件自行解压
```

* 安装依赖:

```shell
pip install -r requirements.txt
```

* 配置及说明:

```python
# settings.py 为项目配置文件

# Mongodb 数据库配置
MONGODB = {
    # 地址
    'HOST': '127.0.0.1',
    # 端口
    'PORT': 27017,
    # 用户名，如果没有填空字符串
    'USERNAME': '',
    # 密码，如果没有填空字符串
    'PASSWORD': '',
    # 验证的数据库，如果没有填空字符串
    'AUTHSOURCE': '',
    # 加密方式，如果没有填空字符串
    'AUTHMECHANISM': '',
}

# 如果需要对数据库以及表名自定义，可修改以下部分设置
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

# 由于大商所的数据需要下载处理，若硬盘空间不够大，可自行修改下载及解压目录，确保硬盘至少有100M以上
# 文件处理完后会自动删除
# 临时下载目录
TEMP_DOWNLOAD_DIR = './temp/download'
# 临时解压目录
TEMP_EXTRACT_DIR = './temp/extract'
```

* 启动

```shell
# 全部爬取
python run.py

# 或单独爬取某个交易所数据
python shfe.py
```

### 注意说明

* 各交易所数据起始时间

| 交易所 | 时间 |
| :-: | :-: |
| 中金所(cffex) | 2010-04-16 |
| 上期所(shfe) | 2002-01-07 |
| 郑商所(czce) | 2005-05-09 |
| 大商所(dce) | 2004-01-05 |

* 大商所开始时间问题

大商所数据可获取到更早，但在网站上只能选取到2005年，项目默认开始时间是2004-01-05，如需要获取更早的数据，可修改 `settings.py` 中的 `DCE_TIME = (2004, 1, 5)` ，其中第一个为年份，第二个是月份，第三个是日期

* 各个交易所排名数量

中金所、上期所、郑商所最多只能获取20个排名信息，而大商所最多会有100多个信息，本项目把把能获取得到的信息都保存到数据库中

* 关于郑商所合约

由于郑商所合约代码时间均使用3个数字，并不能明显区分哪个年份，因此本项目未统一，把郑商所的合约代码修改为4个数字表示，如`TA705`， 2007年的合约代码修改为`TA0705`，2017年的合约代码修改为`TA1705`

* 数据结构说明

```json
{
    "_id" : ObjectId("5cc079f48233d1f7318495e7"),
    "exchange" : "shfe", \\ 交易所
    "goods" : "rb", \\ 品种代码
    "symbol" : "shfe_rb1910", \\ 交易所合约代码，均使用该格式(exchange_symbol)，并且都是小写
    "date" : ISODate("2019-04-24T00:00:00.000+0000"), \\ 时间
    "volume" : 654596, \\ 当天统计的总量
    "volumeDiff" : 12280, \\ 当天统计的变化量
    \\ 排名信息
    "data" : [
        {
            "rank" : 1, \\ 排名
            "name" : "银河期货", \\ 名称
            "volume" : 75896, \\ 持仓量 或 持买单量 或 持卖单量 (具体根据表名区分)
            "volumeDiff" : 15445 \\ 变化量
        },
        {
            "rank" : 2,
            "name" : "永安期货",
            "volume" : 63142,
            "volumeDiff" : 4949
        },
        {
            "rank" : 3,
            "name" : "方正中期",
            "volume" : 57157,
            "volumeDiff" : -1453
        },
        {
            "rank" : 4,
            "name" : "中信期货",
            "volume" : 50625,
            "volumeDiff" : 1158
        },
        {
            "rank" : 5,
            "name" : "国泰君安",
            "volume" : 37298,
            "volumeDiff" : 411
        },
        {
            "rank" : 6,
            "name" : "海通期货",
            "volume" : 34823,
            "volumeDiff" : -915
        },
        {
            "rank" : 7,
            "name" : "东海期货",
            "volume" : 34453,
            "volumeDiff" : 652
        },
        {
            "rank" : 8,
            "name" : "华泰期货",
            "volume" : 33696,
            "volumeDiff" : -10860
        },
        {
            "rank" : 9,
            "name" : "申万期货",
            "volume" : 32396,
            "volumeDiff" : 5198
        },
        {
            "rank" : 10,
            "name" : "东证期货",
            "volume" : 26833,
            "volumeDiff" : -1961
        },
        {
            "rank" : 11,
            "name" : "浙商期货",
            "volume" : 23817,
            "volumeDiff" : -3596
        },
        {
            "rank" : 12,
            "name" : "中财期货",
            "volume" : 23416,
            "volumeDiff" : 3812
        },
        {
            "rank" : 13,
            "name" : "光大期货",
            "volume" : 22121,
            "volumeDiff" : 803
        },
        {
            "rank" : 14,
            "name" : "中信建投",
            "volume" : 20912,
            "volumeDiff" : -1421
        },
        {
            "rank" : 15,
            "name" : "中辉期货",
            "volume" : 20571,
            "volumeDiff" : 5449
        },
        {
            "rank" : 16,
            "name" : "南华期货",
            "volume" : 20423,
            "volumeDiff" : 332
        },
        {
            "rank" : 17,
            "name" : "国投安信",
            "volume" : 20389,
            "volumeDiff" : -1767
        },
        {
            "rank" : 18,
            "name" : "徽商期货",
            "volume" : 19520,
            "volumeDiff" : -1043
        },
        {
            "rank" : 19,
            "name" : "信达期货",
            "volume" : 18756,
            "volumeDiff" : 258
        },
        {
            "rank" : 20,
            "name" : "兴证期货",
            "volume" : 18352,
            "volumeDiff" : -3171
        }
    ]
}
```
