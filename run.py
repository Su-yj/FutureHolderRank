import multiprocessing
import time
import cffex
import czce
import dce
import shfe

from log import Logger

log = Logger('logs/run.log')


def main():
    start = time.time()
    log.logger.info('-'*50+' start '+'-'*50)
    log.logger.info('开始期货大户持仓爬虫程序')
    process_list = [
        multiprocessing.Process(target=cffex.main),
        multiprocessing.Process(target=czce.main),
        multiprocessing.Process(target=dce.main),
        multiprocessing.Process(target=shfe.main),
    ]
    for p in process_list:
        p.start()
    for p in process_list:
        p.join()
    log.logger.info('期货大户持仓数据已更新完成')
    log.logger.info('-'*50+'  end  '+'-'*50)
    log.logger.info('共耗时%ss' % (time.time()-start))


if __name__ == '__main__':
    main()
