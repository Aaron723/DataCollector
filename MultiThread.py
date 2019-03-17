import requests
import time
import pymongo
import json
from jsonpath import jsonpath
from threading import Thread
from queue import Queue

headers = {
    'User-Agent':
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebK'
        'it/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'
}
base_url_1 = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symb' \
             'ol={}&outputsize=compact&interval=1min&apikey=ET84USM4CSJRYCLX&datatype=json'
base_url_2 = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&ou' \
             'tputsize=compact&symbol={}&apikey=ET84USM4CSJRYCLX&datatype=json'
function1 = 'function=TIME_SERIES_DAILY'
function2 = 'function=TIME_SERIES_INTRADAY'
symbols = ['MSFT', 'AMD', 'AMZN', 'BA',
           'GOOG', 'GS', 'HAL',
           'IBM', 'KSS', 'XOM']
datatype = 'datatype=json'
apiKey = 'apikey=ET84USM4CSJRYCLX'
outputsize = 'full'


def mongodb_to_json(collectionName):
    client = pymongo.MongoClient(host='localhost', port=27017)
    db = client['db']  # database_name
    collection = db[collectionName]  # collection_name
    filename = './dataset/' + collectionName + '.json'
    with open(filename, 'w') as f:
        data = list()
        for item in collection.find():
            data.append(item)
        f.write(json.dumps(data))


# crawler class

class CrawlInfo(Thread):
    def __init__(self, url_queue, json_queue):
        Thread.__init__(self)
        self.url_queue = url_queue
        self.json_queue = json_queue

    def run(self):
        while self.url_queue.empty() == False:
            url = self.url_queue.get()
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                self.json_queue.put(response.json())
                print('Got data from:', url)

            time.sleep(62)


class ParseHis(Thread):
    def __init__(self, json_queue):
        Thread.__init__(self)
        self.json_queue = json_queue

    def run(self):
        while self.json_queue.empty() == False:
            client = pymongo.MongoClient(host='localhost', port=27017)
            mydb = client['db']
            data = self.json_queue.get()
            yearly_data = jsonpath(data, '$..[Time Series (Daily),(0)]')[0]
            stockname = jsonpath(data, "$.[Meta Data]..")[2]
            mycol = mydb[stockname + '_his']
            for key in yearly_data.keys():
                latestTime = mycol.find_one({"_id": key})
                if latestTime is None:
                    open = jsonpath(yearly_data, '$..' + str(key) + '..')[1]
                    high = jsonpath(yearly_data, '$..' + str(key) + '..')[2]
                    low = jsonpath(yearly_data, '$..' + str(key) + '..')[3]
                    close = jsonpath(yearly_data, '$..' + str(key) + '..')[4]
                    volume = jsonpath(yearly_data, '$..' + str(key) + '..')[5]
                    record = {'_id': key, 'stockname': stockname, 'open': open, 'high': high, 'low': low,
                              'close': close,
                              'volume': volume}
                    x = mycol.insert_one(record)
                    print(x)
                    print(record)
                else:
                    break


class ParseIntro(Thread):
    def __init__(self, json_queue):
        Thread.__init__(self)
        self.json_queue = json_queue

    def run(self):
        client = pymongo.MongoClient(host='localhost', port=27017)
        mydb = client['db']
        while self.json_queue.empty() == False:
            data = self.json_queue.get()
            daily_data = jsonpath(data, '$..[Time Series (1min),(0)]')[0]
            stockname = jsonpath(data, "$.[Meta Data]..")[2]
            mycol = mydb[stockname + '_intro']
            for key in daily_data.keys():
                latestTime = mycol.find_one({"_id": key})
                if latestTime is None:
                    open = jsonpath(daily_data, '$..' + str(key) + '..')[1]
                    high = jsonpath(daily_data, '$..' + str(key) + '..')[2]
                    low = jsonpath(daily_data, '$..' + str(key) + '..')[3]
                    close = jsonpath(daily_data, '$..' + str(key) + '..')[4]
                    volume = jsonpath(daily_data, '$..' + str(key) + '..')[5]
                    record = {'_id': key, 'stockname': stockname, 'open': open, 'high': high, 'low': low,
                              'close': close,
                              'volume': volume}
                    x = mycol.insert_one(record)
                    print(x)
                    print(record)
                else:
                    break


if __name__ == '__main__':
    # url queue
    url_introday_queue = Queue()
    url_daily_queue = Queue()
    # json queue
    intro_queue = Queue()
    his_queue = Queue()
    # get whole url in a queue
    for symbol in symbols:
        new_url_2 = base_url_2.format(symbol)
        url_daily_queue.put(new_url_2)
    crawls = []
    # get history data
    for i in range(0, 5):
        crawl1 = CrawlInfo(url_daily_queue, his_queue)
        crawl1.start()
        crawls.append(crawl1)
    for crawl in crawls:
        crawl.join()

    for j in range(0, 10):
        parse2 = ParseHis(his_queue)
        parse2.start()

        # real time data
    while True:
        cls = []
        pa = []
        for symbol in symbols:
            new_url_1 = base_url_1.format(symbol)
            url_introday_queue.put(new_url_1)

        for i in range(0, 5):
            crawl2 = CrawlInfo(url_introday_queue, intro_queue)
            cls.append(crawl2)
            crawl2.start()
        for crawl in cls:
            crawl.join()

        for j in range(0, 10):
            parse1 = ParseIntro(intro_queue)
            parse1.start()
            pa.append(parse1)
        for pi in pa:
            pi.join()
        for SN in symbols:
            collectionName = SN + "_intro"
            mongodb_to_json(collectionName)
            cn = SN + '_his'
            mongodb_to_json(cn)
