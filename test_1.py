import requests
import time
import pymongo
from jsonpath import jsonpath
headers = {
    'User-Agent':
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'
}
url = 'https://www.alphavantage.co/query?'
function1 = 'function=TIME_SERIES_DAILY'
function2 = 'function=TIME_SERIES_INTRADAY'
symbols = ['MSFT', 'AMD', 'AMZN', 'BA',
           'GOOG', 'GS', 'HAL',
           'IBM', 'KSS', 'XOM']
datatype = 'datatype=json'
apiKey = 'apikey=ET84USM4CSJRYCLX'
outputsize = 'full'
def DictProcess(dic, collectionName):
    client = pymongo.MongoClient(host='localhost', port=27017)
    mydb = client['db']
    mycol = mydb[collectionName]
    for key in dic.keys():
        latestTime = mycol.find_one({"_id": key})
        if latestTime is None:
            open = jsonpath(dic, '$..' + str(key) + '..')[1]
            high = jsonpath(dic, '$..' + str(key) + '..')[2]
            low = jsonpath(dic, '$..' + str(key) + '..')[3]
            close = jsonpath(dic, '$..' + str(key) + '..')[4]
            volume = jsonpath(dic, '$..' + str(key) + '..')[5]
            record = {'_id': key, 'open': open, 'high': high, 'low': low, 'close': close, 'volume': volume}
            x = mycol.insert_one(record)
            print(record)
            print(collectionName)
            print(x)
        else:
            break
    client.close()
    return

def CollectYearly(symbol):
    collectionName_year = symbol + '_year'
    req_yearly = url + '&' + function1 + '&' + 'symbol=' + symbol + '&' + apiKey + '&' \
                 + datatype + '&' + 'outputsize=' + outputsize
    request_yearly = requests.get(req_yearly, headers=headers)
    data_yearly = request_yearly.json()
    yearly_data = jsonpath(data_yearly, '$..[Time Series (Daily),(0)]')[0]

    DictProcess(yearly_data, collectionName_year)
    return

def CollectDaily(symbol):
    req_daily = url + '&' + function2 + '&' + 'symbol=' + symbol + '&' + 'interval=1min' + '&' \
                + apiKey + '&' + datatype + '&' + 'outputsize=' + outputsize
    request_daily = requests.get(req_daily, headers=headers)
    data_daily = request_daily.json()
    daily_data = jsonpath(data_daily, '$..[Time Series (1min),(0)]')[0]
    return daily_data


if __name__ == '__main__':
    client = pymongo.MongoClient(host='localhost', port=27017)
    timeStamp = dict.fromkeys(symbols)
    for symbol in symbols:
        CollectYearly(symbol)
        time.sleep(11)

    count = 0
    while count < 2:
        for symbol in symbols:
            collectionName_daily = symbol + '_daily'
            data = CollectDaily(symbol)
            DictProcess(data, collectionName_daily)
            time.sleep(11)

        count = count + 1
