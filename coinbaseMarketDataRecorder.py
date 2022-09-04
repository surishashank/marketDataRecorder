from datetime import datetime, timedelta, date
from momoRider.Exchanges.coinbase.client import CoinbaseExchangeAuth

import pandas as pd
import requests
import time
import os

API_URL = 'https://api.pro.coinbase.com/'


# https://docs.cloud.coinbase.com/exchange/reference/exchangerestapi_getproductcandles
# Data output in descending order i.e. oldest date first
# TODO: Add new data to daily data everyday and if date range data is needed it should be taken from the existing
#  daily/ 6hr/ 1hr etc data. This partial data should be stored in a temp folder
class CoinbaseAPI:
    def __init__(self, client=None):
        self.client = client if client else CoinbaseExchangeAuth()

    # Default granularity = daily candles, 60*60*24 = 86400
    # Can only request 300 candles per request
    @staticmethod
    def getAllHistoricalPrices(productId, granularity=86400):
        rows = []
        start = 300
        end = 0
        params = {'start': (datetime.utcnow() - timedelta(start)).isoformat(),
                  'end': (datetime.utcnow() - timedelta(end)).isoformat(),
                  'granularity': granularity}
        r = requests.get(API_URL + 'products/{0}/candles'.format(productId), params=params)

        while len(r.json()) > 1:
            params = {'start': (datetime.utcnow() - timedelta(start)).isoformat(),
                      'end': (datetime.utcnow() - timedelta(end)).isoformat(),
                      'granularity': granularity}
            r = requests.get(API_URL + 'products/{0}/candles'.format(productId), params=params)
            for line in r.json():
                row = [datetime.utcfromtimestamp(line[0]).strftime('%Y-%m-%d')] + line[1:]
                rows.append(row)
            time.sleep(.5)
            end = start
            start += 300
        CoinbaseAPI.writeCsv(rows[::-1], productId, granularity)

    # start = utc start time, e.g. datetime.date(2017, 12, 17)
    # end = utc end time, e.g. datetime.date(2017, 12, 17)
    # Default granularity = daily candles, 60*60*24 = 86400
    # Can only request 300 candles per request
    # TODO: api not returning values for startdate on first run but does on subsequent one..wtf.
    @staticmethod
    def getHistoricalPricesForDateRange(productId, start, end, granularity=86400):
        rows = []
        i = start
        j = i + timedelta(300)
        firstRun = True

        while (j - i).days >= 300:
            # Adding timedelta(1), since API returns data for starDate-endDate inclusive, i.e. to avoid dates overlap
            # from previous iteration.
            i = start if firstRun else j + timedelta(1)
            j = i + timedelta(300) if i + timedelta(300) < end else end
            firstRun = False
            params = {'start': i.isoformat(),
                      'end': j.isoformat(),
                      'granularity': granularity}
            r = requests.get(API_URL + 'products/{0}/candles'.format(productId), params=params)
            # Response gets end data first, so have to traverse response in reverse
            for line in r.json()[::-1]:
                row = [datetime.utcfromtimestamp(line[0]).strftime('%Y-%m-%d')] + line[1:]
                rows.append(row)
            time.sleep(.5)

        CoinbaseAPI.writeCsv(rows, productId, 0, start, end)

    @staticmethod
    def writeCsv(data, productId, granularity, start=None, end=None):
        header = ['date', 'low', 'high', 'open', 'close', 'volume']
        timeframe = CoinbaseAPI.getStrFromIntGranularity(granularity)
        candles = pd.DataFrame(data, columns=header)
        currDirName = os.path.dirname(__file__)
        pair = productId.split("-")[1]

        dateRange = ""
        # Date range data goes into temp folder
        if start:
            dateRange = "_{}_{}".format(start.isoformat(), end.isoformat())
            timeframe = "temp"
        dirToWrite = os.path.join(currDirName, '../../HistoricalData/Coinbase/{0}/{1}/'.format(timeframe, pair))
        fileName = dirToWrite + "{}{}.csv".format(productId, dateRange)
        if not os.path.isdir(dirToWrite):
            os.makedirs(dirToWrite)
        candles.to_csv(fileName, index=False)

    @staticmethod
    def getAllHistoricalPricesForAllUsdCoins():
        for product in CoinbaseAPI.getAllUsdProducts():
            print("Writing Data for {}".format(product))
            CoinbaseAPI.getAllHistoricalPrices(product)
            time.sleep(.5)

    @staticmethod
    def getAllUsdProducts(saveTradingViewCopy=False):
        r = requests.get(API_URL + 'products')
        products = []
        for line in r.json():
            if '-USD' in line['id'] and not any(x in line['id'] for x in ['USDT', 'USDC', 'PAX', 'UST']):
                products.append(line['id'])

        if saveTradingViewCopy:
            file = open('coinbase-usd.txt', 'w')
            for product in products:
                file.write("COINBASE:{}\n".format(product.replace("-", "")))
            file.close()
        print("Total number of products: {}".format(len(products)))
        return products

    @staticmethod
    def getUnlistedCoins():
        params = {'start': date.today(),
                  'end': date.today(),
                  'granularity': 86400}
        for product in CoinbaseAPI.getAllUsdProducts():
            try:
                requests.get(API_URL + 'products/{0}/candles'.format(product), params=params)
            except:
                print(product)

    @staticmethod
    def getStrFromIntGranularity(granularity):
        if granularity == 86400:
            return "daily"
        elif granularity == 21600:
            return "6_hr"
        elif granularity == 3600:
            return "1_hr"
        else:
            return "temp"

CoinbaseAPI.getAllHistoricalPricesForAllUsdCoins()
