import math
from datetime import datetime, timedelta, date
from coinbaseExchangeAuth import CoinbaseConsts as const

import pandas as pd
import requests
import time
import os


# TODO: Make the following command line inputs
#  granularity (getAllHistoricalPrices); saveTradingViewCopy(getAllUsdProducts)

# https://docs.cloud.coinbase.com/exchange/reference/exchangerestapi_getproductcandles
# Data output in descending order i.e. oldest date first
# TODO: Add new data to daily data everyday and if date range data is needed it should be taken from the existing
#  daily/ 6hr/ 1hr etc data. This partial data should be stored in a temp folder
class coinbaseMDRecorder:
    def __init__(self, api_url, header, maxCandlesPerAPIRequest, exchangeName, interestingBaseCurrencies,
                 interestingQuoteCurrencies, outputDirectory, timeframes, writeNewFiles):
        self.api_url = api_url
        self.header = header
        self.maxCandlesPerAPIRequest = maxCandlesPerAPIRequest
        self.exchangeName = exchangeName

        self.interestingBaseCurrencies = interestingBaseCurrencies
        self.interestingQuoteCurrencies = interestingQuoteCurrencies
        self.outputDirectory = outputDirectory
        self.timeframes = timeframes
        # self.granularity = self.getGranularityFromTimeframeStr(timeframe)
        self.writeNewFiles = writeNewFiles

    def getFilenameForProductID(self, productId, timeframe):
        fileName = os.path.join(self.outputDirectory,
                                '{}_{}_{}.csv'.format(self.exchangeName, productId, timeframe))
        return fileName

    def writeHistoricalDataForProductToFile(self, productId, timeframe):
        filename = self.getFilenameForProductID(productId, timeframe)
        minRequestStartTime = 0
        if os.path.isfile(filename) and not self.writeNewFiles:
            minRequestStartTime = self.getLatestTimestampFromFile(filename)
            print('writeHistoricalDataForProductToFile:: File: {} already exists. Setting minRequestStartTime: {} ({})'
                  ''.format(filename, minRequestStartTime, datetime.fromtimestamp(minRequestStartTime)))
        else:
            print('writeHistoricalDataForProductToFile:: Writing new file:', filename, 'minRequestStartTime:',
                  minRequestStartTime, '({})'.format(datetime.fromtimestamp(minRequestStartTime)))

        self.getHistoricalPricesAndWriteToFile(productId, minRequestStartTime, filename,
                                               self.getGranularityFromTimeframeStr(timeframe))

    def getLatestTimestampFromFile(self, filename):
        lastLine = None
        numLines = 0
        with open(filename) as f:
            for line in f:
                numLines += 1
                if len(line) > 0:
                    lastLine = line
            if numLines <= 1:
                return 0
        latestTimestamp = self.getDateTimestamp(lastLine)
        return latestTimestamp

    # Can only request 300 candles per request
    def getHistoricalPricesAndWriteToFile(self, productId, minReqStartTime, filename, granularity):
        candles = []
        numBlankRequests = 0
        reqEndTime = int(granularity * int(time.time() / granularity))
        while reqEndTime >= minReqStartTime:
            reqStartTime = reqEndTime - granularity * (self.maxCandlesPerAPIRequest - 1)
            reqStartTime = max(minReqStartTime, reqStartTime)

            params = {
                'granularity': granularity,
                'start': str(reqStartTime),
                'end': str(reqEndTime)
            }
            requestStr = self.api_url + 'products/{0}/candles'.format(productId)
            r = requests.get(requestStr, params=params)
            print('getHistoricalPricesAndWriteToFile:: Request sent. URL:', r.url, 'start:',
                  datetime.fromtimestamp(reqStartTime), 'end:', datetime.fromtimestamp(reqEndTime))

            if not r.ok:
                print("ERROR! Request returned with status code:", r.status_code,
                      'and text', r.text, ". Exiting...")
                quit()

            reqEndTime = reqStartTime - granularity
            r_json = r.json()
            if len(r_json) == 0:
                numBlankRequests += 1
                print('Received blank response. NumBlankRequests:', numBlankRequests)
                if numBlankRequests >= 3:
                    break
                continue

            candles += r_json
            numBlankRequests = 0
            time.sleep(.12)
            ###############################################
            earliestTimestamp = self.getDateTimestamp(','.join(str(x) for x in r_json[-1]))
            latestTimestamp = self.getDateTimestamp(','.join(str(x) for x in r_json[0]))
            print('NumCandlesReceived:', len(r_json), 'EarliestTimestamp:', earliestTimestamp, 'or',
                  datetime.fromtimestamp(earliestTimestamp), 'LatestTimestamp:', latestTimestamp, 'or',
                  datetime.fromtimestamp(latestTimestamp), '\n')
            ###############################################

        self.writeToCsv(candles[::-1], filename)

    def getDateTimestamp(self, row):
        if not row:
            return 0
        # print('Row:', row)
        elements_arr = [x.strip() for x in row.split(',')]
        df = pd.DataFrame([elements_arr], columns=self.header)
        return int(df.at[0, 'date'])

    def writeToCsv(self, data, filename):
        candles = pd.DataFrame(data, columns=self.header).drop_duplicates('date')

        if not self.writeNewFiles and os.path.isfile(filename):
            old_candles = pd.read_csv(filename)
            candles = pd.merge(candles, old_candles, how='outer').drop_duplicates('date')
            candles.sort_values('date', inplace=True)

        # candles['date'] = pd.to_datetime(candles['date'], unit='s')
        candles.to_csv(filename, index=False)

    def recordHistoricalPricesForAllInterestingCoins(self):
        interestingProductIDs = self.getAllInterestingProductIDs()
        for product in interestingProductIDs:
            for timeframe in self.timeframes:
                print("Writing Data for product:{} on timeframe:{}".format(product, timeframe))
                self.writeHistoricalDataForProductToFile(product, timeframe)
                print('Successfully recorded data for {} on {}'.format(product, timeframe))
                # self.getAllHistoricalPrices(product)

    def getAllInterestingProductIDs(self):
        r = requests.get(self.api_url + 'products', timeout=3)
        if not r.ok:
            print("ERROR! Request returned with status code:", r.status_code, ". Exiting...")
            quit()

        products = []
        response_list = r.json()
        for response in response_list:
            quoteCurrency = response[const.KEY_QUOTECURRENCY]
            symbol = response[const.KEY_BASECURRENCY]
            if self.isInterestingQuoteCurrency(quoteCurrency) and self.isInterestingBaseCurrency(symbol):
                products.append(response[const.KEY_PRODUCTID])

        print("Total number of eligible products: {}".format(len(products)))
        print('\n'.join(products))
        return products

    def isInterestingQuoteCurrency(self, quoteCurrency):
        if not self.interestingQuoteCurrencies or len(self.interestingQuoteCurrencies) == 0:
            return True
        if quoteCurrency in self.interestingQuoteCurrencies:
            return True
        return False

    def isInterestingBaseCurrency(self, baseCurrency):
        if not self.interestingBaseCurrencies or len(self.interestingBaseCurrencies) == 0:
            return True
        if baseCurrency in self.interestingBaseCurrencies:
            return True
        return False

    @staticmethod
    def getGranularityFromTimeframeStr(timeframe):
        match timeframe:
            case '1m':
                return 60
            case '5m':
                return 300
            case '15m':
                return 900
            case '1h':
                return 3600
            case '6h':
                return 21600
            case '1d':
                return 86400
            case _:
                print('ERROR! Unsupported timeframe:', timeframe,
                      'passed to getGranularityFromTimeframeStr. Exiting...')
                quit()
