from datetime import datetime
import os
from MDRecorderBase import MDRecorderBase
# from coinbaseExchangeAuth import CoinbaseConsts as consts

import requests
import time


class consts:
    KEY_PRODUCTID = 'id'
    KEY_COINNAME = 'base_currency'
    KEY_QUOTECURRENCY = 'quote_currency'
    KEY_BASECURRENCY = 'base_currency'

# https://docs.cloud.coinbase.com/exchange/reference/exchangerestapi_getproductcandles
# Data output in descending order i.e. oldest date first
class coinbaseMDRecorder(MDRecorderBase):
    def __init__(self, api_url, header, key_date, maxCandlesPerAPIRequest, exchangeName, interestingBaseCurrencies,
                 interestingQuoteCurrencies, outputDirectory, timeframes, writeNewFiles):
        MDRecorderBase.__init__(self, api_url, header, key_date, maxCandlesPerAPIRequest, exchangeName, interestingBaseCurrencies,
                                interestingQuoteCurrencies, outputDirectory, timeframes, writeNewFiles)

    def getMinReqStartTime(self, filename):
        minReqStartTime = 0
        fileExists = os.path.isfile(filename)

        if fileExists and not self.writeNewFiles:
            minReqStartTime = self.getLatestTimestampFromFile(filename)

        print('getMinRequestStartTime:: File: {} Exists: {} minRequestStartTime: {} ({})'.format(
            filename, fileExists, minReqStartTime, datetime.fromtimestamp(minReqStartTime)))

        return minReqStartTime

    def downloadAndWriteData(self, productId, timeframeStr,filename):
        minReqStartTime = self.getMinReqStartTime(filename)
        candles = []
        numBlankRequests = 0
        granularity = self.getGranularityFromTimeframeStr(timeframeStr)
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
            earliestTimestamp = self.getDateTimestampFromLine(','.join(str(x) for x in r_json[-1]))
            latestTimestamp = self.getDateTimestampFromLine(','.join(str(x) for x in r_json[0]))
            print('NumCandlesReceived:', len(r_json), 'EarliestTimestamp:', earliestTimestamp, 'or',
                  datetime.fromtimestamp(earliestTimestamp), 'LatestTimestamp:', latestTimestamp, 'or',
                  datetime.fromtimestamp(latestTimestamp), '\n')
            ###############################################

        self.writeToCsv(candles[::-1], filename)

    def getAllInterestingProductIDs(self):
        r = requests.get(self.api_url + 'products', timeout=3)
        if not r.ok:
            print("ERROR! Request returned with status code:", r.status_code, ". Exiting...")
            quit()

        products = []
        response_list = r.json()
        for response in response_list:
            quoteCurrency = response[consts.KEY_QUOTECURRENCY]
            symbol = response[consts.KEY_BASECURRENCY]
            if self.isInterestingQuoteCurrency(quoteCurrency) and self.isInterestingBaseCurrency(symbol):
                products.append(response[consts.KEY_PRODUCTID])

        print("Total number of eligible products: {}".format(len(products)))
        print('\n'.join(products))
        return products

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
