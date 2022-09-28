import logging
from datetime import datetime
import os
from MDRecorderBase import MDRecorderBase
import time


class consts:
    KEY_PRODUCTID = 'id'
    KEY_QUOTECURRENCY = 'quote_currency'
    KEY_BASECURRENCY = 'base_currency'


# https://docs.cloud.coinbase.com/exchange/reference/exchangerestapi_getproductcandles
# Data output in descending order i.e. oldest date first
class coinbaseMDRecorder(MDRecorderBase):
    def __init__(self, api_url, header, key_date, maxCandlesPerAPIRequest, exchangeName, interestingBaseCurrencies,
                 interestingQuoteCurrencies, outputDirectory, timeframes, writeNewFiles, maxAPIRequestsPerSec,
                 cooldownPeriodInSec):
        MDRecorderBase.__init__(self, api_url, header, key_date, maxCandlesPerAPIRequest, exchangeName,
                                interestingBaseCurrencies, interestingQuoteCurrencies, outputDirectory, timeframes,
                                writeNewFiles, maxAPIRequestsPerSec, cooldownPeriodInSec)

    def getAllInterestingProductIDs(self):
        request_url = self.api_url + 'products'
        r = self.requestHandler.get(request_url)

        product_ids = []
        response_list = r.json()
        for response in response_list:
            quoteCurrency = response[consts.KEY_QUOTECURRENCY]
            symbol = response[consts.KEY_BASECURRENCY]
            if self.isInterestingQuoteCurrency(quoteCurrency) and self.isInterestingBaseCurrency(symbol):
                product_ids.append(response[consts.KEY_PRODUCTID])

        product_ids_str = '\n' + '\n'.join(product_ids)
        logging.info(f'{len(product_ids)}/{len(response_list)} interesting products found: {product_ids_str}')
        return product_ids

    def downloadAndWriteData(self, productId, timeframeStr,filename):
        granularity = self.getGranularityFromTimeframeStr(timeframeStr)
        minReqStartTime = self.getMinReqStartTime(filename)
        candles = []
        numEmptyResponses = 0
        request_url = self.api_url + f'products/{productId}/candles'
        reqEndTime = int(granularity * int(time.time() / granularity))

        logging.info(f'Starting download of {timeframeStr} candles for {productId} to {filename}.'
                      f' minReqStartTime:{minReqStartTime}')
        loop_iteration_number = 0
        while numEmptyResponses < 3 and reqEndTime >= minReqStartTime:
            loop_iteration_number += 1
            reqStartTime = reqEndTime - granularity * (self.maxCandlesPerAPIRequest - 1)
            reqStartTime = max(minReqStartTime, reqStartTime)
            if loop_iteration_number == 1 and minReqStartTime == 0:
                params = {
                    'granularity': granularity
                }
            else:
                params = {
                    'granularity': str(int(granularity)),
                    'start': str(int(reqStartTime)),
                    'end': str(int(reqEndTime))
                }

            r = self.requestHandler.get(request_url, params)
            r_json = r.json()

            if loop_iteration_number == 1 and minReqStartTime == 0 and len(r_json) > 0:
                reqStartTime = self.getDateTimestampFromLine(','.join(str(x) for x in r_json[-1]))
            reqEndTime = reqStartTime - granularity

            if len(r_json) == 0:
                numEmptyResponses += 1
                reqStartTime += granularity * self.maxCandlesPerAPIRequest
                logging.debug(f'Received empty response. numEmptyResponses:{numEmptyResponses}')
                continue

            candles += r_json
            numEmptyResponses = 0
            earliestTimestamp = self.getDateTimestampFromLine(','.join(str(x) for x in r_json[-1]))
            latestTimestamp = self.getDateTimestampFromLine(','.join(str(x) for x in r_json[0]))
            logging.info(f'NumCandlesReceived:{len(r_json)}'
                          f' EarliestTimestamp:{earliestTimestamp} ({datetime.fromtimestamp(earliestTimestamp)})'
                          f' LatestTimestamp:{latestTimestamp} ({datetime.fromtimestamp(latestTimestamp)})')
        self.writeToCsv(candles[::-1], filename)

    def getMinReqStartTime(self, filename):
        fileExists = os.path.isfile(filename)
        if self.writeNewFiles or not fileExists:
            return 0

        minReqStartTime = self.getLatestTimestampFromFile(filename)
        logging.debug(f'File:{filename} Exists:{fileExists} minReqStartTime:{minReqStartTime}')
        return minReqStartTime

    @staticmethod
    def getGranularityFromTimeframeStr(timeframeStr):
        match timeframeStr:
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
                logging.error(f'Unsupported timeframe:{timeframeStr}. Investigate and fix...')
                quit()
