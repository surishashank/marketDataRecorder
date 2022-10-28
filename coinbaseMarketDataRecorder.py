import logging
import random
from datetime import datetime
import os
from MDRecorderBase import MDRecorderBase
import time


class consts:
    KEY_PRODUCTID = 'id'
    KEY_QUOTECURRENCY = 'quote_currency'
    KEY_BASECURRENCY = 'base_currency'
    KEY_TRADINGSTATUS = 'status'
    KEY_TRADINGSTATUS_TRADING = 'online'
    KEY_TRADINGSTATUS_DELISTED = 'delisted'


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

        interesting_product_ids = []
        response_list = r.json()
        for response in response_list:
            quoteCurrency = response[consts.KEY_QUOTECURRENCY]
            symbol = response[consts.KEY_BASECURRENCY]
            if self.isInterestingQuoteCurrency(quoteCurrency) and self.isInterestingBaseCurrency(symbol):
                product_id = self.getProductIdFromCoinAndQuoteCurrency(symbol, quoteCurrency)
                interesting_product_ids.append(product_id)

        random.shuffle(interesting_product_ids)
        product_ids_str = '\n' + '\n'.join(interesting_product_ids)
        logging.info(
            f'{len(interesting_product_ids)}/{len(response_list)} interesting products found: {product_ids_str}')
        return interesting_product_ids

    def getAllDelistedProductIDs(self, interesting_product_id_list):
        request_url = self.api_url + 'products'
        r = self.requestHandler.get(request_url)
        delisted_product_ids = []
        response_list = r.json()
        for response in response_list:
            trading_status = response[consts.KEY_TRADINGSTATUS]
            if trading_status == consts.KEY_TRADINGSTATUS_DELISTED:
                quoteCurrency = response[consts.KEY_QUOTECURRENCY]
                symbol = response[consts.KEY_BASECURRENCY]
                product_id = self.getProductIdFromCoinAndQuoteCurrency(symbol, quoteCurrency)
                if not interesting_product_id_list or product_id in interesting_product_id_list:
                    delisted_product_ids.append(product_id)

        delisted_product_ids_str = '\n' + '\n'.join(delisted_product_ids)
        logging.info(f'{len(delisted_product_ids)} delisted products found: {delisted_product_ids_str}')
        return delisted_product_ids

    def downloadAndWriteData(self, productId, timeframeStr, filename, isDelisted):
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
            if loop_iteration_number == 1 and (minReqStartTime == 0 or isDelisted):
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

            if loop_iteration_number == 1 and len(r_json) > 0:
                if minReqStartTime == 0 or isDelisted:
                    reqStartTime = self.getDateTimestampFromLine(','.join(str(x) for x in r_json[-1]))

                if isDelisted and minReqStartTime != 0:
                    latestCandleStr = ','.join(str(e) for e in r_json[0])
                    rawLastLineFromDataFile = self.getLastNonBlankLineFromFile(filename)
                    if latestCandleStr == rawLastLineFromDataFile:
                        # This code will only be reached if a request is sent on a delisted product
                        # and there is an up to date existing market data file
                        logging.info(f'Nothing to update for delisted product:{productId}. Skipping file:{filename}')
                        return True

            reqEndTime = reqStartTime - granularity
            if len(r_json) == 0:
                numEmptyResponses += 1
                reqStartTime += granularity * self.maxCandlesPerAPIRequest
                logging.info(f'Received empty response. numEmptyResponses:{numEmptyResponses}')
                continue

            candles += r_json
            numEmptyResponses = 0
            earliestTimestamp = self.getDateTimestampFromLine(','.join(str(x) for x in r_json[-1]))
            latestTimestamp = self.getDateTimestampFromLine(','.join(str(x) for x in r_json[0]))
            logging.info(f'NumCandlesReceived:{len(r_json)}'
                         f' EarliestTimestamp:{earliestTimestamp} ({datetime.fromtimestamp(earliestTimestamp)})'
                         f' LatestTimestamp:{latestTimestamp} ({datetime.fromtimestamp(latestTimestamp)})')

        return self.writeToCsv(candles[::-1], filename)

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
