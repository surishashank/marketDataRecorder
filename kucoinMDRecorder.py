import logging
from datetime import datetime
import os
import random
import time

from MDRecorderBase import MDRecorderBase


class consts:
    KEY_DATA = 'data'
    KEY_BASECURRENCY = 'baseCurrency'
    KEY_QUOTECURRENCY = 'quoteCurrency'
    KEY_TRADING_ENABLED = 'enableTrading'


class kucoinMDRecorder(MDRecorderBase):
    def __init__(self, api_url, header, key_date, maxCandlesPerAPIRequest, exchangeName, interestingBaseCurrencies,
                 interestingQuoteCurrencies, outputDirectory, timeframes, writeNewFiles, maxAPIRequestsPerSec,
                 cooldownPeriodInSec):
        MDRecorderBase.__init__(self, api_url, header, key_date, maxCandlesPerAPIRequest, exchangeName,
                                interestingBaseCurrencies, interestingQuoteCurrencies, outputDirectory, timeframes,
                                writeNewFiles, maxAPIRequestsPerSec, cooldownPeriodInSec)

    def getAllInterestingProductIDs(self):
        request_url = self.api_url + 'api/v2/symbols'
        r = self.requestHandler.get(request_url)

        symbol_info_list = r.json()[consts.KEY_DATA]
        interesting_product_ids = []
        for symbol_info in symbol_info_list:
            quote_currency = symbol_info[consts.KEY_QUOTECURRENCY]
            symbol = symbol_info[consts.KEY_BASECURRENCY]
            if self.isInterestingQuoteCurrency(quote_currency) and self.isInterestingBaseCurrency(symbol):
                product_id = self.getProductIdFromCoinAndQuoteCurrency(symbol, quote_currency)
                interesting_product_ids.append(product_id)

        random.shuffle(interesting_product_ids)
        product_ids_str = '\n' + '\n'.join(interesting_product_ids)
        logging.info(
            f'{len(interesting_product_ids)}/{len(symbol_info_list)} interesting products found:{product_ids_str}')
        return interesting_product_ids

    def getAllDelistedProductIDs(self, interesting_product_id_list):
        request_url = self.api_url + 'api/v2/symbols'
        r = self.requestHandler.get(request_url)

        symbol_info_list = r.json()[consts.KEY_DATA]
        delisted_product_ids = []
        for symbol_info in symbol_info_list:
            trading_enabled = symbol_info[consts.KEY_TRADING_ENABLED]
            if not trading_enabled:
                coin = symbol_info[consts.KEY_BASECURRENCY]
                quote_currency = symbol_info[consts.KEY_QUOTECURRENCY]
                product_id = self.getProductIdFromCoinAndQuoteCurrency(coin, quote_currency)
                if not interesting_product_id_list or product_id in interesting_product_id_list:
                    delisted_product_ids.append(product_id)

        delisted_product_ids_str = '\n' + '\n'.join(delisted_product_ids)
        logging.info(f'{len(delisted_product_ids)} delisted products found: {delisted_product_ids_str}')
        return delisted_product_ids

    def downloadAndWriteData(self, productId, timeframeStr, filename, isDelisted):
        if not self.validateTimeframeStr(timeframeStr):
            logging.error(f'Invalid timeframe:{timeframeStr} for ProductID:{productId}. Skipping...')
            return False

        candleType = self.getCandleTypeFromTimeframeStr(timeframeStr)
        granularity = self.getNumSecondsFromTimeframeStr(timeframeStr)
        minReqStartTime = self.getMinReqStartTime(filename)
        candles = []
        request_url = self.api_url + 'api/v1/market/candles'
        numEmptyResponses = 0
        reqEndTime = int(granularity * int(time.time() / granularity + 1))
        logging.info(f'Starting download of {timeframeStr} candles for {productId} to {filename}.'
                     f' minReqStartTime:{minReqStartTime}')
        loop_iteration_number = 0
        while numEmptyResponses < 3 and reqEndTime >= minReqStartTime:
            loop_iteration_number += 1
            reqStartTime = reqEndTime - granularity * self.maxCandlesPerAPIRequest
            reqStartTime = max(minReqStartTime, reqStartTime)

            if loop_iteration_number == 1 and minReqStartTime == 0:
                reqStartTime = 0
                params = {
                    'symbol': productId,
                    'type': candleType
                }
            else:
                params = {
                    'symbol': productId,
                    'type': candleType,
                    'startAt': str(int(reqStartTime)),
                    'endAt': str(int(reqEndTime))
                }

            r = self.requestHandler.get(request_url, params)
            r_json = r.json()[consts.KEY_DATA]

            if loop_iteration_number == 1 and len(r_json) > 0:
                if minReqStartTime == 0 or isDelisted:
                    reqStartTime = self.getDateTimestampFromLine(','.join(str(x) for x in r_json[-1]))

                if isDelisted and minReqStartTime != 0:
                    latestCandleStr = ','.join(str(e) for e in r_json[0])
                    rawLastLine = self.getLastNonBlankLineFromFile(filename)

                    # also create float arrays in case one of the lines has a number like 1.0 instead of 1 etc
                    latestCandleFloatArr = [float(x) for x in r_json[0]]
                    rawLastLineArr = [float(x) for x in rawLastLine.split(',')]

                    if latestCandleStr == rawLastLine or latestCandleFloatArr == rawLastLineArr:
                        # This code will only be reached if a request is sent on a delisted product
                        # and there is an up to date existing market data file
                        logging.info(f'Nothing to update for delisted product:{productId}. Skipping file:{filename}')
                        return True

            reqEndTime = reqStartTime
            if len(r_json) == 0:
                numEmptyResponses += 1
                logging.info(f'Received blank response. numEmptyResponses:{numEmptyResponses}')
                # If this was the first request sent to get latest candles then find where the
                # data for that instrument stopped being broadcast (can happen with delisted instruments)
                if reqStartTime == 0:
                    reqEndTime = self.findCloseTimestampOfLatestAvailableData(productId, request_url)
                continue

            candles += r_json
            numEmptyResponses = 0
            earliestTimestamp = self.getDateTimestampFromLine(','.join(str(x) for x in r_json[-1]))
            latestTimestamp = self.getDateTimestampFromLine(','.join(str(x) for x in r_json[0]))
            logging.info(f'NumCandlesReceived:{len(r_json)}'
                         f' EarliestTimestamp:{earliestTimestamp} ({datetime.fromtimestamp(earliestTimestamp)})'
                         f' LatestTimestamp:{latestTimestamp} ({datetime.fromtimestamp(latestTimestamp)})')

        return self.writeToCsv(candles[::-1], filename)

    @staticmethod
    def validateTimeframeStr(timeframeStr):
        valid_timeframes = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '1w']

        if timeframeStr not in valid_timeframes:
            logging.error(f'Unsupported timeframe:{timeframeStr}. Investigate and fix...')
            return False

        return True

    @staticmethod
    def getNumSecondsFromTimeframeStr(timeframeStr):
        if not kucoinMDRecorder.validateTimeframeStr(timeframeStr):
            quit()

        match timeframeStr:
            case '1m':
                return 60
            case '3m':
                return 60 * 3
            case '5m':
                return 60 * 5
            case '15m':
                return 60 * 15
            case '30m':
                return 60 * 30
            case '1h':
                return 60 * 60
            case '2h':
                return 60 * 60 * 2
            case '4h':
                return 60 * 60 * 4
            case '6h':
                return 60 * 60 * 6
            case '8h':
                return 60 * 60 * 8
            case '12h':
                return 60 * 60 * 12
            case '1d':
                return 60 * 60 * 24
            case '1w':
                return 60 * 60 * 24 * 7
            case _:  # this should never happen because we validate the parameter beforehand
                logging.error(f'Serious ERROR. Unsupported timeframe:{timeframeStr}. Investigate and fix...')
                quit()

    @staticmethod
    def getCandleTypeFromTimeframeStr(timeframeStr):
        if not kucoinMDRecorder.validateTimeframeStr(timeframeStr):
            quit()

        match timeframeStr:
            case '1m':
                return '1min'
            case '3m':
                return '3min'
            case '5m':
                return '5min'
            case '15m':
                return '15min'
            case '30m':
                return '30min'
            case '1h':
                return '1hour'
            case '2h':
                return '2hour'
            case '4h':
                return '4hour'
            case '6h':
                return '6hour'
            case '8h':
                return '8hour'
            case '12h':
                return '12hour'
            case '1d':
                return '1day'
            case '1w':
                return '1week'
            case _:  # this should never happen because we validate the parameter beforehand
                logging.error(f'Serious ERROR. Unsupported timeframe:{timeframeStr}. Investigate and fix...')
                quit()

    def getMinReqStartTime(self, filename):
        fileExists = os.path.isfile(filename)
        if self.writeNewFiles or not fileExists:
            return 0

        minReqStartTime = self.getLatestTimestampFromFile(filename)
        logging.debug(f'File:{filename} Exists:{fileExists} minReqStartTime:{minReqStartTime}')
        return minReqStartTime

    def findCloseTimestampOfLatestAvailableData(self, productId, request_url):
        latestDataTimestamp = 0
        calculatedCloseTimestamp = 0

        params = {
            'symbol': productId,
            'type': self.getCandleTypeFromTimeframeStr('1d')
        }
        r = self.requestHandler.get(request_url, params)
        r_json = r.json()[consts.KEY_DATA]
        logging.debug(f'findCloseTimestampOfLatestAvailableData received data:\n{r_json}')
        if len(r_json) > 0:
            latestDataTimestamp = self.getDateTimestampFromLine(','.join(str(x) for x in r_json[0]))
            calculatedCloseTimestamp = latestDataTimestamp + self.getNumSecondsFromTimeframeStr('1d')
        logging.info(f'findCloseTimestampOfLatestAvailableData returning calculatedCloseTimestamp:{calculatedCloseTimestamp} '
                     f'for product:{productId} with observed latestDataTimestamp:{latestDataTimestamp}')
        return calculatedCloseTimestamp

























