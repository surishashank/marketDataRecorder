import logging
import os
import random
import time
from datetime import datetime

from MDRecorderBase import MDRecorderBase


class consts:
    KEY_DATA = 'result'
    KEY_BASECURRENCY = 'baseCurrency'
    KEY_QUOTECURRENCY = 'quoteCurrency'
    KEY_TRADING_ENABLED = 'enabled'
    KEY_TOKENIZED_EQUITY = 'tokenizedEquity'
    KEY_IS_ETF_MARKET = 'isEtfMarket'
    KEY_DATA_TIMESTAMP_HUMANREADABLE = 'startTime'
    KEY_DATA_TIME = 'time'
    KEY_DATA_OPEN = 'open'
    KEY_DATA_HIGH = 'high'
    KEY_DATA_LOW = 'low'
    KEY_DATA_CLOSE = 'close'
    KEY_DATA_VOLUME_USD = 'volume'


class ftxMDRecorder(MDRecorderBase):
    def __init__(self, api_url, header, key_date, maxCandlesPerAPIRequest, exchangeName, interestingBaseCurrencies,
                 interestingQuoteCurrencies, outputDirectory, timeframes, writeNewFiles, maxAPIRequestsPerSec,
                 cooldownPeriodInSec):
        MDRecorderBase.__init__(self, api_url, header, key_date, maxCandlesPerAPIRequest, exchangeName,
                                interestingBaseCurrencies, interestingQuoteCurrencies, outputDirectory, timeframes,
                                writeNewFiles, maxAPIRequestsPerSec, cooldownPeriodInSec)

    def getAllInterestingProductIDs(self):
        request_url = self.api_url + 'markets'
        r = self.requestHandler.get(request_url)

        symbol_info_list = r.json()[consts.KEY_DATA]
        interesting_product_ids = []
        for symbol_info in symbol_info_list:
            quote_currency = symbol_info[consts.KEY_QUOTECURRENCY]
            coin = symbol_info[consts.KEY_BASECURRENCY]
            isTokenizedEquity = symbol_info.get(consts.KEY_TOKENIZED_EQUITY, False)
            isETF = symbol_info[consts.KEY_IS_ETF_MARKET]
            if self.isInterestingQuoteCurrency(quote_currency) and self.isInterestingBaseCurrency(coin) and \
                    not isTokenizedEquity and not isETF:
                product_id = self.getProductIdFromCoinAndQuoteCurrency(coin, quote_currency)
                interesting_product_ids.append(product_id)

        random.shuffle(interesting_product_ids)
        product_ids_str = '\n' + '\n'.join(interesting_product_ids)
        logging.info(
            f'{len(interesting_product_ids)}/{len(symbol_info_list)} interesting products found:{product_ids_str}')
        return interesting_product_ids

    def getAllDelistedProductIDs(self, interesting_product_id_list):
        request_url = self.api_url + 'markets'
        r = self.requestHandler.get(request_url)

        symbol_info_list = r.json()[consts.KEY_DATA]
        delisted_product_ids = []
        for symbol_info in symbol_info_list:
            trading_enabled = symbol_info[consts.KEY_TRADING_ENABLED]
            if not trading_enabled:
                quote_currency = symbol_info[consts.KEY_QUOTECURRENCY]
                coin = symbol_info[consts.KEY_BASECURRENCY]
                product_id = self.getProductIdFromCoinAndQuoteCurrency(coin, quote_currency)
                if not interesting_product_id_list or product_id in interesting_product_id_list:
                    delisted_product_ids.append(product_id)

        delisted_product_ids_str = '\n' + '\n'.join(delisted_product_ids)
        logging.info(f'{len(delisted_product_ids)} delisted products found: {delisted_product_ids_str}')
        return delisted_product_ids

    # Note: isDelisted case is not handled in FTX because I couldn't find an existing delisted product to
    # test is with.
    def downloadAndWriteData(self, productId, timeframeStr, filename, isDelisted):
        resolution = self.getResolutionFromTimeframeStrInSec(timeframeStr)
        minReqStartTime = self.getMinReqStartTime(filename)
        candles = []
        request_url = self.api_url + f'markets/{productId.replace("-", "/")}/candles'
        numEmptyResponses = 0
        reqEndTime = int(resolution * int(time.time() / resolution))
        loop_iteration_number = 0
        logging.info(f'Starting download of {timeframeStr} candles for {productId} to {filename}.'
                     f' minReqStartTime:{minReqStartTime}')
        while numEmptyResponses < 3 and reqEndTime >= minReqStartTime:
            loop_iteration_number += 1
            reqStartTime = reqEndTime - resolution * (self.maxCandlesPerAPIRequest - 1)
            reqStartTime = max(minReqStartTime, reqStartTime)

            if loop_iteration_number == 1 and minReqStartTime == 0:
                reqStartTime = 0
                params = {
                    'resolution': str(int(resolution))
                }
            else:
                params = {
                    'resolution': str(int(resolution)),
                    'start_time': str(int(reqStartTime)),
                    'end_time': str(int(reqEndTime))
                }

            r = self.requestHandler.get(request_url, params)
            r_json = r.json()[consts.KEY_DATA]

            if reqStartTime == 0 and len(r_json) > 0:
                reqStartTime = int(int(r_json[0][consts.KEY_DATA_TIME]) / 1000)

            reqEndTime = reqStartTime - resolution
            if len(r_json) == 0:
                numEmptyResponses += 1
                if reqStartTime == 0:
                    logging.error('Received empty response with reqStartTime:0. Unexpected behavior, aborting...')
                    return False
                logging.info(f'Received empty response. numEmptyResponses:{numEmptyResponses}')
                continue

            candles += [self.convertJSONLineToMDFileString(x) for x in r_json]
            numEmptyResponses = 0
            earliestTimestamp = int(r_json[0][consts.KEY_DATA_TIME])
            latestTimestamp = int(r_json[-1][consts.KEY_DATA_TIME])
            logging.info(f'NumCandlesReceived:{len(r_json)}'
                         f' EarliestTimestamp:{earliestTimestamp} ({datetime.fromtimestamp(earliestTimestamp / 1000)})'
                         f' LatestTimestamp:{latestTimestamp} ({datetime.fromtimestamp(latestTimestamp / 1000)})')
        return self.writeToCsv(candles, filename)

    @staticmethod
    def getResolutionFromTimeframeStrInSec(timeframeStr):
        match timeframeStr:
            case '15s':
                return 15
            case '1m':
                return 60
            case '5m':
                return 300
            case '15m':
                return 900
            case '1h':
                return 3600
            case '4h':
                return 21600
            case '1d':
                return 86400
            case _:
                logging.error(f'Unsupported timeframe:{timeframeStr}. Investigate and fix...')
                quit()

    def getMinReqStartTime(self, filename):
        fileExists = os.path.isfile(filename)
        if self.writeNewFiles or not fileExists:
            return 0

        minReqStartTime = int(self.getLatestTimestampFromFile(filename) / 1000)
        logging.debug(f'File:{filename} Exists:{fileExists} minReqStartTime:{minReqStartTime}')
        return minReqStartTime

    @staticmethod
    def convertJSONLineToMDFileString(json_line):
        timestamp_str = json_line[consts.KEY_DATA_TIMESTAMP_HUMANREADABLE]
        timestamp = json_line[consts.KEY_DATA_TIME]
        open = json_line[consts.KEY_DATA_OPEN]
        high = json_line[consts.KEY_DATA_HIGH]
        low = json_line[consts.KEY_DATA_LOW]
        close = json_line[consts.KEY_DATA_CLOSE]
        volume_usd = json_line[consts.KEY_DATA_VOLUME_USD]

        retval = [timestamp_str, timestamp, open, high, low, close, volume_usd]
        return retval
