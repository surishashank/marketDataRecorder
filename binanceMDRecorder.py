import logging
import os.path
import random
import time
from datetime import datetime
from MDRecorderBase import MDRecorderBase


class consts:
    KEY_SYMBOLS = 'symbols'
    KEY_BASEASSET = 'baseAsset'
    KEY_QUOTEASSET = 'quoteAsset'
    KEY_PRODUCTID = 'symbol'
    KEY_TRADINGSTATUS = 'status'
    KEY_TRADINGSTATUS_TRADING = 'TRADING'
    KEY_TRADINGSTATUS_DELISTED = 'BREAK'


class binanceMDRecorder(MDRecorderBase):
    def __init__(self, api_url, header, key_date, maxCandlesPerAPIRequest, exchangeName, interestingBaseCurrencies,
                 interestingQuoteCurrencies, outputDirectory, timeframes, writeNewFiles, maxAPIRequestsPerSec,
                 cooldownPeriodInSec):
        MDRecorderBase.__init__(self, api_url, header, key_date, maxCandlesPerAPIRequest, exchangeName,
                                interestingBaseCurrencies, interestingQuoteCurrencies, outputDirectory, timeframes,
                                writeNewFiles, maxAPIRequestsPerSec, cooldownPeriodInSec)

    def getAllInterestingProductIDs(self):
        request_url = self.api_url + 'exchangeInfo'
        r = self.requestHandler.get(request_url)

        interesting_product_ids = []
        symbol_info_list = r.json()[consts.KEY_SYMBOLS]
        for symbol_info in symbol_info_list:
            quote_currency = symbol_info[consts.KEY_QUOTEASSET]
            symbol = symbol_info[consts.KEY_BASEASSET]
            if self.isInterestingQuoteCurrency(quote_currency) and self.isInterestingBaseCurrency(symbol):
                product_id = self.getProductIdFromCoinAndQuoteCurrency(symbol, quote_currency)
                interesting_product_ids.append(product_id)

        random.shuffle(interesting_product_ids)
        product_ids_str = '\n' + '\n'.join(interesting_product_ids)
        logging.info(f'{len(interesting_product_ids)}/{len(symbol_info_list)} interesting products found:{product_ids_str}')
        return interesting_product_ids

    def getAllDelistedProductIDs(self, interesting_product_id_list):
        request_url = self.api_url + 'exchangeInfo'
        r = self.requestHandler.get(request_url)

        delisted_product_ids = []
        symbol_info_list = r.json()[consts.KEY_SYMBOLS]
        for symbol_info in symbol_info_list:
            trading_status = symbol_info[consts.KEY_TRADINGSTATUS]
            if trading_status == consts.KEY_TRADINGSTATUS_DELISTED:
                symbol = symbol_info[consts.KEY_BASEASSET]
                quote_currency = symbol_info[consts.KEY_QUOTEASSET]
                product_id = self.getProductIdFromCoinAndQuoteCurrency(symbol, quote_currency)
                if not interesting_product_id_list or product_id in interesting_product_id_list:
                    delisted_product_ids.append(product_id)

        delisted_product_ids_str = '\n' + '\n'.join(delisted_product_ids)
        logging.info(f'{len(delisted_product_ids)} delisted products found: {delisted_product_ids_str}')
        return delisted_product_ids

    def downloadAndWriteData(self, productId, timeframeStr, filename, isDelisted):
        if not self.validateTimeframeStr(timeframeStr):
            logging.error(f'Invalid timeframe:{timeframeStr} for ProductID:{productId}. Skipping...')
            return

        granularity = self.getNumMillisecondsFromTimeframeStr(timeframeStr)
        reqStartTime = self.getReqStartTime(filename)
        candles = []
        numEmptyResponses = 0
        request_url = self.api_url + 'klines'

        while numEmptyResponses < 3 and reqStartTime < time.time() * 1000:
            params = {
                'symbol': productId.replace('-',''),
                'interval': timeframeStr,
                'startTime': str(int(reqStartTime)),
                # 'endTime': e,
                'limit': str(int(self.maxCandlesPerAPIRequest))
            }
            r = self.requestHandler.get(request_url, params)
            r_json = r.json()
            if len(r_json) == 0:
                numEmptyResponses += 1
                reqStartTime += granularity * self.maxCandlesPerAPIRequest
                logging.info(f'Received blank response. numEmptyResponses:{numEmptyResponses}')
                continue

            candles += r_json
            numEmptyResponses = 0
            earliestTimestamp = self.getDateTimestampFromLine(','.join(str(x) for x in r_json[0]))
            latestTimestamp = self.getDateTimestampFromLine(','.join(str(x) for x in r_json[-1]))
            logging.info(f'NumCandlesReceived:{len(r_json)}'
                          f' EarliestTimestamp:{earliestTimestamp} ({datetime.fromtimestamp(earliestTimestamp / 1000)})'
                          f' LatestTimestamp:{latestTimestamp} ({datetime.fromtimestamp(latestTimestamp / 1000)})')

            # These conditions would be true only if a request is sent on a delisted product
            # and there is an up to date existing market data file
            if isDelisted and len(r_json) == 1 and len(candles) == 1 and reqStartTime != 0:
                rawLastLine = self.getLastNonBlankLineFromFile(filename)
                candleStr = ','.join(str(e) for e in candles[0])
                if candleStr == rawLastLine:
                    logging.info(f'Nothing to update for delisted product:{productId}. Skipping file:{filename}')
                    return True

            reqStartTime = latestTimestamp + granularity
        return self.writeToCsv(candles, filename)

    # Available timeframes:
    # s-> seconds; m -> minutes; h -> hours; d -> days; w -> weeks; M -> months
    # 1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
    @staticmethod
    def validateTimeframeStr(timeframeStr):
        valid_timeframes = ['1s', '1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w',
                            '1M']

        if timeframeStr not in valid_timeframes:
            logging.error(f'Unsupported timeframe:{timeframeStr}. Investigate and fix...')
            return False

        return True

    @staticmethod
    def getNumMillisecondsFromTimeframeStr(timeframeStr):
        if not binanceMDRecorder.validateTimeframeStr(timeframeStr):
            return 0

        match timeframeStr:
            case '1s':
                return 1000 * 1
            case '1m':
                return 1000 * 60
            case '3m':
                return 1000 * 60 * 3
            case '5m':
                return 1000 * 60 * 5
            case '15m':
                return 1000 * 60 * 15
            case '30m':
                return 1000 * 60 * 30
            case '1h':
                return 1000 * 60 * 60
            case '2h':
                return 1000 * 60 * 60 * 2
            case '4h':
                return 1000 * 60 * 60 * 4
            case '6h':
                return 1000 * 60 * 60 * 6
            case '8h':
                return 1000 * 60 * 60 * 8
            case '12h':
                return 1000 * 60 * 60 * 12
            case '1d':
                return 1000 * 60 * 60 * 24
            case '3d':
                return 1000 * 60 * 60 * 24 * 3
            case '1w':
                return 1000 * 60 * 60 * 24 * 7
            case '1M':
                return 1000 * 60 * 60 * 24 * 28
            case _:  # this should never happen because we validate the parameter beforehand
                logging.error(f'Serious ERROR. Unsupported timeframe:{timeframeStr}. Investigate and fix...')
                quit()

    def getReqStartTime(self, filename):
        fileExists = os.path.isfile(filename)
        if self.writeNewFiles or not fileExists:
            return 0

        return self.getLatestTimestampFromFile(filename)
