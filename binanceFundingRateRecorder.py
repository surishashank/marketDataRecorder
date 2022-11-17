import logging
import os.path
import random
import sys
import time
from datetime import datetime
from MDRecorderBase import MDRecorderBase
import os


class consts:
    BINANCE_FUNDINGRATE_TIMEFRAME = '8h'
    KEY_SYMBOLS = 'symbols'
    KEY_BASEASSET = 'baseAsset'
    KEY_QUOTEASSET = 'quoteAsset'
    KEY_CONTRACTTYPE = 'contractType'
    KEY_CONTRACTTYPE_PERP = 'PERPETUAL'
    KEY_TRADINGSTATUS = 'status'
    KEY_TRADINGSTATUS_TRADING = 'TRADING'
    KEY_FUNDINGTIME = 'fundingTime'
    KEY_FUNDINGRATE = 'fundingRate'


class binanceFundingRateRecorder(MDRecorderBase):
    def __init__(self, api_url, header, key_date, maxCandlesPerAPIRequest, exchangeName, interestingBaseCurrencies,
                 interestingQuoteCurrencies, outputDirectory, writeNewFiles, maxAPIRequestsPerSec,
                 cooldownPeriodInSec):
        MDRecorderBase.__init__(self, api_url, header, key_date, maxCandlesPerAPIRequest, exchangeName,
                                interestingBaseCurrencies, interestingQuoteCurrencies, outputDirectory,
                                [consts.BINANCE_FUNDINGRATE_TIMEFRAME], writeNewFiles, maxAPIRequestsPerSec,
                                cooldownPeriodInSec)

    def getAllInterestingProductIDs(self):
        request_url = self.api_url + 'exchangeInfo'
        r = self.requestHandler.get(request_url)

        interesting_product_ids = []
        symbol_info_list = r.json()[consts.KEY_SYMBOLS]
        for symbol_info in symbol_info_list:
            symbol = symbol_info[consts.KEY_BASEASSET]
            quote_currency = symbol_info[consts.KEY_QUOTEASSET]
            isPerpetualFuture = symbol_info[consts.KEY_CONTRACTTYPE] == consts.KEY_CONTRACTTYPE_PERP
            if isPerpetualFuture and self.isInterestingQuoteCurrency(quote_currency) and self.isInterestingBaseCurrency(symbol):
                product_id = self.getProductIdFromCoinAndQuoteCurrency(symbol, quote_currency)
                interesting_product_ids.append(product_id)

        random.shuffle(interesting_product_ids)
        product_ids_str = '\n' + '\n'.join(interesting_product_ids)
        logging.info(
            f'{len(interesting_product_ids)}/{len(symbol_info_list)} interesting products found:{product_ids_str}')
        return interesting_product_ids

    def getAllDelistedProductIDs(self, interesting_product_id_list):
        request_url = self.api_url + 'exchangeInfo'
        r = self.requestHandler.get(request_url)

        delisted_product_ids = []
        symbol_info_list = r.json()[consts.KEY_SYMBOLS]
        for symbol_info in symbol_info_list:
            trading_status = symbol_info[consts.KEY_TRADINGSTATUS]
            if trading_status != consts.KEY_TRADINGSTATUS_TRADING:
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
            return False

        granularity = self.getNumMillisecondsFromTimeframeStr(timeframeStr)
        minReqStartTime = self.getMinReqStartTime(filename)
        candles = []
        request_url = self.api_url + 'fundingRate'
        reqEndTime = int(granularity * int(time.time()*1000 / granularity))
        logging.info(f'Starting download of funding rates for {productId} to {filename}. '
                     f'minReqStartTime:{minReqStartTime}')
        loop_iteration_number = 0
        while reqEndTime > minReqStartTime:
            loop_iteration_number += 1
            reqStartTime = reqEndTime - granularity * self.maxCandlesPerAPIRequest
            reqStartTime = max(minReqStartTime, reqStartTime)

            params = {
                'symbol': productId.replace('-', ''),
                'startTime': str(int(reqStartTime)),
                'endTime': str(int(reqEndTime)),
                'limit': str(int(self.maxCandlesPerAPIRequest))
            }
            if loop_iteration_number == 1 and isDelisted:
                params = {
                    'symbol': productId.replace('-', ''),
                    'limit': str(int(self.maxCandlesPerAPIRequest))
                }
            r = self.requestHandler.get(request_url, params)

            r_json = r.json()
            if len(r_json) == 0:
                logging.info(f'Received blank response. Breaking out of loop')
                break

            new_candles_arr = []
            for entry in r_json:
                fundingTime = entry[consts.KEY_FUNDINGTIME]
                fundingRate = entry[consts.KEY_FUNDINGRATE]
                new_candles_arr.append([fundingTime, fundingRate])

            if loop_iteration_number == 1 and isDelisted:
                reqStartTime = self.getDateTimestampFromLine(','.join(str(x) for x in new_candles_arr[0]))
                # if file exists, check if it is already up to date
                if minReqStartTime != 0:
                    latestCandleStr = ','.join(str(x) for x in new_candles_arr[-1])
                    rawLastLine = self.getLastNonBlankLineFromFile(filename)

                    # also create float arrays in case one of the lines has a number like 1.0 instead of 1 etc
                    latestCandleFloatArr = [float(x) for x in latestCandleStr.split(',')]
                    rawLastLineArr = [float(x) for x in rawLastLine.split(',')]

                    if latestCandleStr == rawLastLine or latestCandleFloatArr == rawLastLineArr:
                        # This code will only be reached if a request is sent on a delisted product
                        # and there is an up to date existing market data file
                        logging.info(f'Nothing to update for delisted product:{productId}. Skipping file:{filename}')
                        return True

            reqEndTime = int(reqStartTime/1000)*1000

            candles += new_candles_arr
            earliestTimestamp = self.getDateTimestampFromLine(','.join(str(x) for x in new_candles_arr[0]))
            latestTimestamp = self.getDateTimestampFromLine(','.join(str(x) for x in new_candles_arr[-1]))
            logging.info(f'URL:{r.url} NumCandlesReceived:{len(new_candles_arr)} '
                         f'EarliestTimestamp:{earliestTimestamp} ({datetime.fromtimestamp(earliestTimestamp / 1000)}) '
                         f'LatestTimestamp:{latestTimestamp} ({datetime.fromtimestamp(latestTimestamp / 1000)})')

        if loop_iteration_number == 0 and len(candles) == 0 and minReqStartTime != 0:
            logging.info(f'Data already up to date for {filename}')
            return True

        return self.writeToCsv(candles, filename)

    # Available timeframes: 8h
    @staticmethod
    def validateTimeframeStr(timeframeStr):
        valid_timeframes = ['8h']

        if timeframeStr not in valid_timeframes:
            logging.error(f'Unsupported timeframe:{timeframeStr}. Investigate and fix...')
            return False

        return True

    @staticmethod
    def getNumMillisecondsFromTimeframeStr(timeframeStr):
        if not binanceFundingRateRecorder.validateTimeframeStr(timeframeStr):
            os._exit(1)

        match timeframeStr:
            case '8h':
                return 1000 * 60 * 60 * 8
            case _:  # this should never happen because we validate the parameter beforehand
                logging.error(f'Serious ERROR. Unsupported timeframe:{timeframeStr}. Investigate and fix...')
                os._exit(1)

    def getMinReqStartTime(self, filename):
        fileExists = os.path.isfile(filename)
        if self.writeNewFiles or not fileExists:
            return 0

        return self.getLatestTimestampFromFile(filename)
