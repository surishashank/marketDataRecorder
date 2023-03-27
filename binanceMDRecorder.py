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
    def __init__(self, api_url: str, header: list[str], key_date: str, maxCandlesPerAPIRequest: int, exchangeName: str,
                 interestingBaseCurrencies: list[str], interestingQuoteCurrencies: list[str], outputDirectory: str,
                 timeframes: list[str], writeNewFiles: bool, maxAPIRequestsPerSec: int, cooldownPeriodInSec: int):
        MDRecorderBase.__init__(self, api_url, header, key_date, maxCandlesPerAPIRequest, exchangeName,
                                interestingBaseCurrencies, interestingQuoteCurrencies, outputDirectory, timeframes,
                                writeNewFiles, maxAPIRequestsPerSec, cooldownPeriodInSec)

    def getAllInterestingProductIDs(self) -> list[str]:
        request_url = self.api_url + 'exchangeInfo'
        r = self.requestHandler.get(request_url)

        interesting_product_ids: list[str] = []
        symbol_info_list: list[dict[str, str]] = r.json()[consts.KEY_SYMBOLS]
        for symbol_info in symbol_info_list:
            quote_currency: str = symbol_info[consts.KEY_QUOTEASSET]
            symbol: str = symbol_info[consts.KEY_BASEASSET]
            if self.isInterestingQuoteCurrency(quote_currency) and self.isInterestingBaseCurrency(symbol):
                product_id: str = self.getProductIdFromCoinAndQuoteCurrency(symbol, quote_currency)
                interesting_product_ids.append(product_id)

        random.shuffle(interesting_product_ids)
        product_ids_str = '\n' + '\n'.join(interesting_product_ids)
        logging.info(
            f'{len(interesting_product_ids)}/{len(symbol_info_list)} interesting products found:{product_ids_str}')
        return interesting_product_ids

    def getAllDelistedProductIDs(self, interesting_product_ids: list[str]) -> list[str]:
        request_url = self.api_url + 'exchangeInfo'
        r = self.requestHandler.get(request_url)

        delisted_product_ids: list[str] = []
        symbol_info_list: list[dict[str, str]] = r.json()[consts.KEY_SYMBOLS]
        for symbol_info in symbol_info_list:
            trading_status: str = symbol_info[consts.KEY_TRADINGSTATUS]
            if trading_status == consts.KEY_TRADINGSTATUS_DELISTED:
                symbol: str = symbol_info[consts.KEY_BASEASSET]
                quote_currency: str = symbol_info[consts.KEY_QUOTEASSET]
                product_id: str = self.getProductIdFromCoinAndQuoteCurrency(symbol, quote_currency)
                if not interesting_product_ids or product_id in interesting_product_ids:
                    delisted_product_ids.append(product_id)

        delisted_product_ids_str = '\n' + '\n'.join(delisted_product_ids)
        logging.info(f'{len(delisted_product_ids)} delisted products found: {delisted_product_ids_str}')
        return delisted_product_ids

    def downloadAndWriteData(self, product_id: str, timeframe: str, filename: str, is_delisted: str) -> bool:
        if not self.validateTimeframeStr(timeframe):
            logging.error(f'Invalid timeframe:{timeframe} for ProductID:{product_id}. Skipping...')
            return False

        granularity: int = self.getNumMillisecondsFromTimeframeStr(timeframe)
        req_start_time: int = self.getReqStartTime(filename)
        candles: list[list] = []
        num_empty_responses: int = 0
        request_url: str = self.api_url + 'klines'
        logging.info(f'Starting download of {timeframe} candles for {product_id} to {filename}.'
                     f' reqStartTime:{req_start_time}')

        while num_empty_responses < 3 and req_start_time < time.time() * 1000:
            params: dict[str, str] = {
                'symbol': product_id.replace('-', ''),
                'interval': timeframe,
                'startTime': str(int(req_start_time)),
                # 'endTime': e,
                'limit': str(int(self.maxCandlesPerAPIRequest))
            }
            r = self.requestHandler.get(request_url, params)
            r_json: list[list] = r.json()
            if len(r_json) == 0:
                num_empty_responses += 1
                req_start_time += granularity * self.maxCandlesPerAPIRequest
                logging.info(f'Received blank response. numEmptyResponses:{num_empty_responses}')
                continue

            candles += r_json
            num_empty_responses = 0
            earliest_timestamp: int = self.getDateTimestampFromLine(','.join(str(x) for x in r_json[0]))
            latest_timestamp: int = self.getDateTimestampFromLine(','.join(str(x) for x in r_json[-1]))
            logging.info(f'URL:{r.url} NumCandlesReceived:{len(r_json)} '
                         f'EarliestTimestamp:{earliest_timestamp} ({datetime.fromtimestamp(earliest_timestamp / 1000)})'
                         f' LatestTimestamp:{latest_timestamp} ({datetime.fromtimestamp(latest_timestamp / 1000)})')

            # These conditions would be true only if a request is sent on a delisted product
            # and there is an up to date existing market data file
            if is_delisted and len(r_json) == 1 and len(candles) == 1 and req_start_time != 0:
                candle_str: str = ','.join(str(e) for e in candles[0])
                raw_last_line: str = self.getLastNonBlankLineFromFile(filename)

                # also create float arrays in case one of the lines has a number like 1.0 instead of 1 etc
                candle_float_arr: list[float] = [float(x) for x in candles[0]]
                raw_last_line_arr: list[float] = [float(x) for x in raw_last_line.split(',')]

                if candle_str == raw_last_line or candle_float_arr == raw_last_line_arr:
                    logging.info(f'Nothing to update for delisted product:{product_id}. Skipping file:{filename}')
                    return True

            req_start_time = latest_timestamp + granularity
        return self.writeToCsv(candles, filename)

    # Available timeframes:
    # s-> seconds; m -> minutes; h -> hours; d -> days; w -> weeks; M -> months
    # 1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
    @staticmethod
    def validateTimeframeStr(timeframe: str) -> bool:
        valid_timeframes: set[str] = {'1s', '1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h',
                                      '8h', '12h', '1d', '3d', '1w', '1M'}

        if timeframe not in valid_timeframes:
            logging.error(f'Unsupported timeframe:{timeframe}. Investigate and fix...')
            return False

        return True

    @staticmethod
    def getNumMillisecondsFromTimeframeStr(timeframe: str) -> int:
        if not binanceMDRecorder.validateTimeframeStr(timeframe):
            os._exit(1)

        match timeframe:
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
                logging.error(f'Serious ERROR. Unsupported timeframe:{timeframe}. Investigate and fix...')
                os._exit(1)

    def getReqStartTime(self, filename: str) -> int:
        file_exists = os.path.isfile(filename)
        if self.writeNewFiles or not file_exists:
            return 0

        return self.getLatestTimestampFromFile(filename)
