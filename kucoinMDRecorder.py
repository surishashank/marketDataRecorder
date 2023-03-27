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
    def __init__(self, api_url: str, header: list[str], key_date: str, max_candles_per_api_request: int,
                 exchange_name: str, interesting_base_currencies: list[str], interesting_quote_currencies: list[str],
                 output_directory: str, timeframes: list[str], write_new_files: bool, max_api_requests_per_sec: int,
                 cooldown_period_in_sec: int):
        MDRecorderBase.__init__(self, api_url, header, key_date, max_candles_per_api_request, exchange_name,
                                interesting_base_currencies, interesting_quote_currencies, output_directory, timeframes,
                                write_new_files, max_api_requests_per_sec, cooldown_period_in_sec)

    def getAllInterestingProductIDs(self) -> list[str]:
        request_url = self.api_url + 'api/v2/symbols'
        r = self.request_handler.get(request_url)

        symbol_info_list: list[dict[str, str]] = r.json()[consts.KEY_DATA]
        interesting_product_ids: list[str] = []
        for symbol_info in symbol_info_list:
            quote_currency: str = symbol_info[consts.KEY_QUOTECURRENCY]
            symbol: str = symbol_info[consts.KEY_BASECURRENCY]
            if self.isInterestingQuoteCurrency(quote_currency) and self.isInterestingBaseCurrency(symbol):
                product_id: str = self.getProductIdFromCoinAndQuoteCurrency(symbol, quote_currency)
                interesting_product_ids.append(product_id)

        random.shuffle(interesting_product_ids)
        product_ids_str = '\n' + '\n'.join(interesting_product_ids)
        logging.info(
            f'{len(interesting_product_ids)}/{len(symbol_info_list)} interesting products found:{product_ids_str}')
        return interesting_product_ids

    def getAllDelistedProductIDs(self, interesting_product_id_list: list[str]) -> list[str]:
        request_url = self.api_url + 'api/v2/symbols'
        r = self.request_handler.get(request_url)

        symbol_info_list: list[dict[str, str]] = r.json()[consts.KEY_DATA]
        delisted_product_ids: list[str] = []
        for symbol_info in symbol_info_list:
            trading_enabled: str = symbol_info[consts.KEY_TRADING_ENABLED]
            if not trading_enabled:
                coin: str = symbol_info[consts.KEY_BASECURRENCY]
                quote_currency: str = symbol_info[consts.KEY_QUOTECURRENCY]
                product_id: str = self.getProductIdFromCoinAndQuoteCurrency(coin, quote_currency)
                if not interesting_product_id_list or product_id in interesting_product_id_list:
                    delisted_product_ids.append(product_id)

        delisted_product_ids_str = '\n' + '\n'.join(delisted_product_ids)
        logging.info(f'{len(delisted_product_ids)} delisted products found: {delisted_product_ids_str}')
        return delisted_product_ids

    def downloadAndWriteData(self, product_id: str, timeframe: str, filename: str, is_delisted: bool) -> bool:
        if not self.validateTimeframeStr(timeframe):
            logging.error(f'Invalid timeframe:{timeframe} for ProductID:{product_id}. Skipping...')
            return False

        candle_type: str = self.getCandleTypeFromTimeframeStr(timeframe)
        granularity: int = self.getNumSecondsFromTimeframeStr(timeframe)
        min_req_start_time: int = self.getMinReqStartTime(filename)
        candles: list[list] = []
        request_url = self.api_url + 'api/v1/market/candles'
        num_empty_responses: int = 0
        req_end_time: int = int(granularity * int(time.time() / granularity + 1))
        logging.info(f'Starting download of {timeframe} candles for {product_id} to {filename}.'
                     f' minReqStartTime:{min_req_start_time}')
        loop_iteration_number: int = 0
        while num_empty_responses < 3 and req_end_time > min_req_start_time:
            loop_iteration_number += 1
            req_start_time: int = req_end_time - granularity * self.max_candles_per_api_request
            req_start_time = max(min_req_start_time, req_start_time)

            if loop_iteration_number == 1 and min_req_start_time == 0:
                req_start_time = 0
                params: dict[str, str] = {
                    'symbol': product_id,
                    'type': candle_type
                }
            else:
                params = {
                    'symbol': product_id,
                    'type': candle_type,
                    'startAt': str(int(req_start_time)),
                    'endAt': str(int(req_end_time))
                }

            r = self.request_handler.get(request_url, params)
            r_json: list[list] = r.json()[consts.KEY_DATA]

            if loop_iteration_number == 1 and len(r_json) > 0:
                if min_req_start_time == 0 or is_delisted:
                    req_start_time = self.getDateTimestampFromLine(','.join(str(x) for x in r_json[-1]))

                if is_delisted and min_req_start_time != 0:
                    latest_candle_str: str = ','.join(str(e) for e in r_json[0])
                    raw_last_line: str = self.getLastNonBlankLineFromFile(filename)

                    # also create float arrays in case one of the lines has a number like 1.0 instead of 1 etc
                    latest_candle_float_arr: list[float] = [float(x) for x in r_json[0]]
                    raw_last_line_arr: list[float] = [float(x) for x in raw_last_line.split(',')]

                    if latest_candle_str == raw_last_line or latest_candle_float_arr == raw_last_line_arr:
                        # This code will only be reached if a request is sent on a delisted product
                        # and there is an up to date existing market data file
                        logging.info(f'Nothing to update for delisted product:{product_id}. Skipping file:{filename}')
                        return True

            req_end_time = req_start_time
            if len(r_json) == 0:
                num_empty_responses += 1
                logging.info(f'Received blank response. numEmptyResponses:{num_empty_responses}')
                # If this was the first request sent to get latest candles then find where the
                # data for that instrument stopped being broadcast (can happen with delisted instruments)
                if req_start_time == 0:
                    req_end_time = self.findCloseTimestampOfLatestAvailableData(product_id, request_url)
                continue

            candles += r_json
            num_empty_responses = 0
            earliest_timestamp = self.getDateTimestampFromLine(','.join(str(x) for x in r_json[-1]))
            latest_timestamp = self.getDateTimestampFromLine(','.join(str(x) for x in r_json[0]))
            logging.info(f'URL:{r.url} NumCandlesReceived:{len(r_json)}'
                         f' EarliestTimestamp:{earliest_timestamp} ({datetime.fromtimestamp(earliest_timestamp)})'
                         f' LatestTimestamp:{latest_timestamp} ({datetime.fromtimestamp(latest_timestamp)})')

        return self.writeToCsv(candles[::-1], filename)

    @staticmethod
    def validateTimeframeStr(timeframe: str) -> bool:
        valid_timeframes: set[str] = {'1m', '3m', '5m', '15m', '30m', '1h', '2h',
                                      '4h', '6h', '8h', '12h', '1d', '1w'}

        if timeframe not in valid_timeframes:
            logging.error(f'Unsupported timeframe:{timeframe}. Investigate and fix...')
            return False

        return True

    @staticmethod
    def getNumSecondsFromTimeframeStr(timeframe: str) -> int:
        if not kucoinMDRecorder.validateTimeframeStr(timeframe):
            os._exit(1)

        match timeframe:
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
                logging.error(f'Serious ERROR. Unsupported timeframe:{timeframe}. Investigate and fix...')
                os._exit(1)

    @staticmethod
    def getCandleTypeFromTimeframeStr(timeframe: str) -> str:
        if not kucoinMDRecorder.validateTimeframeStr(timeframe):
            os._exit(1)

        match timeframe:
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
                logging.error(f'Serious ERROR. Unsupported timeframe:{timeframe}. Investigate and fix...')
                os._exit(1)

    def getMinReqStartTime(self, filename: str) -> int:
        file_exists: bool = os.path.isfile(filename)
        if self.write_new_files or not file_exists:
            return 0

        min_req_start_time = self.getLatestTimestampFromFile(filename)
        logging.debug(f'File:{filename} Exists:{file_exists} minReqStartTime:{min_req_start_time}')
        return min_req_start_time

    def findCloseTimestampOfLatestAvailableData(self, product_id: str, request_url: str) -> int:
        latest_data_timestamp: int = 0
        calculated_close_timestamp: int = 0

        params = {
            'symbol': product_id,
            'type': self.getCandleTypeFromTimeframeStr('1d')
        }
        r = self.request_handler.get(request_url, params)
        r_json: list[list] = r.json()[consts.KEY_DATA]
        logging.debug(f'findCloseTimestampOfLatestAvailableData received data:\n{r_json}')
        if len(r_json) > 0:
            latest_data_timestamp = self.getDateTimestampFromLine(','.join(str(x) for x in r_json[0]))
            calculated_close_timestamp = latest_data_timestamp + self.getNumSecondsFromTimeframeStr('1d')
        logging.info(f'findCloseTimestampOfLatestAvailableData returning calculatedCloseTimestamp:{calculated_close_timestamp} '
                     f'for product:{product_id} with observed latestDataTimestamp:{latest_data_timestamp}')
        return calculated_close_timestamp
