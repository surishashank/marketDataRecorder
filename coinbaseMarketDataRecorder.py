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
    def __init__(self, api_url: str, header: list[str], key_date: str, max_candles_per_api_request: int,
                 exchange_name: str, interesting_base_currencies: list[str], interesting_quote_currencies: list[str],
                 output_directory: str, timeframes: list[str], write_new_files: bool, max_api_requests_per_sec: int,
                 cooldown_period_in_sec: int):
        MDRecorderBase.__init__(self, api_url, header, key_date, max_candles_per_api_request, exchange_name,
                                interesting_base_currencies, interesting_quote_currencies, output_directory, timeframes,
                                write_new_files, max_api_requests_per_sec, cooldown_period_in_sec)

    def getAllInterestingProductIDs(self) -> list[str]:
        request_url = self.api_url + 'products'
        r = self.requestHandler.get(request_url)

        interesting_product_ids: list[str] = []
        response_list: list[dict[str, str]] = r.json()
        for response in response_list:
            quoteCurrency: str = response[consts.KEY_QUOTECURRENCY]
            symbol: str = response[consts.KEY_BASECURRENCY]
            if self.isInterestingQuoteCurrency(quoteCurrency) and self.isInterestingBaseCurrency(symbol):
                # Get product_id from response instead of from getProductIdFromCoinAndQuoteCurrency because
                # coinbase has cases like BTCAUCTION-USD where symbol = BTC & quoteCurrency = USD for both
                # BTCUSD and BTCAUCTION-USD
                product_id: str = response[consts.KEY_PRODUCTID]
                interesting_product_ids.append(product_id)

        random.shuffle(interesting_product_ids)
        product_ids_str = '\n' + '\n'.join(interesting_product_ids)
        logging.info(
            f'{len(interesting_product_ids)}/{len(response_list)} interesting products found: {product_ids_str}')
        return interesting_product_ids

    def getAllDelistedProductIDs(self, interesting_product_id_list: list[str]) -> list[str]:
        request_url = self.api_url + 'products'
        r = self.requestHandler.get(request_url)
        delisted_product_ids: list[str] = []
        response_list: list[dict[str, str]] = r.json()
        for response in response_list:
            trading_status: str = response[consts.KEY_TRADINGSTATUS]
            if trading_status == consts.KEY_TRADINGSTATUS_DELISTED:
                # Get product_id from response instead of from getProductIdFromCoinAndQuoteCurrency because
                # coinbase has cases like BTCAUCTION-USD where symbol = BTC & quoteCurrency = USD for both
                # BTCUSD and BTCAUCTION-USD
                product_id: str = response[consts.KEY_PRODUCTID]
                if not interesting_product_id_list or product_id in interesting_product_id_list:
                    delisted_product_ids.append(product_id)

        delisted_product_ids_str = '\n' + '\n'.join(delisted_product_ids)
        logging.info(f'{len(delisted_product_ids)} delisted products found: {delisted_product_ids_str}')
        return delisted_product_ids

    def downloadAndWriteData(self, product_id: str, timeframe: str, filename: str, is_delisted: bool) -> bool:
        granularity: int = self.getGranularityFromTimeframeStr(timeframe)
        min_req_start_time: int = self.getMinReqStartTime(filename)
        candles: list[list] = []
        num_empty_responses: int = 0
        request_url = self.api_url + f'products/{product_id}/candles'
        req_end_time: int = int(granularity * int(time.time() / granularity))

        logging.info(f'Starting download of {timeframe} candles for {product_id} to {filename}.'
                     f' minReqStartTime:{min_req_start_time}')
        loop_iteration_number: int = 0
        while num_empty_responses < 3 and req_end_time >= min_req_start_time:
            loop_iteration_number += 1
            req_start_time: int = req_end_time - granularity * (self.maxCandlesPerAPIRequest - 1)
            req_start_time = max(min_req_start_time, req_start_time)
            if loop_iteration_number == 1 and (min_req_start_time == 0 or is_delisted):
                params: dict[str, str] = {
                    'granularity': str(int(granularity))
                }
            else:
                params = {
                    'granularity': str(int(granularity)),
                    'start': str(int(req_start_time)),
                    'end': str(int(req_end_time))
                }

            r = self.requestHandler.get(request_url, params)
            r_json: list[list] = r.json()

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

            req_end_time = req_start_time - granularity
            if len(r_json) == 0:
                num_empty_responses += 1
                logging.info(f'Received empty response. numEmptyResponses:{num_empty_responses}')
                continue

            candles += r_json
            num_empty_responses = 0
            earliest_timestamp: int = self.getDateTimestampFromLine(','.join(str(x) for x in r_json[-1]))
            latest_timestamp: int = self.getDateTimestampFromLine(','.join(str(x) for x in r_json[0]))
            logging.info(f'URL:{r.url} NumCandlesReceived:{len(r_json)}'
                         f' EarliestTimestamp:{earliest_timestamp} ({datetime.fromtimestamp(earliest_timestamp)})'
                         f' LatestTimestamp:{latest_timestamp} ({datetime.fromtimestamp(latest_timestamp)})')

        return self.writeToCsv(candles[::-1], filename)

    def getMinReqStartTime(self, filename: str) -> int:
        file_exists: bool = os.path.isfile(filename)
        if self.writeNewFiles or not file_exists:
            return 0

        min_req_start_time: int = self.getLatestTimestampFromFile(filename)
        logging.debug(f'File:{filename} Exists:{file_exists} minReqStartTime:{min_req_start_time}')
        return min_req_start_time

    @staticmethod
    def getGranularityFromTimeframeStr(timeframe: str) -> int:
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
                logging.error(f'Unsupported timeframe:{timeframe}. Investigate and fix...')
                os._exit(1)
