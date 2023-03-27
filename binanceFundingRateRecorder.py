import logging
import os.path
import random
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
    def __init__(self, api_url: str, header: list[str], key_date: str, max_candles_per_api_request: int,
                 exchange_name: str, interesting_base_currencies: list[str], interesting_quote_currencies: list[str],
                 output_directory: str, write_new_files: bool, max_api_requests_per_sec: int,
                 cooldown_period_in_sec: int):
        MDRecorderBase.__init__(self, api_url, header, key_date, max_candles_per_api_request, exchange_name,
                                interesting_base_currencies, interesting_quote_currencies, output_directory,
                                [consts.BINANCE_FUNDINGRATE_TIMEFRAME], write_new_files, max_api_requests_per_sec,
                                cooldown_period_in_sec)

    def getAllInterestingProductIDs(self) -> list[str]:
        request_url = self.api_url + 'exchangeInfo'
        r = self.request_handler.get(request_url)

        interesting_product_ids: list[str] = []
        symbol_info_list: list[dict] = r.json()[consts.KEY_SYMBOLS]
        for symbol_info in symbol_info_list:
            symbol: str = symbol_info[consts.KEY_BASEASSET]
            quote_currency: str = symbol_info[consts.KEY_QUOTEASSET]
            isPerpetualFuture: bool = symbol_info[consts.KEY_CONTRACTTYPE] == consts.KEY_CONTRACTTYPE_PERP
            if isPerpetualFuture and self.isInterestingQuoteCurrency(quote_currency) and self.isInterestingBaseCurrency(symbol):
                product_id: str = self.getProductIdFromCoinAndQuoteCurrency(symbol, quote_currency)
                interesting_product_ids.append(product_id)

        random.shuffle(interesting_product_ids)
        product_ids_str = '\n' + '\n'.join(interesting_product_ids)
        logging.info(
            f'{len(interesting_product_ids)}/{len(symbol_info_list)} interesting products found:{product_ids_str}')
        return interesting_product_ids

    def getAllDelistedProductIDs(self, interesting_product_id_list: list[str]) -> list[str]:
        request_url = self.api_url + 'exchangeInfo'
        r = self.request_handler.get(request_url)

        delisted_product_ids: list[str] = []
        symbol_info_list: list[dict] = r.json()[consts.KEY_SYMBOLS]
        for symbol_info in symbol_info_list:
            trading_status: str = symbol_info[consts.KEY_TRADINGSTATUS]
            if trading_status != consts.KEY_TRADINGSTATUS_TRADING:
                symbol: str = symbol_info[consts.KEY_BASEASSET]
                quote_currency: str = symbol_info[consts.KEY_QUOTEASSET]
                product_id: str = self.getProductIdFromCoinAndQuoteCurrency(symbol, quote_currency)
                if not interesting_product_id_list or product_id in interesting_product_id_list:
                    delisted_product_ids.append(product_id)

        delisted_product_ids_str = '\n' + '\n'.join(delisted_product_ids)
        logging.info(f'{len(delisted_product_ids)} delisted products found: {delisted_product_ids_str}')
        return delisted_product_ids

    def downloadAndWriteData(self, product_id: str, timeframe: str, filename: str, is_delisted: bool) -> bool:
        if not self.validateTimeframeStr(timeframe):
            logging.error(f'Invalid timeframe:{timeframe} for ProductID:{product_id}. Skipping...')
            return False

        granularity: int = self.getNumMillisecondsFromTimeframe(timeframe)
        min_req_start_time: int = self.getMinReqStartTime(filename)
        candles: list[list[str | int]] = []
        request_url = self.api_url + 'fundingRate'
        req_end_time: int = int(granularity * int(time.time()*1000 / granularity))
        logging.info(f'Starting download of funding rates for {product_id} to {filename}. '
                     f'minReqStartTime:{min_req_start_time}')
        loop_iteration_number = 0
        while req_end_time > min_req_start_time:
            loop_iteration_number += 1
            req_start_time = req_end_time - granularity * self.max_candles_per_api_request
            req_start_time = max(min_req_start_time, req_start_time)

            params: dict[str, str] = {
                'symbol': product_id.replace('-', ''),
                'startTime': str(int(req_start_time)),
                'endTime': str(int(req_end_time)),
                'limit': str(int(self.max_candles_per_api_request))
            }
            if loop_iteration_number == 1 and is_delisted:
                params = {
                    'symbol': product_id.replace('-', ''),
                    'limit': str(int(self.max_candles_per_api_request))
                }
            r = self.request_handler.get(request_url, params)

            r_json: list[dict] = r.json()
            if len(r_json) == 0:
                logging.info(f'Received blank response. Breaking out of loop')
                break

            new_candles_arr: list[list[str | int]] = []
            for entry in r_json:
                funding_time: int = entry[consts.KEY_FUNDINGTIME]
                funding_rate: str = entry[consts.KEY_FUNDINGRATE]
                new_candles_arr.append([funding_time, funding_rate])

            if loop_iteration_number == 1 and is_delisted:
                req_start_time = self.getDateTimestampFromLine(','.join(str(x) for x in new_candles_arr[0]))
                # if file exists, check if it is already up to date
                if min_req_start_time != 0:
                    latest_candle_str = ','.join(str(x) for x in new_candles_arr[-1])
                    raw_last_line: str = self.getLastNonBlankLineFromFile(filename)

                    # also create float arrays in case one of the lines has a number like 1.0 instead of 1 etc
                    latest_candle_float_arr: list[float] = [float(x) for x in latest_candle_str.split(',')]
                    rawLastLineArr: list[float] = [float(x) for x in raw_last_line.split(',')]

                    if latest_candle_str == raw_last_line or latest_candle_float_arr == rawLastLineArr:
                        # This code will only be reached if a request is sent on a delisted product
                        # and there is an up to date existing market data file
                        logging.info(f'Nothing to update for delisted product:{product_id}. Skipping file:{filename}')
                        return True

            req_end_time = int(req_start_time/1000)*1000

            candles += new_candles_arr
            earliest_timestamp: int = self.getDateTimestampFromLine(','.join(str(x) for x in new_candles_arr[0]))
            latest_timestamp: int = self.getDateTimestampFromLine(','.join(str(x) for x in new_candles_arr[-1]))
            logging.info(f'URL:{r.url} NumCandlesReceived:{len(new_candles_arr)} '
                         f'EarliestTimestamp:{earliest_timestamp} ({datetime.fromtimestamp(earliest_timestamp / 1000)})'
                         f' LatestTimestamp:{latest_timestamp} ({datetime.fromtimestamp(latest_timestamp / 1000)})')

        if loop_iteration_number == 0 and len(candles) == 0 and min_req_start_time != 0:
            logging.info(f'Data already up to date for {filename}')
            return True

        return self.writeToCsv(candles, filename)

    # Available timeframes: 8h
    @staticmethod
    def validateTimeframeStr(timeframe: str) -> bool:
        valid_timeframes: set[str] = {'8h'}

        if timeframe not in valid_timeframes:
            logging.error(f'Unsupported timeframe:{timeframe}. Investigate and fix...')
            return False

        return True

    @staticmethod
    def getNumMillisecondsFromTimeframe(timeframe: str) -> int:
        if not binanceFundingRateRecorder.validateTimeframeStr(timeframe):
            os._exit(1)

        match timeframe:
            case '8h':
                return 1000 * 60 * 60 * 8
            case _:  # this should never happen because we validate the parameter beforehand
                logging.error(f'Serious ERROR. Unsupported timeframe:{timeframe}. Investigate and fix...')
                os._exit(1)

    def getMinReqStartTime(self, filename: str) -> int:
        file_exists = os.path.isfile(filename)
        if self.write_new_files or not file_exists:
            return 0

        return self.getLatestTimestampFromFile(filename)
