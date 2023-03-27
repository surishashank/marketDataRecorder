import concurrent.futures
import math
import os
import pandas as pd
import logging
from totalRequestHandler import totalRequestHandler as requestHandler


class MDRecorderBase:
    def __init__(self, api_url: str, header: list[str], key_date: str, max_candles_per_api_request: int,
                 exchange_name: str, interesting_base_currencies: list[str], interesting_quote_currencies: list[str],
                 output_directory: str, timeframes: list[str], write_new_files: bool, max_api_requests_per_sec: int,
                 cooldown_period_in_sec: int):
        self.api_url: str = api_url
        self.header: list[str] = header
        self.key_date: str = key_date
        self.max_candles_per_api_request: int = max_candles_per_api_request
        self.exchange_name: str = exchange_name

        self.interesting_base_currencies: list[str] = interesting_base_currencies
        self.interesting_quote_currencies: list[str] = interesting_quote_currencies
        self.output_directory: str = output_directory
        self.timeframes: list[str] = timeframes
        self.write_new_files: bool = write_new_files
        self.request_handler: requestHandler = requestHandler(max_api_requests_per_sec, 1, cooldown_period_in_sec)

    @staticmethod
    def getProductIdFromCoinAndQuoteCurrency(coin_name: str, quote_currency: str) -> str:
        return f'{coin_name}-{quote_currency}'

    def getFilenameFromProductIdAndTimeframe(self, product_id: str, timeframe: str) -> str:
        file_name: str = os.path.join(self.output_directory,
                                      '{}_{}_{}.csv'.format(self.exchange_name, product_id, timeframe))
        return file_name

    def getLatestTimestampFromFile(self, filename: str) -> int:
        if not os.path.isfile(filename) or os.path.getsize(filename) == 0:
            return 0

        all_candles: pd.DataFrame = pd.read_csv(filename)
        latest_timestamp: int = all_candles[self.key_date].max()
        if math.isnan(latest_timestamp):
            latest_timestamp = 0
        return latest_timestamp

    @staticmethod
    def getLastNonBlankLineFromFile(filename: str) -> str:
        last_line: str = ''
        for line in open(filename):
            if line.strip():
                last_line = line.strip()
        return last_line

    def getDateTimestampFromLine(self, line: str) -> int:
        if not line:
            return 0
        elements_arr: list[str] = [x.strip() for x in line.split(',')]
        df: pd.DataFrame = pd.DataFrame([elements_arr], columns=self.header)
        return int(df.at[0, self.key_date])

    def writeToCsv(self, data: list[list], filename: str) -> bool:
        candles: pd.DataFrame = pd.DataFrame(data, columns=self.header).drop_duplicates(self.key_date)
        if not self.write_new_files and os.path.isfile(filename):
            try:
                old_candles: pd.DataFrame = pd.read_csv(filename, dtype=candles.dtypes.to_dict())
                candles = pd.merge(candles, old_candles, how='outer').drop_duplicates(self.key_date)
            except Exception as e:
                old_candles = pd.read_csv(filename)
                type1: pd.Series = candles.dtypes
                type2: pd.Series = old_candles.dtypes
                logging.exception(f'Caught exception "{e}" while reading/writing file {filename}.\n'
                                  f'Type1:{type1}\nType2:{type2}')
                try:
                    logging.exception(f'Trying to convert new candles to Type2 instead.')
                    for key, value in type2.items():
                        candles[key] = candles[key].astype(value)
                    candles = pd.merge(candles, old_candles, how='outer').drop_duplicates(self.key_date)
                    logging.info(f'Converted new candles to Type2 successfully')
                except Exception as e:
                    logging.exception(f'Caught exception "{e}" while retrying. Skipping...\n'
                                      f'Type1:{type1}\nType2:{type2}')
                    return False

            # Sanity check of new data (check that all the "old_candles" (except the last one) exist is "candles"
            if len(old_candles.iloc[:-1,:].merge(candles)) == len(old_candles.iloc[:-1,:]):
                logging.info(f'Sanity check passed before rewriting existing data file:{filename}')
            else:
                logging.error(f'Differences found between existing and new candles when writing file:{filename}. '
                              f'Not updating this file. Investigate further.')
                return False

        candles.sort_values(self.key_date, inplace=True)
        candles.to_csv(filename, index=False)
        return True

    def startRecordingProcess(self, max_threads: int) -> None:
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_threads)
        futures: list[concurrent.futures.Future] = []
        logging.info(f'Starting recording process with maxThreads={max_threads}')

        interesting_product_ids: list[str] = list(dict.fromkeys(self.getAllInterestingProductIDs()))  # to remove any duplicates
        delisted_product_ids: list[str] = list(dict.fromkeys(self.getAllDelistedProductIDs(interesting_product_ids)))  # to remove any duplicates
        total_number_of_files: int = len(interesting_product_ids) * len(self.timeframes)
        iteration_number: int = 0
        failed_iterations: list[str] = []
        for product_id in interesting_product_ids:
            is_delisted: bool = product_id in delisted_product_ids
            for timeframe in self.timeframes:
                iteration_number += 1
                futures.append(executor.submit(self.initiateDownloadAndRecord, product_id, timeframe, is_delisted))

        num_successful_iterations: int = 0
        num_failed_iterations: int = 0
        filenum: int = 0
        for future in concurrent.futures.as_completed(futures):
            filenum += 1
            success, filename = future.result()
            if success:
                log_message_prefix = 'Successfully recorded data for'
                num_successful_iterations += 1
            else:
                log_message_prefix = 'Failed to record data for'
                failed_iterations.append(filename)
                num_failed_iterations += 1
            logging.info(f'{log_message_prefix} {filename} ({filenum}/{total_number_of_files})')

        logging.info(f'Recording Process Completed. TotalIterations:{total_number_of_files} '
                     f'NumSuccesses:{num_successful_iterations} NumFailures:{num_failed_iterations}')
        if num_failed_iterations > 0:
            print_str = '\n' + '\n'.join(failed_iterations)
            logging.info(f'Files with errors:{print_str}')

    def initiateDownloadAndRecord(self, product_id: str, timeframe: str, is_delisted: bool) -> tuple[bool, str]:
        filename: str = self.getFilenameFromProductIdAndTimeframe(product_id, timeframe)
        success: bool = self.downloadAndWriteData(product_id, timeframe, filename, is_delisted)
        return success, filename

    def isInterestingQuoteCurrency(self, quote_currency: str) -> bool:
        if not self.interesting_quote_currencies or len(self.interesting_quote_currencies) == 0:
            return True
        if quote_currency in self.interesting_quote_currencies:
            return True
        return False

    def isInterestingBaseCurrency(self, base_currency: str) -> bool:
        if not self.interesting_base_currencies or len(self.interesting_base_currencies) == 0:
            return True
        if base_currency in self.interesting_base_currencies:
            return True
        return False

    def getAllInterestingProductIDs(self) -> list[str]:
        raise NotImplementedError('ERROR: Method getAllInterestingProductIDs must be defined in child class!')

    def getAllDelistedProductIDs(self, interesting_product_id_list: list[str]) -> list[str]:
        raise NotImplementedError('ERROR: Method getAllDelistedProductIDs must be defined in child class!')

    def downloadAndWriteData(self, product_id: str, timeframe: str, filename: str, is_delisted: bool) -> bool:
        raise NotImplementedError('ERROR: Method downloadAndWriteData must be defined in child class!')
