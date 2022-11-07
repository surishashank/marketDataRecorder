import math
import os
import pandas as pd
import logging
from requestHandler import requestHandler


class MDRecorderBase:
    def __init__(self, api_url, header, key_date, maxCandlesPerAPIRequest, exchangeName, interestingBaseCurrencies,
                 interestingQuoteCurrencies, outputDirectory, timeframes, writeNewFiles, maxAPIRequestsPerSec,
                 cooldownPeriodInSec):
        self.api_url = api_url
        self.header = header
        self.key_date = key_date
        self.maxCandlesPerAPIRequest = maxCandlesPerAPIRequest
        self.exchangeName = exchangeName

        self.interestingBaseCurrencies = interestingBaseCurrencies
        self.interestingQuoteCurrencies = interestingQuoteCurrencies
        self.outputDirectory = outputDirectory
        self.timeframes = timeframes
        self.writeNewFiles = writeNewFiles
        self.requestHandler = requestHandler(maxAPIRequestsPerSec, cooldownPeriodInSec)

    @staticmethod
    def getProductIdFromCoinAndQuoteCurrency(coinName, quoteCurrency):
        return f'{coinName}-{quoteCurrency}'

    def getFilenameFromProductIdAndTimeframe(self, productId, timeframe):
        fileName = os.path.join(self.outputDirectory,
                                '{}_{}_{}.csv'.format(self.exchangeName, productId, timeframe))
        return fileName

    def getLatestTimestampFromFile(self, filename):
        if not os.path.isfile(filename) or os.path.getsize(filename) == 0:
            return 0

        all_candles = pd.read_csv(filename)
        latestTimestamp = all_candles[self.key_date].max()
        if math.isnan(latestTimestamp):
            latestTimestamp = 0
        return latestTimestamp

    @staticmethod
    def getLastNonBlankLineFromFile(filename):
        lastLine = None
        for line in open(filename):
            if line.strip():
                lastLine = line.strip()
        return lastLine

    def getDateTimestampFromLine(self, lineStr):
        if not lineStr:
            return 0
        elements_arr = [x.strip() for x in lineStr.split(',')]
        df = pd.DataFrame([elements_arr], columns=self.header)
        return int(df.at[0, self.key_date])

    def writeToCsv(self, data, filename):
        candles = pd.DataFrame(data, columns=self.header).drop_duplicates(self.key_date)
        if not self.writeNewFiles and os.path.isfile(filename):
            try:
                old_candles = pd.read_csv(filename, dtype=candles.dtypes.to_dict())
                candles = pd.merge(candles, old_candles, how='outer').drop_duplicates(self.key_date)
            except Exception as e:
                old_candles = pd.read_csv(filename)
                type1 = candles.dtypes
                type2 = old_candles.dtypes
                logging.exception(f'Caught exception "{e}" while reading/writing file {filename}.\n'
                                  f'Type1:{type1}\nType2:{type2}')
                try:
                    logging.exception(f'Trying to convert new candles to Type2 instead.')
                    for key, value in type2.items():
                        candles[key] = candles[key].astype(value)
                    candles = pd.merge(candles, old_candles, how='outer').drop_duplicates(self.key_date)
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

    def startRecordingProcess(self):
        interestingProductIDs = self.getAllInterestingProductIDs()
        delistedProductIDs = self.getAllDelistedProductIDs(interestingProductIDs)
        totalNumberOfFiles = len(interestingProductIDs) * len(self.timeframes)
        product_number = 0
        iteration_number = 0
        failed_iterations = []
        for productId in interestingProductIDs:
            product_number += 1
            for timeframeStr in self.timeframes:
                iteration_number += 1
                filename = self.getFilenameFromProductIdAndTimeframe(productId, timeframeStr)
                is_delisted = productId in delistedProductIDs

                logging.info(f'Writing data for product:{productId} ({product_number}/{len(interestingProductIDs)}) on '
                             f'{timeframeStr} to {filename} ({iteration_number}/{totalNumberOfFiles})')
                success = self.downloadAndWriteData(productId, timeframeStr, filename, is_delisted)
                log_message_prefix = 'Successfully recorded data for'
                if not success:
                    log_message_prefix = 'Failed to record data for'
                    failed_iterations.append(filename)
                logging.info(f'{log_message_prefix} {productId} ({product_number}/{len(interestingProductIDs)}) '
                             f'on {timeframeStr} to {filename} ({iteration_number}/{totalNumberOfFiles})')

        num_failed_iterations = len(failed_iterations)
        num_successful_iterations = totalNumberOfFiles - num_failed_iterations
        logging.info(f'Recording Process Completed. TotalIterations:{totalNumberOfFiles} '
                     f'NumSuccesses:{num_successful_iterations} NumFailures:{num_failed_iterations}')
        if num_failed_iterations > 0:
            print_str = '\n' + '\n'.join(failed_iterations)
            logging.info(f'Files with errors:{print_str}')

    def isInterestingQuoteCurrency(self, quoteCurrency):
        if not self.interestingQuoteCurrencies or len(self.interestingQuoteCurrencies) == 0:
            return True
        if quoteCurrency in self.interestingQuoteCurrencies:
            return True
        return False

    def isInterestingBaseCurrency(self, baseCurrency):
        if not self.interestingBaseCurrencies or len(self.interestingBaseCurrencies) == 0:
            return True
        if baseCurrency in self.interestingBaseCurrencies:
            return True
        return False

    def getAllInterestingProductIDs(self):
        raise NotImplementedError('ERROR: Method getAllInterestingProductIDs must be defined in child class!')

    def getAllDelistedProductIDs(self, interesting_product_id_list):
        raise NotImplementedError('ERROR: Method getAllDelistedProductIDs must be defined in child class!')

    def downloadAndWriteData(self, productId, timeframeStr, filename, isDelisted):
        raise NotImplementedError('ERROR: Method downloadAndWriteData must be defined in child class!')
