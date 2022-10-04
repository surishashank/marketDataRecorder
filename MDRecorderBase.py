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

    def getFilenameFromProductIdAndTimeframe(self, productId, timeframe):
        fileName = os.path.join(self.outputDirectory,
                                '{}_{}_{}.csv'.format(self.exchangeName, productId, timeframe))
        return fileName

    def getLatestTimestampFromFile(self, filename):
        all_candles = pd.read_csv(filename)
        latestTimestamp = all_candles[self.key_date].max()
        if math.isnan(latestTimestamp):
            latestTimestamp = 0
        return latestTimestamp

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

        candles.sort_values(self.key_date, inplace=True)
        candles.to_csv(filename, index=False)
        return True

    def startRecordingProcess(self):
        interestingProductIDs = self.getAllInterestingProductIDs()
        totalNumberOfFiles = len(interestingProductIDs) * len(self.timeframes)
        product_number = 0
        iteration_number = 0
        failed_iterations = []
        for productId in interestingProductIDs:
            product_number += 1
            for timeframeStr in self.timeframes:
                iteration_number += 1
                filename = self.getFilenameFromProductIdAndTimeframe(productId, timeframeStr)
                logging.info(f'Writing data for product:{productId} ({product_number}/{len(interestingProductIDs)}) on '
                             f'{timeframeStr} to {filename} ({iteration_number}/{totalNumberOfFiles})')
                success = self.downloadAndWriteData(productId, timeframeStr, filename)
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

    def downloadAndWriteData(self, productId, timeframeStr, filename):
        raise NotImplementedError('ERROR: Method downloadAndWriteData must be defined in child class!')
