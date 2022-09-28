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
            old_candles = pd.read_csv(filename)
            candles = pd.merge(candles, old_candles, how='outer').drop_duplicates(self.key_date)

        candles.sort_values(self.key_date, inplace=True)
        candles.to_csv(filename, index=False)

    def startRecordingProcess(self):
        interestingProductIDs = self.getAllInterestingProductIDs()
        totalNumberOfFiles = len(interestingProductIDs) * len(self.timeframes)
        product_number = 0
        iteration_number = 0
        for productId in interestingProductIDs:
            product_number += 1
            for timeframeStr in self.timeframes:
                iteration_number += 1
                filename = self.getFilenameFromProductIdAndTimeframe(productId, timeframeStr)
                logging.info(f'Writing Data for product:{productId} ({product_number}/{len(interestingProductIDs)}) on '
                             f'{timeframeStr} to {filename} ({iteration_number}/{totalNumberOfFiles})')
                self.downloadAndWriteData(productId, timeframeStr, filename)
                logging.info(f'Finished recording data for {productId} ({product_number}/{len(interestingProductIDs)}) '
                             f'on {timeframeStr} to {filename} ({iteration_number}/{totalNumberOfFiles})')

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
