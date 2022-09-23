import os
import pandas as pd


class MDRecorderBase:
    def __init__(self, api_url, header, key_date, maxCandlesPerAPIRequest, exchangeName, interestingBaseCurrencies,
                 interestingQuoteCurrencies, outputDirectory, timeframes, writeNewFiles):
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

    def getFilenameFromProductIdAndTimeframe(self, productId, timeframe):
        fileName = os.path.join(self.outputDirectory,
                                '{}_{}_{}.csv'.format(self.exchangeName, productId, timeframe))
        return fileName

    def getLatestTimestampFromFile(self, filename):
        lastLine = None
        numLines = 0
        with open(filename) as f:
            for line in f:
                numLines += 1
                if len(line) > 0:
                    lastLine = line
            if numLines <= 1:
                return 0
        latestTimestamp = self.getDateTimestampFromLine(lastLine)
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
        for productId in interestingProductIDs:
            for timeframeStr in self.timeframes:
                filename = self.getFilenameFromProductIdAndTimeframe(productId, timeframeStr)
                print("Writing Data for product:{} on timeframe:{}".format(productId, timeframeStr))
                self.downloadAndWriteData(productId, timeframeStr, filename)
                print('Successfully recorded data for {} on {}'.format(productId, timeframeStr))

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
