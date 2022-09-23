from configparser import ConfigParser

class mdRecorderConfig:
    KEY_DUMMYSECTION = 'dummy_section'
    KEY_EXCHANGENAME = 'exchange'
    KEY_APIURL = 'api_url'
    KEY_DATAHEADER = 'data_header'
    KEY_DATEKEY = 'date_key'
    KEY_MAXCANDLESPERREQUEST = 'maxCandlesPerRequest'

    def __init__(self, configFilePath):
        with open(configFilePath, 'r') as f:
            config_string = '[{}]\n'.format(self.KEY_DUMMYSECTION) + f.read()
        self.config = ConfigParser()
        self.config.optionxform = str
        self.config.read_string(config_string)

    def getExchangeName(self):
        exchangeName = self.config.get(self.KEY_DUMMYSECTION, self.KEY_EXCHANGENAME)
        return exchangeName

    def getAPIURL(self):
        api_url = self.config.get(self.KEY_DUMMYSECTION, self.KEY_APIURL)
        return api_url

    def getHeaderColumns(self):
        header = self.config.get(self.KEY_DUMMYSECTION, self.KEY_DATAHEADER)
        header_list = [x.strip() for x in header.split(',')]
        return header_list

    def getDateKey(self):
        date_key = self.config.get(self.KEY_DUMMYSECTION, self.KEY_DATEKEY)
        return date_key

    def getMaxCandlesPerAPIRequest(self):
        maxCandlesPerRequest = self.config.getint(self.KEY_DUMMYSECTION, self.KEY_MAXCANDLESPERREQUEST)
        return maxCandlesPerRequest

    def getOrderSize(self):
        orderSize = int(self.config.get(consts.KEY_SECTION_GENERAL, consts.KEY_ORDERSIZE))
        return orderSize

    def getFeePct(self):
        fee = float(self.config.get(consts.KEY_SECTION_GENERAL, consts.KEY_FEE))
        return fee

    def getStartDate(self):
        datestr = self.config.get(consts.KEY_SECTION_GENERAL, consts.KEY_STARTDATE)
        return datetime.strptime(datestr, '%Y%m%d')

    def getEndDate(self):
        datestr = self.config.get(consts.KEY_SECTION_GENERAL, consts.KEY_ENDDATE)
        return datetime.strptime(datestr, '%Y%m%d')

    def getTargetSymbolFilename(self):
        mktDataFile = self.config.get(consts.KEY_SECTION_GENERAL, consts.KEY_TARGETSYMBOLFILE)
        return mktDataFile

    def getMaxOpenPositions(self):
        maxOpenPositions = int(self.config.get(consts.KEY_SECTION_GENERAL, consts.KEY_MAXOPENPOSITIONS))
        return maxOpenPositions

    def isMaxDistBwEntryAndStopLossEnabled(self):
        isEnabled = bool(self.config.get(consts.KEY_SECTION_GENERAL, consts.KEY_MAXDISTBWENTRYANDSTOPLOSS,
                                         fallback=False))
        return isEnabled

    def getMaxDistBwEntryAndStopLossStr(self):
        maxDistBwEntryAndStopLossStr = self.config.get(consts.KEY_SECTION_GENERAL, consts.KEY_MAXDISTBWENTRYANDSTOPLOSS)
        return maxDistBwEntryAndStopLossStr

    def getMaxDistBwEntryAndStopLossMultiplier(self):
        if not self.isMaxDistBwEntryAndStopLossEnabled():
            return 0

        maxDistBwEntryAndStopLossStr = self.getMaxDistBwEntryAndStopLossStr()
        parts = [x.strip() for x in str.split(maxDistBwEntryAndStopLossStr, ';')]
        multiplierAndIndicator = parts[0]
        parts_multiplierAndIndicator = [x.strip() for x in str.split(multiplierAndIndicator, ':')]
        multiplier = float(parts_multiplierAndIndicator[0])
        return multiplier

    def getMaxDistBwEntryAndStopLossIndicator(self):
        if not self.isMaxDistBwEntryAndStopLossEnabled():
            return ''

        maxDistBwEntryAndStopLossStr = self.getMaxDistBwEntryAndStopLossStr()
        parts = [x.strip() for x in str.split(maxDistBwEntryAndStopLossStr, ';')]
        multiplierAndIndicator = parts[0]
        parts_multiplierAndIndicator = [x.strip() for x in str.split(multiplierAndIndicator, ':')]
        indicator = ':'.join(parts_multiplierAndIndicator[1:])
        return indicator

    def getMaxDistBwEntryAndStopLossHardCapPct(self):
        if not self.isMaxDistBwEntryAndStopLossEnabled():
            return 0

        maxDistBwEntryAndStopLossStr = self.getMaxDistBwEntryAndStopLossStr()
        parts = [x.strip() for x in str.split(maxDistBwEntryAndStopLossStr, ';')]
        hardCapPct = float(parts[1])
        return hardCapPct

    def getMoveUpLimitBuysWithExitMAs(self):
        moveUpLimitBuysWithMAs = self.config.getboolean(consts.KEY_SECTION_GENERAL, consts.KEY_MOVEUPLIMITBUYSWITHEXITMAS)
        return moveUpLimitBuysWithMAs

    def getCancelOpenBuyIfLimitPriceBelowStopLoss(self):
        cancelOpenBuyIfLimitPriceBelowStopLoss = self.config.getboolean(consts.KEY_SECTION_GENERAL, consts.KEY_CANCELOPENBUYIFLIMITPRICEBELOWSL)
        return cancelOpenBuyIfLimitPriceBelowStopLoss

    def getFileNameFromSection(self, section):
        mktDataFile = self.config.get(section, consts.KEY_FILENAME)
        return mktDataFile

    def getTagFromSection(self, condition):
        tag = self.config.get(condition, consts.KEY_TAG)
        return tag

    def getNumFalseAllowedFromSection(self, section):
        return int(self.config.get(section, consts.KEY_NUMFALSEALLOWED))

    def getConditionSubsectionsFromSection(self, section):
        retval = [x.strip() for x in str.split(self.config.get(section, consts.KEY_CONDITION_SUBSECTIONS),',')]
        return retval

    def getConditionsFromSection(self, section):
        signals = [x.strip() for x in str.split(self.config.get(section, consts.KEY_CONDITIONS),',')]
        return signals

    def getColumnMappingDict(self):
        SECTION_NAME = 'ColumnMapping'
        if not self.config.has_section(SECTION_NAME):
            print(f"Config file missing section \'{SECTION_NAME}\'. Exiting...")
            quit()

        columnMappingDict = dict(self.config.items(SECTION_NAME))
        return columnMappingDict

    def validateColumns(self, columns):
        expectedColumns = self.getColumnMappingDict().keys()
        missingColumns = []
        for x in expectedColumns:
            if x not in columns:
                missingColumns.append(x)

        if len(missingColumns) > 0:
            print("ERROR. Market data file is missing the following columns:", missingColumns, "Exiting...")
            quit()

    def removeExtraColumnsFromDataFrame(self, dataFrame):
        columnMappingDict = self.getColumnMappingDict()
        self.validateColumns(dataFrame.columns.values)
        unwantedColumns = []
        for col in dataFrame.columns.values:
            if col not in columnMappingDict.keys():
                unwantedColumns.append(col)
        dataFrame.drop(columns = unwantedColumns, inplace = True)
        return dataFrame

    def renameColumnsForInternalUse(self, dataFrame):
        columnMappingDict = self.getColumnMappingDict()
        dataFrame.rename(columns = columnMappingDict, inplace=True)
        return dataFrame

    def getProfitTargetsStr(self):
        return self.config.get(consts.KEY_SECTION_ENTRYCONDITIONS, consts.KEY_PROFITTARGETS)