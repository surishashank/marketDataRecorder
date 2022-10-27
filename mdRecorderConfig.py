from configparser import ConfigParser


class mdRecorderConfig:
    KEY_DUMMYSECTION = 'dummy_section'
    KEY_EXCHANGENAME = 'exchange'
    KEY_APIURL = 'api_url'
    KEY_DATAHEADER = 'data_header'
    KEY_DATEKEY = 'date_key'
    KEY_MAXCANDLESPERREQUEST = 'maxCandlesPerRequest'
    KEY_MAXNUMBEROFAPIREQUESTPERSECOND = 'max_api_requests_per_second'
    KEY_COOLDOWNPERIODINSECONDS = 'cooldown_period_in_seconds'
    KEY_TIMEFRAMES = 'timeframes'
    KEY_INTERESTINGQUOTECURRENCIES = 'interesting_quote_currencies'
    KEY_INTERESTINGCOINS = 'interesting_coins'

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

    def getMaxNumberOfAPIRequestsPerSecond(self):
        maxAPIRequestsPerSec = self.config.getint(self.KEY_DUMMYSECTION, self.KEY_MAXNUMBEROFAPIREQUESTPERSECOND)
        return maxAPIRequestsPerSec

    def getCooldownPeriodInSec(self):
        cooldownPeriodInSec = self.config.getint(self.KEY_DUMMYSECTION, self.KEY_COOLDOWNPERIODINSECONDS)
        return cooldownPeriodInSec

    def getTimeframes(self):
        configStr = self.config.get(self.KEY_DUMMYSECTION, self.KEY_TIMEFRAMES)
        retval = [x.strip() for x in configStr.split(',')]
        return retval

    def getInterestingQuoteCurrencies(self):
        configStr = self.config.get(self.KEY_DUMMYSECTION, self.KEY_INTERESTINGQUOTECURRENCIES, fallback=None)
        if not configStr:
            return []
        retval = [x.strip() for x in configStr.split(',')]
        return retval

    def getInterestingCoins(self):
        configStr = self.config.get(self.KEY_DUMMYSECTION, self.KEY_INTERESTINGCOINS, fallback=None)
        if not configStr:
            return []
        retval = [x.strip() for x in configStr.split(',')]
        return retval
