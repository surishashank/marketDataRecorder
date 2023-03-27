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

    def __init__(self, configFilePath: str):
        with open(configFilePath, 'r') as f:
            config_string = '[{}]\n'.format(self.KEY_DUMMYSECTION) + f.read()
        self.config: ConfigParser = ConfigParser()
        self.config.read_string(config_string)

    def getExchangeName(self) -> str:
        exchange_name: str = self.config.get(self.KEY_DUMMYSECTION, self.KEY_EXCHANGENAME)
        return exchange_name

    def getAPIURL(self) -> str:
        api_url: str = self.config.get(self.KEY_DUMMYSECTION, self.KEY_APIURL)
        return api_url

    def getHeaderColumns(self) -> list[str]:
        header: str = self.config.get(self.KEY_DUMMYSECTION, self.KEY_DATAHEADER)
        header_list = [x.strip() for x in header.split(',')]
        return header_list

    def getDateKey(self) -> str:
        date_key: str = self.config.get(self.KEY_DUMMYSECTION, self.KEY_DATEKEY)
        return date_key

    def getMaxCandlesPerAPIRequest(self) -> int:
        max_candles_per_request: int = self.config.getint(self.KEY_DUMMYSECTION, self.KEY_MAXCANDLESPERREQUEST)
        return max_candles_per_request

    def getMaxNumberOfAPIRequestsPerSecond(self) -> int:
        max_api_requests_per_sec: int = self.config.getint(self.KEY_DUMMYSECTION, self.KEY_MAXNUMBEROFAPIREQUESTPERSECOND)
        return max_api_requests_per_sec

    def getCooldownPeriodInSec(self) -> int:
        cooldown_period_in_sec: int = self.config.getint(self.KEY_DUMMYSECTION, self.KEY_COOLDOWNPERIODINSECONDS)
        return cooldown_period_in_sec

    def getTimeframes(self) -> list[str]:
        config_str: str = self.config.get(self.KEY_DUMMYSECTION, self.KEY_TIMEFRAMES)
        retval: list[str] = [x.strip() for x in config_str.split(',')]
        return retval

    def getInterestingQuoteCurrencies(self) -> list[str]:
        config_str: str | None = self.config.get(self.KEY_DUMMYSECTION, self.KEY_INTERESTINGQUOTECURRENCIES,
                                                 fallback=None)
        if config_str is None:
            return []
        retval: list[str] = [x.strip() for x in config_str.split(',')]
        return retval

    def getInterestingCoins(self) -> list[str]:
        config_str: str | None = self.config.get(self.KEY_DUMMYSECTION, self.KEY_INTERESTINGCOINS, fallback=None)
        if config_str is None:
            return []
        retval: list[str] = [x.strip() for x in config_str.split(',')]
        return retval
