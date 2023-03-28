# Load external modules
from ext_modules.ext_modules_loader import load_ext_modules
load_ext_modules()

# Regular imports
import argparse
import sys

from MDRecorderBase import MDRecorderBase
from binanceFundingRateRecorder import binanceFundingRateRecorder
from ftxMDRecorder import ftxMDRecorder
from mdRecorderConfig import mdRecorderConfig
from coinbaseMarketDataRecorder import coinbaseMDRecorder
from binanceMDRecorder import binanceMDRecorder
from kucoinMDRecorder import kucoinMDRecorder
import logging


def main():
    parser = argparse.ArgumentParser(description='Download market data files from crypto exchanges')
    requiredArgs = parser.add_argument_group('Required arguments')
    optionalArgs = parser.add_argument_group('Optional arguments')
    cfgOverrideArgs = parser.add_argument_group('Config override options (recommended to be set in config file)')

    parser.add_argument('-d', '--debug', dest='debug', action='store_true', help='run in debug mode (more logging)')

    requiredArgs.add_argument('-c', dest='config', type=str, required=True, metavar='', help='Config file')
    requiredArgs.add_argument('-o', dest='outputDirectory', type=str, required=True, metavar='',
                              help='Directory where market data files are saved')

    optionalArgs.add_argument('-s', dest='interestingBaseCurrencies', type=str, required=False, metavar='',
                              help='List of coins to download market data for (default = all)')
    optionalArgs.add_argument('-q', dest='interestingQuoteCurrencies', type=str, required=False, metavar='',
                              help='List of quote currencies to download market data for (default = all)')
    optionalArgs.add_argument('-x', dest='numThreads', type=int, required=False, metavar='',
                              help='Number of threads to run (default = 5)')
    optionalArgs.add_argument('-n', dest='writeNewFiles', action='store_true', required=False,
                              help='Force write new market data files (even if old ones exist)')
    optionalArgs.add_argument('-z', dest='useParquet', action='store_true', required=False,
                              help='Write to parquet file (default=csv)')

    cfgOverrideArgs.add_argument('-t', dest='timeframes', type=str, required=False, metavar='',
                                 help='Timeframes to download data for (must be set here or in cfg file)')
    cfgOverrideArgs.add_argument('-u', dest='apiURL', type=str, required=False, metavar='',
                                 help='URL of exchange API')
    cfgOverrideArgs.add_argument('-r', dest='header', type=str, required=False, metavar='',
                                 help='List of header column names of received data (in order)')
    cfgOverrideArgs.add_argument('-k', dest='dateKey', type=str, required=False, metavar='',
                                 help='Name of header column that represents date')
    cfgOverrideArgs.add_argument('-m', dest='maxCandlesPerAPIRequest', type=int, required=False, metavar='',
                                 help='Max number of candles that can be returned per API request made')
    cfgOverrideArgs.add_argument('-e', dest='exchangeName', type=str, required=False, metavar='', help='Exchange name')
    cfgOverrideArgs.add_argument('-l', dest='maxAPIRequestsPerSec', type=int, required=False, metavar='',
                                 help='Max number of API requests that can be sent to the exchange per sec')
    cfgOverrideArgs.add_argument('-p', dest='cooldownPeriodInSec', type=int, required=False, metavar='',
                                 help='Cooldown period (in sec) before retrying in case of connection error')

    args = parser.parse_args()
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    config: mdRecorderConfig = mdRecorderConfig(args.config)

    interestingQuoteCurrencies: list[str] = [x.strip() for x in args.interestingQuoteCurrencies.split(',')] if args.interestingQuoteCurrencies else config.getInterestingQuoteCurrencies()
    interestingBaseCurrencies: list[str] = [x.strip() for x in args.interestingBaseCurrencies.split(',')] if args.interestingBaseCurrencies else config.getInterestingCoins()

    apiURL: str = args.apiURL if args.apiURL else config.getAPIURL()
    header: list[str] = [x.strip() for x in args.header.split(',')] if args.header else config.getHeaderColumns()
    dateKey: str = args.dateKey if args.dateKey else config.getDateKey()
    maxCandlesPerAPIRequest: int = args.maxCandlesPerAPIRequest if args.maxCandlesPerAPIRequest else config.getMaxCandlesPerAPIRequest()
    exchangeName: str = args.exchangeName if args.exchangeName else config.getExchangeName()
    maxAPIRequestsPerSec: int = args.maxAPIRequestsPerSec if args.maxAPIRequestsPerSec else config.getMaxNumberOfAPIRequestsPerSecond()
    cooldownPeriodInSec: int = args.cooldownPeriodInSec if args.cooldownPeriodInSec else config.getCooldownPeriodInSec()

    cmd: str = ' '.join(sys.argv)
    logging.info(f'Running command: python {cmd}')

    match exchangeName:
        case 'COINBASE':
            timeframes: list[str] = [x.strip() for x in
                                     args.timeframes.split(',')] if args.timeframes else config.getTimeframes()
            mdRecorder: MDRecorderBase = coinbaseMDRecorder(apiURL, header, dateKey, maxCandlesPerAPIRequest,
                                                            exchangeName, interestingBaseCurrencies,
                                                            interestingQuoteCurrencies, args.outputDirectory,
                                                            timeframes, args.writeNewFiles, maxAPIRequestsPerSec,
                                                            cooldownPeriodInSec, args.useParquet)
        case 'BINANCE':
            timeframes = [x.strip() for x in args.timeframes.split(',')] if args.timeframes else config.getTimeframes()
            mdRecorder = binanceMDRecorder(apiURL, header, dateKey, maxCandlesPerAPIRequest, exchangeName,
                                           interestingBaseCurrencies, interestingQuoteCurrencies, args.outputDirectory,
                                           timeframes, args.writeNewFiles, maxAPIRequestsPerSec, cooldownPeriodInSec,
                                           args.useParquet)
        case 'KUCOIN':
            timeframes = [x.strip() for x in args.timeframes.split(',')] if args.timeframes else config.getTimeframes()
            mdRecorder = kucoinMDRecorder(apiURL, header, dateKey, maxCandlesPerAPIRequest, exchangeName,
                                          interestingBaseCurrencies, interestingQuoteCurrencies, args.outputDirectory,
                                          timeframes, args.writeNewFiles, maxAPIRequestsPerSec, cooldownPeriodInSec,
                                          args.useParquet)
        case 'FTX':
            timeframes = [x.strip() for x in args.timeframes.split(',')] if args.timeframes else config.getTimeframes()
            mdRecorder = ftxMDRecorder(apiURL, header, dateKey, maxCandlesPerAPIRequest, exchangeName,
                                       interestingBaseCurrencies, interestingQuoteCurrencies, args.outputDirectory,
                                       timeframes, args.writeNewFiles, maxAPIRequestsPerSec, cooldownPeriodInSec,
                                       args.useParquet)
        case 'BINANCEFR':
            mdRecorder = binanceFundingRateRecorder(apiURL, header, dateKey, maxCandlesPerAPIRequest, exchangeName,
                                                    interestingBaseCurrencies, interestingQuoteCurrencies,
                                                    args.outputDirectory, args.writeNewFiles, maxAPIRequestsPerSec,
                                                    cooldownPeriodInSec, args.useParquet)
        case _:
            print(f'Exchange:{exchangeName} not supported. Exiting...')
            quit()

    numThreads: int = args.numThreads if args.numThreads else 5
    mdRecorder.startRecordingProcess(numThreads)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(name)s %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logging.Formatter(datefmt='%Y-%m-%d %H:%M:%S')
    main()
