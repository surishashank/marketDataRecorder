import sys
import getopt
from mdRecorderConfig import mdRecorderConfig
from coinbaseMarketDataRecorder import coinbaseMDRecorder
from binanceMDRecorder import binanceMDRecorder
import logging


def main(argv):
    interestingQuoteCurrencies = None
    interestingBaseCurrencies = None
    outputDirectory = None
    timeframe = None
    api_url = None
    header = None
    key_date = None
    maxCandlesPerAPIRequest = None
    exchangeName = None
    writeNewFiles = False
    maxAPIRequestsPerSec = None
    cooldownPeriodInSec = None

    try:
        opts, args = getopt.getopt(argv, "hndq:k:s:o:t:u:r:m:e:c:l:p:")
    except:
        printHelp()
        sys.exit(2)
    for opt, arg in opts:
        match opt:
            case '-q':
                interestingQuoteCurrencies = [x.strip() for x in arg.split(',')]
            case '-s':
                interestingBaseCurrencies = [x.strip() for x in arg.split(',')]
            case '-o':
                outputDirectory = arg
            case '-t':
                timeframe = [x.strip() for x in arg.split(',')]
            case '-u':
                api_url = arg
            case '-r':
                header = [x.strip() for x in arg.split(',')]
            case '-k':
                key_date = arg
            case '-m':
                maxCandlesPerAPIRequest = int(arg)
            case '-e':
                exchangeName = arg
            case '-n':
                writeNewFiles = True
            case '-h':
                printHelp()
                sys.exit()
            case '-l':
                maxAPIRequestsPerSec = int(arg)
            case '-p':
                cooldownPeriodInSec = int(arg)
            case '-d':
                logging.getLogger().setLevel(logging.DEBUG)
            case '-c':
                config = mdRecorderConfig(arg)
                if not exchangeName:
                    exchangeName = config.getExchangeName()
                if not api_url:
                    api_url = config.getAPIURL()
                if not header:
                    header = config.getHeaderColumns()
                if not key_date:
                    key_date = config.getDateKey()
                if not maxCandlesPerAPIRequest:
                    maxCandlesPerAPIRequest = config.getMaxCandlesPerAPIRequest()
                if not maxAPIRequestsPerSec:
                    maxAPIRequestsPerSec = config.getMaxNumberOfAPIRequestsPerSecond()
                if not cooldownPeriodInSec:
                    cooldownPeriodInSec = config.getCooldownPeriodInSec()
            case _:
                printHelp()
                sys.exit(2)

    cmd = ' '.join(sys.argv)
    logging.info(f'Running command: python {cmd}')

    match exchangeName:
        case 'COINBASE':
            mdRecorder = coinbaseMDRecorder(api_url, header, key_date, maxCandlesPerAPIRequest, exchangeName,
                                            interestingBaseCurrencies, interestingQuoteCurrencies, outputDirectory,
                                            timeframe, writeNewFiles, maxAPIRequestsPerSec, cooldownPeriodInSec)
        case 'BINANCE':
            mdRecorder = binanceMDRecorder(api_url, header, key_date, maxCandlesPerAPIRequest, exchangeName,
                                           interestingBaseCurrencies, interestingQuoteCurrencies, outputDirectory,
                                           timeframe, writeNewFiles, maxAPIRequestsPerSec, cooldownPeriodInSec)
        case _:
            print(f'Exchange:{exchangeName} not supported. Exiting...')
            quit()

    mdRecorder.startRecordingProcess()


def printHelp():
    print('--------------------------------------------- HELP ---------------------------------------------')
    print('main.py -c <config> -q <interestingQuoteCurrencies> -s <interestingSymbols> -o <outputDirectory> -t '
          '<timeframe>')
    print('Optional params (must be present in command line or config):')
    print('-e <Exchange>')
    print('-u <API URL>')
    print('-r <header colums> eg. date, low, open, high, close, volume')
    print('-k <column name of date/time in received data>')
    print('-m <maxCandlesPerAPIRequest>')
    print('-l <max number of API requests per sec>')
    print('-p <Cooldown period in case of connection error (in sec)>')
    print('-n Force write new files even if old file with some data exists')
    print('-d Debug mode (set logging level to debug)')
    print('-h Help')
    print('------------------------------------------------------------------------------------------------')


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(name)s %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logging.Formatter(datefmt='%Y-%m-%d %H:%M:%S')
    main(sys.argv[1:])
