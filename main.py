import sys
import getopt
from mdRecorderConfig import mdRecorderConfig
from coinbaseMarketDataRecorder import coinbaseMDRecorder


def main(argv):
    interestingQuoteCurrencies = None
    interestingBaseCurrencies = None
    outputDirectory = None
    timeframe = None
    api_url = None
    header = None
    maxCandlesPerAPIRequest = None
    exchangeName = None
    writeNewFiles = False

    try:
        opts, args = getopt.getopt(argv, "hnq:s:o:t:u:r:m:e:c:")
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
            case '-m':
                maxCandlesPerAPIRequest = int(arg)
            case '-e':
                exchangeName = arg
            case '-n':
                writeNewFiles = True
            case '-h':
                printHelp()
                sys.exit()
            case '-c':
                config = mdRecorderConfig(arg)
                if not exchangeName:
                    exchangeName = config.getExchangeName()
                if not api_url:
                    api_url = config.getAPIURL()
                if not header:
                    header = config.getHeaderColumns()
                if not maxCandlesPerAPIRequest:
                    maxCandlesPerAPIRequest = config.getMaxCandlesPerAPIRequest()
            case _:
                printHelp()
                sys.exit(2)

    match exchangeName:
        case 'COINBASE':
            mdRecorder = coinbaseMDRecorder(api_url, header, maxCandlesPerAPIRequest, exchangeName,
                                            interestingBaseCurrencies, interestingQuoteCurrencies, outputDirectory,
                                            timeframe, writeNewFiles)
            mdRecorder.recordHistoricalPricesForAllInterestingCoins()
        case _:
            print('ERROR! exchange:', exchangeName, 'not supported. Exiting...')
            quit()


def printHelp():
    print('--------------------------------------------- HELP ---------------------------------------------')
    print('main.py -c <config> -q <interestingQuoteCurrencies> -s <interestingSymbols> -o <outputDirectory> -t '
          '<timeframe>')
    print('Optional params (must be present in command line or config):')
    print('-e <Exchange>')
    print('-u <API URL>')
    print('-r <header colums> eg. date, low, open, high, close, volume')
    print('-m <maxCandlesPerAPIRequest>')
    print('-n Force write new files even if old file with some data exists')
    print('-h Help')
    print('------------------------------------------------------------------------------------------------')

if __name__ == "__main__":
   main(sys.argv[1:])