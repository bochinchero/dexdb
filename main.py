import dbmgr
import dexapi
import logging
import pandas as pd
import datetime
import time
import configparser


# read configuration
config = configparser.ConfigParser()
config.read('config.conf')
# parse required configuration parameters
dbPath = config['dataHandling']['dbPath']
logPath = config['dataHandling']['logPath']
sleepTimer = float(config['dataHandling']['sleepTimer'])
exchangeList = config['dataHandling']['exchanges'].split(',')

logger = logging.getLogger(__name__)

def initialize():
    # initalize log
    logging.basicConfig(handlers=[logging.FileHandler(logPath), logging.StreamHandler()],
                        format='%(asctime)s %(levelname)-8s %(name)s:%(funcName)s: %(message)s',
                        level=logging.INFO,
                        datefmt='%Y-%m-%d %H:%M:%S')
    # connect to database, this will create the db and tables if it doesn't exist
    dbmgr.pathCheck(logPath)
    dbmgr.initalizeDB(dbPath)
    logger.info('Initialization Complete')


def updateExchanges():
    # insert/update records for exchange list
    dbmgr.insertRecords(dbPath, 'exchanges', pd.DataFrame({'name':exchangeList}),{'name':'name'})
    # read exchange data into a df
    dfExchanges = dbmgr.readTable(dbPath,'exchanges')
    logger.info('process complete')
    return dfExchanges


def updateMarket(exchange):
    # for every exchange we need to get the respective market data from the api
    try:
        if exchange is None:
            raise Exception (f'exchange is None, exiting.')
        # api call int o dataframe
        markets = dexapi.getMarkets(exchange['name'])
        if markets is None:
            raise Exception (f'markets is None, exiting.')
        # add the exchange ID and the timestamp
        markets['exchangeID'] = exchange['ID']
        markets['exchangeName'] = exchange['name']
        markets['LastUpdated'] = datetime.datetime.now()
        # create the column dictionary
        colDict = {'exchangeID':'exchangeID',
                   'name':'name',
                   'base':'base',
                   'quote':'quote'}
        # insert data into markets table, ignore if duplicate
        dbmgr.insertRecords(dbPath, 'markets', markets, colDict)
        # get the market IDs from the database
        marketsTable = dbmgr.readTable(dbPath, 'markets').loc[lambda df: df['exchangeID']==exchange['ID'] ]
        # get the IDs from the marketstable into the marketdata
        markets['marketID'] = marketsTable.loc[marketsTable['name'] == markets['name']].ID
        colDict = {'marketID' : 'marketID',
                   'epochlen':'epochlen',
                   'lotsize':'lotsize',
                   'parcelSize':'parcelSize',
                   'ratestep':'ratestep',
                   'baseConversionFactor': 'baseConversionFactor',
                   'quoteConversionFactor': 'quoteConversionFactor',
                   'LastUpdated': 'LastUpdated'}
        # insert data into marketconfig, replace if necessary.
        dbmgr.insertRecords(dbPath, 'marketConfig', markets, colDict,replace=True)
        output = markets
        logger.info(f'complete for {exchange["name"]}')
        return output
    except Exception as err:
        logger.error(f'{err=}, {type(err)=}')
        return None


def updateBooks(markets):
    if markets is None:
        logger.error(f'markets is None, exiting.')
        return None
    # for every market we need to get the respective order book data from the api
    for idx, market in markets.iterrows():
        try:
            # api call int o dataframe
            books = dexapi.getOrderBook(market['exchangeName'],market['base'],market['quote'])
            # add the market ID and the timestamp
            books['marketID'] = market['marketID']
            books['TimeStamp'] = datetime.datetime.now()
            # create the column dictionary
            colDict = {'marketID':'marketID',
                       'TimeStamp':'TimeStamp',
                       'side':'side',
                       'rate':'rate',
                       'qty':'qty'}
            # insert data into order book table, ignore if duplicate
            dbmgr.insertRecords(dbPath, 'books', books, colDict)
            logger.info(f'complete for {market["exchangeName"]} {market["name"]}')
            # pause to avoid too many request errors
            time.sleep(sleepTimer)
        except Exception as err:
            logger.error(f' {err=}, {type(err)=}')
            break
    logger.info(f'complete.')
    return True


def updateCandles(markets):
    # for every market we need to get the respective order book data from the api
    for idx, market in markets.iterrows():
        try:
            # api call int o dataframe
            candleData = dexapi.getCandles(market['exchangeName'],market['base'],market['quote'])
            # add the market ID and the timestamp
            candleData['marketID'] = market['marketID']
            # create the column dictionary
            colDict = {'marketID':'marketID',
                       'startStamps':'timeOpen',
                       'endStamps': 'timeClose',
                       'matchVolumes': 'baseVolume',
                       'quoteVolumes': 'quoteVolume',
                       'highRates':'high',
                       'lowRates':'low',
                       'startRates':'open',
                       'endRates':'close'}
            # insert data into order book table, ignore if duplicate
            dbmgr.insertRecords(dbPath, 'candles', candleData, colDict, replace=True)
            logger.info(f'complete for {market["exchangeName"]} {market["name"]}')
            # pause to avoid too many request errors
            time.sleep(sleepTimer)
        except Exception as err:
            logger.error(f' {err=}, {type(err)=}')
            break
    return True


if __name__ == '__main__':
    initialize()
    logger.info('Starting data collection...')
    exchanges = updateExchanges()
    for index, row in exchanges.iterrows():
        markets = updateMarket(row)
        orderbooks = updateBooks(markets)
        candles = updateCandles(markets)
    logger.info('Data collection completed.')