import pandas as pd
import traceback
import cm
import dbmgr
import configparser
import logging

logger = logging.getLogger(__name__)
# read configuration
config = configparser.ConfigParser()
config.read('config.conf')
# parse required configuration parameters
dbPath = config['dataHandling']['dbPath']

# this function gets all the required data from the specified database
def getCandleData(path):
    try:
        candlesQry = """
        select e.name as exchange, 
        m.name as market, 
        c.timeOpen, 
        c.timeClose, 
        cast(c.baseVolume as float) / mc.baseConversionFactor as baseVol, 
        cast(c.quoteVolume as float) / mc.quoteConversionFactor as quoteVol, 
        c.high, 
        c.low, 
        c.open, 
        c.close, 
        m.base as baseAsset  
        FROM candles c 
        left join markets m on c.marketID = m.ID 
        left join marketConfig mc on m.ID = mc.marketID 
        left join exchanges e on e.ID = m.exchangeID 
        """
        output = dbmgr.freeQuery(path,candlesQry)
        output['timeOpen'] = pd.to_datetime(output['timeOpen'], utc=True, format='%Y-%m-%d %H:%M:%S')
        output['timeClose'] = pd.to_datetime(output['timeClose'], utc=True, format='%Y-%m-%d %H:%M:%S')
        return output
    except Exception as error:
        logger.error(f'{error} ')
        return None


def getBookData(path):
    try:
        candlesQry = """
        select e.name as exchange, 
        m.name as market, 
        b.TimeStamp, 
        b.Side, 
        b.Rate, 
        b.Qty, 
        m.base as baseAsset, 
        m.quote as quoteAsset 
        FROM books b
        left join markets m on b.marketID = m.ID 
        left join exchanges e on m.exchangeID = m.exchangeID 
        """
        output = dbmgr.freeQuery(path,candlesQry)
        output['TimeStamp'] = pd.to_datetime(output['TimeStamp'], utc=True, format='%Y-%m-%d %H:%M:%S')
        return output
    except Exception as error:
        logger.error(f'{error} ')
        return None


def convertValueUSD(data,dateCol,assetCol,valueCol):
    # this function accepts a dataframe as an input,
    # gets teh cm PriceUSD data for all assets listed in the assetCol column
    # and converts the valueCol to USD, returning the data
    try:
        # get list of assets
        assetList = list(data[assetCol].apply(lambda x: x.split(".")[0]).unique())
        # get start and end timestamps from the df
        startDate = pd.to_datetime(data[dateCol].min(),  utc=True, format='%Y-%m-%d %H:%M:%S')
        endDate = pd.to_datetime(data[dateCol].max(),  utc=True, format='%Y-%m-%d %H:%M:%S')
        # get coinemtrics data for the specified time range and assets
        PriceUSD = cm.getMetric(assetList,'PriceUSD',startDate,endDate)
        # merge the price data in and into a temporary df
        temp = data[[dateCol,assetCol,valueCol]].sort_values(by=[dateCol])
        temp = pd.merge_asof(temp,PriceUSD,left_on=dateCol,right_on='date')
        # calculate the value USD
        colUSD = (valueCol+'USD') # new column name
        temp[colUSD] = temp.apply(lambda row: row[valueCol] * row[row[assetCol].split(".")[0]],axis=1)
        # clean up and return output
        output = temp[[dateCol,colUSD]]
        # merge back on original data
        output = pd.merge(data,output,how='left',left_on=dateCol, right_on=dateCol)
        return output
    except Exception as error:
        print(traceback.format_exc())
        logger.error(f'{error} ')
        return None

test = getCandleData(dbPath)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
print(test[['timeOpen','market','baseVol']].sort_values(by='timeOpen'))
data = convertValueUSD(test,'timeOpen','baseAsset','baseVol')
print(data[['timeOpen','market','baseVol','baseVolUSD']].sort_values(by='timeOpen'))
test = data[['timeOpen','market','baseVolUSD',]]
output = test.set_index('timeOpen').groupby([pd.Grouper(freq='1D'), 'market']).sum()

print(output)
