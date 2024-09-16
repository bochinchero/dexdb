import pandas as pd
from datetime import date
import datetime as dt
import requests
import os
import time
import logging
logger = logging.getLogger(__name__)


pd.set_option('display.max_columns', None)

# theee functions grab data from the exchange and preprocess them into
# pandas dataframes for storage

# list of exchange servers
exchanges = ["dex.decred.org"]

def checkKeys(keyList,data):
    # checks that all keys exist in the data, it will return the key not found
    # if the data is a list, grab the first element
    if isinstance(data, list):
        tdata = data[0]
    else:
        tdata = data
    for key in keyList:
        if key not in tdata:
            raise Exception(f'{key} key not found')
    return None


def getResponse(url):
    try:
        # get response
        response = requests.get(url,timeout=4)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
    except Exception as error:
        logger.error(f'response error {str(error)} ')
        raise
    if data is not None:
        return data
    else:
        raise Exception(f'no data returned')

# this function gets a list of all markets for a server
def getMarkets(exchange):
    try:
        url = "https://" + exchange + "/api/config"
        # get response
        data = getResponse(url)
        # verify that the response has the desired keys
        checkKeys(['assets','markets'], data)
        checkKeys(['quote', 'base'], data['markets'])
        checkKeys(['conversionFactor'], data['assets'][0]['unitinfo']['conventional'])
        # extract asset list
        assets = pd.json_normalize(data['assets']).set_index('id')
        # extract markets list
        markets = pd.DataFrame.from_dict(data['markets'])
        # base conversion factory
        markets['baseConversionFactor'] = markets['base'].apply(lambda x: assets.loc[x]['unitinfo.conventional.conversionFactor'])
        # quote conversion factory
        markets['quoteConversionFactor'] = markets['quote'].apply(lambda x: assets.loc[x]['unitinfo.conventional.conversionFactor'])
        # replace base with strings
        markets['base'] = markets['base'].apply(lambda x: assets.loc[x].symbol)
        # replace quote with strings
        markets['quote'] = markets['quote'].apply(lambda x: assets.loc[x].symbol)
        logger.info(f'data processed for {exchange}')
        return markets
    except Exception as error:
        logger.error(f' {str(error)} on exchange {exchange}')
        return None


# this function gets order book data for a market
def getOrderBook(exchange,base,quote):
    try:
        url = "https://" + exchange + "/api/orderbook/" + base + '/' + quote
        # get response
        data = getResponse(url)
        # verify that the response has the desired keys
        checkKeys(['orders'], data)
        checkKeys(['rate','qty','side'], data['orders'])
        # extract orders list
        books = pd.DataFrame.from_dict(data['orders'])[['rate','qty','side']].copy().sort_values(by=['rate'])
        # replace side with buy/sell
        books['side'] = books['side'].apply(lambda x: 'buy' if x == 1 else 'sell')
        books = books.groupby('rate').agg({'qty': 'sum',
                                 'side': 'first'}).reset_index()
        logger.debug(f'data processed for {exchange} {base}/{quote}')
        return books
    except Exception as error:
        logger.error(f'{str(error)} for {exchange} {base}/{quote}')
        return None


# this function gets candle data for a market
def getCandles(exchange, base,quote, period='24h'):
    try:
        url = "https://" + exchange + "/api/candles/" + base + '/' + quote + '/' + period
        # get response
        data = getResponse(url)
        checkKeys(['startStamps','endStamps'], data)
        # extract candles list
        candles = pd.DataFrame.from_dict(data)
        # convert timestamps to something readable
        candles['startStamps'] = pd.to_datetime(candles['startStamps'], unit='ms')
        candles['endStamps'] = pd.to_datetime(candles['endStamps'], unit='ms')
        logger.debug(f'data processed for {exchange} {base}/{quote} {period}')
        return candles
    except Exception as error:
        logger.error(f'{str(error)} for {exchange} {base}/{quote} {period}')
