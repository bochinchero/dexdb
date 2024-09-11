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


# this function gets a list of all markets for a server
def getMarkets(exchange):
    try:
        url = "https://" + exchange + "/api/config"
        # get response
        response = requests.get(url)
        # handle error responses
        if not (200 <= response.status_code < 300):
            raise Exception(f'API Response error: {response.status_code}')
        data = response.json()
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
        logger.info(f'getMarkets: data processed for {exchange}')
        return markets
    except Exception as error:
        print('getServerConfig:' + str(error))
        logger.error(f'getMarkets: {str(error)} on exchange {exchange}')

# this function gets order book data for a market
def getOrderBook(exchange,base,quote):
    try:
        url = "https://" + exchange + "/api/orderbook/" + base + '/' + quote
        # get response
        response = requests.get(url)
        # handle error responses
        if not (200 <= response.status_code < 300):
            raise Exception(f'API Response error: {response.status_code}')
        data = response.json()
        # extract orders list
        books = pd.DataFrame.from_dict(data['orders'])[['rate','qty','side']].copy().sort_values(by=['rate'])
        # replace side with buy/sell
        books['side'] = books['side'].apply(lambda x: 'buy' if x == 1 else 'sell')
        books = books.groupby('rate').agg({'qty': 'sum',
                                 'side': 'first'}).reset_index()
        logger.debug(f'getOrderBook: data processed for {exchange} {base}/{quote}')
        return books
    except Exception as error:
        print('getOrderBook:' + str(error))
        logger.error(f'getOrderBook: {str(error)} for {exchange} {base}/{quote}')


# this function gets candle data for a market
def getCandles(exchange, base,quote, period='24h'):
    try:
        url = "https://" + exchange + "/api/candles/" + base + '/' + quote + '/' + period
        # get response
        response = requests.get(url)
        # handle error responses
        if not (200 <= response.status_code < 300):
            raise Exception(f'API Response error: {response.status_code}')
        data = response.json()
        # extract candles list
        candles = pd.DataFrame.from_dict(data)
        # convert timestamps to something readable
        candles['startStamps'] = pd.to_datetime(candles['startStamps'], unit='ms')
        candles['endStamps'] = pd.to_datetime(candles['endStamps'], unit='ms')
        logger.debug(f'getCandles: data processed for {exchange} {base}/{quote} {period}')
        return candles
    except Exception as error:
        print('getCandles:' + str(error))
        logger.error(f'getCandles: {str(error)} for {exchange} {base}/{quote} {period}')
