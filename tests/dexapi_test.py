import pytest
import dexapi
import logging
import pytest_mock

LOGGER = logging.getLogger(__name__)


def checkLog(expLogMessage,caplog):
    # check that the message exists in the log
    matchesFound = 0
    if expLogMessage is not None:
        for record in caplog.records:
            if expLogMessage in record.msg:
                matchesFound += 1
        assert matchesFound == 1


@pytest.mark.parametrize("urlInput,expLogMessage,expException", [
    ("https://dex.decred.org/api/config", None, False),
    ("https://dex.decred.org/api/error", 'response error', True)
    ])
def test_getResponse(urlInput,expLogMessage,expException,caplog):
    with caplog.at_level(logging.INFO):
        if expException:
            with pytest.raises(Exception):
                actual = dexapi.getResponse(urlInput)
        else:
            actual = dexapi.getResponse(urlInput)
            assert actual is not None
    checkLog(expLogMessage,caplog)


@pytest.mark.parametrize("exchangeUrl,expLogMessage,expException,returnNone", [
    ("dex.decred.org", 'data processed for', False, False),
    ("thiswontworkatall.org", 'response error', False, True),
    ])
def test_getMarkets(exchangeUrl,expLogMessage,expException,returnNone,caplog):
    with caplog.at_level(logging.DEBUG):
        if expException:
            with pytest.raises(Exception):
                actual = dexapi.getMarkets(exchangeUrl)
        else:
            actual = dexapi.getMarkets(exchangeUrl)
            if returnNone:
                assert actual is None
            else:
                assert actual is not None
    checkLog(expLogMessage,caplog)


@pytest.mark.parametrize("exchangeUrl,expLogMessage,expException,returnNone,mockReturn", [
    ("exchange", 'assets key not found', False, True,{}),
    ("exchange", 'markets key not found', False, True, {'assets':''}),
    ("exchange", 'quote key not found', False, True, {'assets':{}, 'markets':{}}),
    ("exchange", 'base key not found', False, True, {'assets': {'unitinfo.conventional.conversionFactor':''}, 'markets': {'quote':''}}),
    ("exchange", 'conversionFactor key not found', False, True, {'assets': [{'unitinfo':{'conventional':''}}], 'markets': {'quote': '','base':''}})
    ])
def test_getMarkets_mockResponse(exchangeUrl,expLogMessage,expException,returnNone,caplog,mockReturn,mocker):
    mocker.patch('dexapi.getResponse', return_value=mockReturn)
    with caplog.at_level(logging.DEBUG):
        if expException:
            with pytest.raises(Exception):
                actual = dexapi.getMarkets(exchangeUrl)
        else:
            actual = dexapi.getMarkets(exchangeUrl)
            if returnNone:
                assert actual is None
            else:
                assert actual is not None
    checkLog(expLogMessage,caplog)


@pytest.mark.parametrize("exchangeUrl,base,quote,expLogMessage,expException,returnNone", [
    ("dex.decred.org",'dcr','btc', 'data processed for', False, False),
    ("dex.decred.org", 'btc', 'btc', 'response error', False, True),
    ("thiswontworkatall.org",'dcr','btc', 'response error', False, True),
    ])
def test_getOrderBooks(exchangeUrl,base,quote,expLogMessage,expException,returnNone,caplog):
    with caplog.at_level(logging.DEBUG):
        if expException:
            with pytest.raises(Exception):
                actual = dexapi.getOrderBook(exchangeUrl,base,quote)
        else:
            actual = dexapi.getOrderBook(exchangeUrl,base,quote)
            if returnNone:
                assert actual is None
            else:
                assert actual is not None
    checkLog(expLogMessage,caplog)


@pytest.mark.parametrize("exchangeUrl,base,quote,expLogMessage,expException,returnNone", [
    ("dex.decred.org",'dcr','btc', 'data processed for', False, False),
    ("dex.decred.org", 'btc', 'btc', 'response error', False, True),
    ("thiswontworkatall.org",'dcr','btc', 'response error', False, True),
    ])
def test_getCandles(exchangeUrl,base,quote,expLogMessage,expException,returnNone,caplog):
    with caplog.at_level(logging.DEBUG):
        if expException:
            with pytest.raises(Exception):
                actual = dexapi.getCandles(exchangeUrl,base,quote)
        else:
            actual = dexapi.getCandles(exchangeUrl,base,quote)
            if returnNone:
                assert actual is None
            else:
                assert actual is not None
    checkLog(expLogMessage,caplog)