import pytest
import cm
import datetime
import pandas as pd


@pytest.fixture
def comparisonData():
    d = {'date': [pd.to_datetime('2018-01-01 00:00:00+00:00'),
                  pd.to_datetime('2018-01-02 00:00:00+00:00'),
                  pd.to_datetime('2018-01-03 00:00:00+00:00')],
         'btc': [13464.653612, 14754.322205, 15010.286160],
         'dcr': [106.121930, 105.131133, 106.430008],
         'eth': [756.071766, 863.918308, 943.648126]}
    t = pd.DataFrame(data=d, index=[0, 1, 2])
    yield t

# simple test to verify that the coinmetrics API is working as expected
def test_getMetric(comparisonData):
    # get start date and end date for the range
    startDate = comparisonData['date'].iloc[0]
    endDate = comparisonData['date'].iloc[-1]
    # get results
    results = cm.getMetric(['dcr', 'btc', 'eth'], 'PriceUSD', startDate, endDate)
    # compare data frame against retrieved data
    pd.testing.assert_frame_equal(results,comparisonData)