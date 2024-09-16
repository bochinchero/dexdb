from coinmetrics.api_client import CoinMetricsClient
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def getMetric(assets,metric,date_start,date_end):
    try:
        # this function grabs the metric for asset within the specified date range,
        # removes the timezone, sets the date as an index and changes the column name to
        # the name of the metric
        logger.info(f'Getting coinmetrics data for {assets}')
        client = CoinMetricsClient()
        frequency = "1d"
        data = client.get_asset_metrics(
            assets=assets,
            metrics=metric,
            frequency=frequency,
            start_time=date_start.strftime("%Y-%m-%d"),
            end_time=date_end.strftime("%Y-%m-%d")
            ).to_dataframe()
        # convert time, rename and sort by date
        data["time"] = data['time'].dt.tz_convert(None)
        data = data.rename(columns={"time": "date"})
        data = data.sort_values(by=['date'])
        # pivot to create the asset columns
        output = data.pivot(index='date', columns="asset", values=metric)
        # format timestamps
        output.index = pd.to_datetime(output.index, utc=True, format='%Y-%m-%dT%H:%M:%S')
        # flatten multiindex
        output = pd.DataFrame(output.to_records())
        # purge
        del data
        # return output data
        return output
    except Exception as error:
        logger.error(f'{error} ')
        return None
