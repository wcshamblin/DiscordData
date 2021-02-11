import pandas as pd
from pandas.api.types import is_datetime64_any_dtype as is_datetime
import resources.tz


def count_df(df, interval='D', col='timestamp'):
    series = df[col].value_counts()
    # Set missing dates to 0
    series = series.resample(interval).sum().sort_index()

    count = pd.to_numeric(series.values, downcast='unsigned')
    return pd.DataFrame({col: series.index, 'Count': count})


def count_timestamp(df, interval='D', col='timestamp'):
    """Count number of events per day within dataframe. Mutates argument."""

    PRUNE = set(df.columns) - {col, }
    df.drop(PRUNE, axis=1, inplace=True)

    df[col] = df[col].apply(parse_timestamp)

    if interval == 'D':  # Remove time but keep date
        df[col] = df[col].dt.normalize()

    return count_df(df, col=col, interval=interval)

def parse_timestamp(i):
    if isinstance(i, int):
        return pd.to_datetime(i, unit='ms', utc=True)
    elif isinstance(i, str):
        return pd.to_datetime(i.replace('"', ''))
    elif isinstance(i, pd.Timestamp) or isinstance(i, pd.core.indexes.datetimes.DatetimeIndex):
        return i
    else:
        print('Could not parse', type(i), i)
        return i