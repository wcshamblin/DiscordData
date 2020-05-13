import pandas as pd
import resources.tz


def count_df(df, interval='D', col='timestamp'):
    series = df[col].value_counts()
    # Set missing dates to 0
    series = series.resample(interval).sum().sort_index()

    count = pd.to_numeric(series.values, downcast='unsigned')
    return pd.DataFrame({col: series.index, 'Count': count})


def count_timestamp(df, localized=False, unix=True, interval='D', col='timestamp'):
    """Count number of events per day within dataframe. Mutates argument."""

    PRUNE = set(df.columns) - {col, }
    df.drop(PRUNE, axis=1, inplace=True)

    if unix:
        # Keep unix timestamps
        df[col] = df[col][df[col].apply(isinstance, args=(int,))]
        df[col] = pd.to_datetime(df[col], unit='ms')
    else:
        df[col] = pd.to_datetime(df[col])

    if interval == 'D':  # Remove time but keep date
        df[col] = df[col].dt.normalize()

    return count_df(df, col=col, interval=interval)
