import pandas as pd


def per_day(acsv):
    ddf = acsv.copy()
    ddf['Timestamp'] = pd.to_datetime(ddf['Timestamp']).dt.normalize()  # remove time, keep date
    ddf['Timestamp'] = ddf['Timestamp'].dt.day_name()  # convert to day of week

    series = ddf['Timestamp'].value_counts()
    ddf = pd.DataFrame({'Timestamp': series.index, 'Count': series.values})

    dow = ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')
    return ddf.groupby(['Timestamp']).sum().reindex(dow).fillna(0.0)


def per_hour(acsv, col='Timestamp'):
    hdf = acsv.copy()
    hdf[col] = pd.to_datetime(hdf[col]).dt.floor('h')  # floor hours
    del hdf['Contents']  # remove contents of message

    series = hdf[col].dt.hour.value_counts()
    hdf = pd.DataFrame({col: series.index, 'Count': series.values})

    hod = range(0, 24)
    return hdf.groupby([col]).sum().reindex(hod).fillna(0.0)
