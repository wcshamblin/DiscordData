from glob import glob
import multiprocessing as mp
import os
import pandas as pd

import resources.df_tools
import resources.reader


def load_directory(path, atype):
    directory = os.path.join(path, "activity", atype)
    search = os.path.join(directory, "*json")
    files = [os.path.abspath(i) for i in glob(search)]

    assert files

    return resources.reader.load_cols(pd.read_json, files,
                                      convert_dates=False, lines=True)


def count_activity(path, interval='D'):
    """Return dictionary of dataframes representing number of events per day"""
    activity_path = os.path.join(path, "activity")
    activity = {}

    def get_analytics_count(path, atype):
        """Return dataframe representing number of events per day"""

        df = load_directory(path, atype)
        return resources.df_tools.count_timestamp(df, interval=interval)

    for atype in os.listdir(activity_path):
        activity[atype] = get_analytics_count(path, atype)

    return activity


def get_series(odf, col, interval='W'):
    """Count number of events per interval within dataframe. Does not mutate argument."""

    PRUNE = set(odf.columns) - {"timestamp", col}
    df = odf.drop(PRUNE, axis=1)

    # Filter strings
    df.timestamp = df.timestamp.apply(resources.df_tools.parse_timestamp)
    df.dropna(inplace=True)

    # Localize and remove time but keep date
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df['timestamp'] = resources.tz.localize_utc(df['timestamp'])
    df['timestamp'] = df['timestamp'].dt.normalize()

    multidf = {}
    for key in df[col].unique():
        cdf = df[df[col] == key]  # Filter each column value
        multidf[key] = resources.df_tools.count_df(cdf, interval=interval)

    print(f"Finish {col} series.")
    return {col: multidf}


def get_all_series(path, cols):
    """Return dataframe for cols per day"""
    search = os.path.join(path, "activity", "*", "*json")
    files = [os.path.abspath(i) for i in glob(search)]

    dtypes = {col: "category" for col in cols}

    # Load all files but drop all non-essential columns
    df = resources.reader.load_cols(pd.read_json, files, cols, dtype=dtypes,
                                    convert_dates=False, lines=True)

    def minify(df, col):
        PRUNE = set(df.columns) - {"timestamp", col}
        return df.drop(PRUNE, axis=1)

    args = ((minify(df, col), col) for col in cols)
    with mp.Pool() as pool:
        results = pool.starmap(get_series, args)

    fp_df = {}
    while results:
        fp_df.update(results.pop())

    if 'private' in fp_df:
        fp_df['private']['Private'] = fp_df['private'][True]
        fp_df['private']['Public'] = fp_df['private'][False]
        del fp_df['private'][True]
        del fp_df['private'][False]

    return fp_df


def get_fp(path, servers):
    COLS = ('city', 'event_type', 'guild_id', 'ip', 'os', 'private', 'release_channel')
    fp_df = get_all_series(path, COLS)

    # Rename servers
    for key, value in servers.items():
        if key in fp_df["guild_id"]:
            fp_df["guild_id"][value] = fp_df["guild_id"].pop(key)

    return fp_df
