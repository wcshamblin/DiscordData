#!/usr/bin/python3
from collections import Counter
from glob import glob
import re
import sys
import pandas as pd
import plotly.graph_objects as go
import argparse
import json
import os
from plotly.subplots import make_subplots
from pandas.api.types import CategoricalDtype
import warnings

import graphs.messages

ps = argparse.ArgumentParser(description='Parses and presents data from Discord\'s data dump')
ps.add_argument("path", type=str, help='Path to folder in which Discord\'s data is held. Will contain a /messages/ folder')
ps.add_argument("-c", "--cloud", action="store_true", help="Present data as word cloud")
ps.add_argument("-d", "--dash", action="store_true", help="Present data with dashboard (default)")
ps.add_argument("-r", "--remove", type=str, help="Remove list of date(s) from data (year/month/day) or (year/month/day-year/month/day)")
ps.add_argument("-n", "--num", type=int, nargs="+", help="Number of words to display. Bar chart defaults to 20, WordCloud defaults to 512")
ps.add_argument("-s", "--start", type=str, nargs="+", help="Starting date (year/month/day) Defaults to beginning of data")
ps.add_argument("-e", "--end", type=str, nargs="+", help="Stop date (year/month/day) Defaults to end of data")

args=ps.parse_args()
regex = re.compile('[^a-zA-Z]')
try:
    assert os.path.isdir(args.path)
except AssertionError as err:
    print("Path not a valid folder")
messages_path = os.path.join(args.path, "messages")

channels = glob(os.path.join(messages_path, "*", "messages.csv"))
if len(channels)<1:
    print(args.path + " is not a readable discord data folder")
    exit()

with open(os.path.join(args.path, "servers", "index.json")) as f:
    servers = json.load(f)

def load_cache(read_func, path, **kwargs):
    abspath = os.path.abspath(path)
    CACHE_PATH = "cache"
    os.makedirs(CACHE_PATH, exist_ok=True)

    key = json.dumps({
        "path": abspath,
        **kwargs
    })

    # Associates each message path with cache file
    CACHE_INDEX = os.path.join(CACHE_PATH, "index.json")

    if os.path.isfile(CACHE_INDEX):
        with open(CACHE_INDEX) as f:
            index = json.load(f)

        try:
            cache_file = index[key]
        except KeyError:  # File is not cached
            read_source = True
        else:  # Check if cache file exists
            read_source = not os.path.isfile(cache_file)
    else:
        read_source = True

        with open(CACHE_INDEX, 'w') as f:  # Initialize index.json
            json.dump({}, f)

    if read_source:
        # Read number of entries
        with open(CACHE_INDEX) as f:
            count = len(json.load(f))

        filename = f"{count}.pkl"
        cache_file = os.path.abspath(os.path.join(CACHE_PATH, filename))

        # Write cache file
        df = read_func(abspath, **kwargs)
        df.to_pickle(cache_file)

        # Update index.json
        with open(CACHE_INDEX) as f:
            index = json.load(f)

        index[key] = cache_file

        with open(CACHE_INDEX, 'w') as f:
            json.dump(index, f)

    else:  # Load cache
        df = pd.read_pickle(cache_file)

    return df


def load_cols(read_func, files, cols=[], **kwargs):
    dfs = []
    for i in files:
        next_df = load_cache(read_func, i, **kwargs)
        drop_cols = set(next_df.columns) - {"timestamp", *cols}
        next_df.drop(drop_cols, axis=1, inplace=True)
        dfs.append(next_df)

    return pd.concat(dfs, ignore_index=True)


def count_timestamp(df, unix=True, col='timestamp'):
    """Count number of events per day within dataframe. Mutates argument."""

    PRUNE = set(df.columns) - {col,}
    df.drop(PRUNE, axis=1, inplace=True)

    if unix:
        # Keep unix timestamps
        df[col] = df[col][df[col].apply(isinstance, args=(int,))]
        df[col] = pd.to_datetime(df[col], unit='ms')
    else:
        df[col] = pd.to_datetime(df[col])

    # Localize and remove time but keep date
    df[col] = df[col].dt.tz_localize(None).dt.normalize()

    series = df[col].value_counts()
    # Set missing dates to 0
    series = series.resample('D').sum().sort_index()
    return pd.DataFrame({col: series.index, 'Count': series.values})

def get_series(odf, col):
    """Count number of events per day within dataframe. Does not mutate argument."""

    PRUNE = set(odf.columns) - {"timestamp", col}
    df = odf.drop(PRUNE, axis=1)

    # Filter strings
    df.timestamp = df.timestamp[df.timestamp.apply(isinstance, args=(int,))]
    df.dropna(inplace=True)

    # Localize and remove time but keep date
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms').dt.tz_localize(None)
    df['timestamp'] = df['timestamp'].dt.normalize()

    multidf = {}
    for key in df[col].unique():
        cdf = df[df[col] == key]  # Filter each column value
        series = cdf['timestamp'].value_counts()
        series = series.resample('D').sum().sort_index()
        multidf[key] = pd.DataFrame({'timestamp': series.index, 'Count': series.values})

    return multidf

def get_analytics_count(path, atype):
    """Return dataframe representing number of events per day"""
    search = os.path.join(path, "activity", atype, "*json")
    files = [os.path.abspath(i) for i in glob(search)]

    df = load_cols(pd.read_json, files,
                   convert_dates=False, lines=True)

    return count_timestamp(df)

def get_all_series(path, cols):
    """Return dataframe for cols per day"""
    search = os.path.join(path, "activity", "*", "*json")
    files = [os.path.abspath(i) for i in glob(search)]

    dtypes = {col: "category" for col in cols}

    # Load all files but drop all non-essential columns
    df = load_cols(pd.read_json, files, cols,
                   dtype=dtypes, convert_dates=False, lines=True)

    fp_df = {}
    for col in cols:
        print(f"Produced {col} series.")
        fp_df[col] = get_series(df, col)

    return fp_df

def get_activity_count(path):
    """Return dictionary of dataframes representing number of events per day"""
    activity_path = os.path.join(path, "activity")
    activity = {}

    for atype in os.listdir(activity_path):
        activity[atype] = get_analytics_count(path, atype)

    return activity

activity = get_activity_count(args.path)


cols = ('city', 'event_type', 'guild_id', 'ip', 'os', 'private', 'release_channel')
fp_df = get_all_series(args.path, cols)

# Rename servers
for key, value in servers.items():
    if key in fp_df["guild_id"]:
        fp_df["guild_id"][value] = fp_df["guild_id"].pop(key)

messages=[]
acsv = pd.concat([load_cache(pd.read_csv, i, usecols=[1, 2]) for i in channels])
acsv['Timestamp'] = pd.to_datetime(acsv['Timestamp'])

sdate=min(acsv['Timestamp'])
edate=max(acsv['Timestamp'])

if args.start is not None:
    sdate=args.start[0]
if args.end is not None:
    edate=args.end[0]
try:
    acsv=(acsv.loc[(acsv['Timestamp'] > sdate) & (acsv['Timestamp'] <= edate)])

except TypeError as error:
    print("TypeError: Start/End date passed was not parsable")
    exit()

if args.remove:
    args.remove=args.remove.split('-')
    args.remove=pd.date_range(args.remove[0], args.remove[-1], tz='UTC')
    try:
        acsv=acsv[[d not in args.remove for d in acsv['Timestamp'].dt.date]]
    except ValueError as error:
        print("ValueError: Remove date passed was not parsable")
        exit()

def get_twords(acsv):
    uwords = Counter()

    for channel in acsv.to_numpy():
        for word in str(channel[1]).split():
            cleanedword=regex.sub('', word).strip().lower()
            if cleanedword:
                uwords[cleanedword] += 1

    twords = list(uwords.items())
    twords=sorted(twords, key=lambda x: x[1])
    return twords

twords = get_twords(acsv)

if args.num is not None:
    nmax=args.num[0]
else:
    if args.cloud:
        nmax=512
    else:
        nmax=20
if nmax>len(twords) or nmax==-1:
    nmax=len(twords)

if args.cloud:
    from wordcloud import WordCloud
    import matplotlib.pyplot as plt
    print(str(str(len(twords))+' words selected over '+str(len(servers))+' servers.<br>'+
           'Date range: '+str(pd.Timestamp(sdate).date())+' - '+str(pd.Timestamp(edate).date())+'\n'))
    wordcloud = WordCloud(width=1920,height=1080, max_words=nmax,relative_scaling=1,normalize_plurals=False).generate_from_frequencies(dict(twords))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    plt.show()


else:
    fig = make_subplots(rows=4, cols=3, subplot_titles=("Total words", "Timeseries", "Messages per hour", "Messages per day",
                                                        "Analytics", "City info", "IP info", "OS info",
                                                        "Release channel", "Guild ID", "Event Type", "Private"))

    x = [i[0] for i in twords[-nmax:]]
    y = [i[1] for i in twords[-nmax:]]
    fig.add_trace(go.Bar(x=x,y=y, name="Unique words used"), row=1, col=1)

    #tsdf = graphs.messages.perDate(acsv)
    tsdf = count_timestamp(acsv.copy(), unix=False, col="Timestamp")
    fig.add_trace(go.Scatter(x=list(tsdf['Timestamp']), y=list(tsdf['Count']),name="Messages/Date"), row=1, col=2)

    ddf = graphs.messages.perDay(acsv)

    fig.add_trace(go.Bar(x=ddf.index.values.tolist(),y=ddf['Count'], name="Messages/Day"), row=2, col=1)

    hdf = graphs.messages.perHour(acsv)
    fig.add_trace(go.Bar(x=hdf.index.values.tolist(),y=hdf['Count'], name="Messages/Hour"), row=1, col=3)

    for key, value in activity.items():
        fig.add_trace(go.Scatter(x=list(value['timestamp']), y=list(value['Count']),
                                 name=f"{key.title()}/Day"), row=2, col=2)

    def get_stacked_area(key, value, **kwargs):
        return go.Scatter(
            x=list(value['timestamp']), y=list(value['Count']),
            mode='lines', stackgroup='one', groupnorm='percent',
            name=key, **kwargs)

    for key, value in fp_df['city'].items():
        fig.add_trace(get_stacked_area(key, value), row=2, col=3)

    for key, value in fp_df['ip'].items():
        fig.add_trace(get_stacked_area(key, value, showlegend=False), row=3, col=1)

    for key, value in fp_df['os'].items():
        fig.add_trace(get_stacked_area(key, value), row=3, col=2)

    for key, value in fp_df['release_channel'].items():
        fig.add_trace(get_stacked_area(key, value), row=3, col=3)

    for key, value in fp_df['guild_id'].items():
        fig.add_trace(get_stacked_area(key, value, showlegend=False), row=4, col=1)

    for key, value in fp_df['event_type'].items():
        fig.add_trace(get_stacked_area(key, value, showlegend=False), row=4, col=2)

    for key, value in fp_df['private'].items():
        fig.add_trace(get_stacked_area(key, value, showlegend=False), row=4, col=3)

    fig.update_layout(xaxis3=dict(tickmode="array", tickvals=list(range(24)), ticktext=[str(i) + ':00' for i in range(24)]))

    tt=str(str(sum([i[1] for i in twords]))+' words / '+str(int(tsdf['Count'].sum()))+' messages selected over '+str(len(servers))+' servers.<br>'+
           'Date range: '+str(pd.Timestamp(sdate).date())+' - '+str(pd.Timestamp(edate).date()))

    fig.update_layout(
        title_text=tt
    )

    layout = go.Layout(
        title=tt,
        xaxis=dict(
            title='Word'
        ),
        yaxis=dict(
            title='Count'
        ),
        hovermode='closest',
        #showlegend=True
    )
    fig.show()