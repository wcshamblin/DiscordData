#!/usr/bin/python3
from collections import defaultdict
from glob import glob
import re
import sys
import pandas as pd
import plotly.graph_objects as go
import argparse
import json
import os
from plotly.subplots import make_subplots
from time import tzname
from pandas.api.types import CategoricalDtype
from pytz.exceptions import UnknownTimeZoneError
import warnings

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
    servers = json.load(f).values()

def load_cache(read_func, path, **kwargs):
    abspath = os.path.abspath(path)
    CACHE_PATH = "cache"
    os.makedirs(CACHE_PATH, exist_ok=True)

    # Associates each message path with cache file
    CACHE_INDEX = os.path.join(CACHE_PATH, "index.json")

    if os.path.isfile(CACHE_INDEX):
        with open(CACHE_INDEX) as f:
            index = json.load(f)

        try:
            cache_file = index[abspath]
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

        index[abspath] = cache_file

        with open(CACHE_INDEX, 'w') as f:
            json.dump(index, f)

    else:  # Load cache
        df = pd.read_pickle(cache_file)

    return df

def count_timestamp(idf):
    """Count number of events per day within dataframe"""
    df = idf.copy()  # Don't modify the argument

    PRUNE = set(df.columns) - {"timestamp",}
    df.drop(PRUNE, axis=1, inplace=True)

    # Keep unix timestamps
    df.timestamp = df.timestamp[df.timestamp.apply(isinstance, args=(int,))]

	# Localize and remove time but keep date
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms').dt.tz_localize(None)
    df['timestamp'] = df['timestamp'].dt.normalize()

    series = df['timestamp'].value_counts()
    # Set missing dates to 0
    series = series.resample('D').sum().sort_index()
    return pd.DataFrame({'timestamp': series.index, 'Count': series.values})

def get_analytics_count(path, atype):
    """Return dataframe representing number of events per day"""
    search = os.path.join(path, "activity", atype, "*json")
    files = [os.path.abspath(i) for i in glob(search)]

    df = pd.concat([load_cache(pd.read_json, i, convert_dates=False, lines=True) for i in files],
                   ignore_index=True)
    return count_timestamp(df)

def get_activity_count(path):
    """Return dictionary of dataframes representing number of events per day"""
    activity_path = os.path.join(path, "activity")
    activity = {}

    for atype in os.listdir(activity_path):
        activity[atype] = get_analytics_count(path, atype)

    return activity

activity = get_activity_count(args.path)

messages=[]
uwords = defaultdict(int)
twords = []
acsv = pd.concat([load_cache(pd.read_csv, i, usecols=[1, 2]) for i in channels])
acsv['Timestamp'] = pd.to_datetime(acsv['Timestamp'])

sdate=min(acsv['Timestamp'])
edate=max(acsv['Timestamp'])

ltz=''.join(re.findall('([A-Z])', tzname[0]))

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

for channel in acsv.to_numpy():
	for word in str(channel[1]).split():
		cleanedword=regex.sub('', word).strip().lower()
		if cleanedword:
			uwords[cleanedword] += 1

for tple in uwords:
	twords.append((tple, uwords[tple]))
twords=sorted(twords, key=lambda x: x[1])

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
	fig = make_subplots(rows=3, cols=2, subplot_titles=("Total words", "Timeseries", "Messages per hour", "Messages per day",
														"Analytics per day"))

	ddf = acsv.copy()
	ddf['Timestamp'] = pd.to_datetime(ddf['Timestamp']).dt.normalize()                   #remove time, keep date
	ddf['Timestamp'] = ddf['Timestamp'].dt.day_name()                                    #convert to day of week

	series = ddf['Timestamp'].value_counts()
	ddf = pd.DataFrame({'Timestamp': series.index, 'Count': series.values})

	dow=('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')
	ddf = ddf.groupby(['Timestamp']).sum().reindex(dow).fillna(0.0)

	hdf=acsv.copy()
	hdf['Timestamp'] = pd.to_datetime(hdf['Timestamp']).dt.floor('h')                    #floor hours
	try:
		hdf['Timestamp'] = pd.to_datetime(hdf['Timestamp']).dt.tz_convert(ltz).dt.hour       #localize timestamp
	except UnknownTimeZoneError as error:
		warnings.warn("Timezone could not be localized, using UTC...")
		hdf['Timestamp'] = pd.to_datetime(hdf['Timestamp']).dt.hour
	del hdf['Contents']                                                                  #remove contents of message

	series = hdf['Timestamp'].value_counts()
	hdf = pd.DataFrame({'Timestamp': series.index, 'Count': series.values})

	hod=range(0,24)
	hdf = hdf.groupby(['Timestamp']).sum().reindex(hod).fillna(0.0)

	acsv['Timestamp'] = pd.to_datetime(acsv['Timestamp']).dt.normalize()                 #remove time, keep date
	del acsv['Contents']                                                                 #remove contents of message

	series = acsv['Timestamp'].value_counts()
	# Set missing dates to 0
	series = series.resample('D').sum().sort_index()
	tsdf = pd.DataFrame({'Timestamp': series.index, 'Count': series.values})


	x = [i[0] for i in twords[-nmax:]]
	y = [i[1] for i in twords[-nmax:]]
	fig.add_trace(go.Bar(x=x,y=y, name="Unique words used"), row=1, col=1)
	fig.add_trace(go.Bar(x=ddf.index.values.tolist(),y=ddf['Count'], name="Messages/Day"), row=2, col=2)
	fig.add_trace(go.Bar(x=hdf.index.values.tolist(),y=hdf['Count'], name="Messages/Hour"), row=2, col=1)
	fig.add_trace(go.Scatter(x=list(tsdf['Timestamp']), y=list(tsdf['Count']),name="Messages/Date"), row=1, col=2)

	for key, value in activity.items():
		fig.add_trace(go.Scatter(x=list(value['timestamp']), y=list(value['Count']),
								 name=f"{key.title()}/Day"), row=3, col=1)

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
