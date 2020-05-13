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

import resources.df_tools
import resources.heatmap
import resources.messages
import resources.reader
import resources.tz

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


heatmap = resources.heatmap.Heatmap(args.path)
activity = resources.analytics.count_activity(args.path)
fp_df = resources.analytics.get_fp(args.path, servers)

messages=[]
acsv = pd.concat([resources.reader.load_cache(pd.read_csv, i, usecols=[1, 2]) for i in channels])
acsv['Timestamp'] = pd.to_datetime(acsv['Timestamp'])
acsv['Timestamp'] = resources.tz.localize_utc(acsv['Timestamp'])

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
                                                        "Analytics", "Analytics Heatmap", "City info", "OS info",
                                                        "Release channel", "Guild ID", "Event Type", "DMs vs Public"))


    x = [i[0] for i in twords[-nmax:]]
    y = [i[1] for i in twords[-nmax:]]
    fig.add_trace(go.Bar(x=x,y=y, name="Unique words used"), row=1, col=1)

    tsdf = resources.df_tools.count_timestamp(acsv.copy(), localized=True,
                                              unix=False, col="Timestamp")
    fig.add_trace(go.Scatter(x=list(tsdf['Timestamp']), y=list(tsdf['Count']),name="Messages/Date"), row=1, col=2)

    ddf = resources.messages.per_day(acsv)
    fig.add_trace(go.Bar(x=ddf.index.values.tolist(),y=ddf['Count'], name="Messages/Day"), row=2, col=1)

    hdf = resources.messages.per_hour(acsv)
    fig.add_trace(go.Bar(x=hdf.index.values.tolist(),y=hdf['Count'], name="Messages/Hour"), row=1, col=3)

    for key, value in activity.items():
        fig.add_trace(go.Scatter(x=list(value['timestamp']), y=list(value['Count']),
								 name=f"{key.title()}/Day"), row=2, col=2)

    def get_stacked_area(key, value, **kwargs):
        return go.Scatter(
            x=list(value['timestamp']), y=list(value['Count']),
            mode='lines', stackgroup='one', groupnorm='percent',
            name=key, **kwargs)

    fig.add_trace(heatmap.get_go(showlegend=False), row=2, col=3)

    row = 3
    col = 1
    for t in ('city', 'os', 'release_channel', 'guild_id', 'event_type', 'private'):
        showlegend = t not in ('ip', 'guild_id', 'event_type', 'private')
        for key, value in fp_df[t].items():
            fig.add_trace(get_stacked_area(key, value, showlegend=showlegend),
			              row=row, col=col)

        col += 1
        if col > 3:
            col = 1
            row += 1

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
