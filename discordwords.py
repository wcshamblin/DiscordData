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

ps = argparse.ArgumentParser(description='Parses and presents data from Discord\'s data dump')
ps.add_argument("path", type=str, help='Path to folder in which Discord\'s data is held. Will contain a /messages/ folder')
ps.add_argument("-c", "--cloud", action="store_true", help="Present data as word cloud")
ps.add_argument("-d", "--dash", action="store_true", help="Present data with dashboard (default)")
ps.add_argument("-r", "--remove", action='append', help="Remove list of date(s) from data")
ps.add_argument("-n", "--num", type=int, nargs="+", help="Number of words to display. Bar chart defaults to 20, WordCloud defaults to 512")
ps.add_argument("-s", "--start", type=str, nargs="+", help="Starting date (year-month-day) Defaults to beginning of data")
ps.add_argument("-e", "--end", type=str, nargs="+", help="Stop date (year-month-day) Defaults to end of data")

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

messages=[]
uwords = defaultdict(int)
twords=[]
acsv = pd.concat([pd.read_csv(str(i), usecols=[1, 2]) for i in channels])
acsv['Timestamp'] = pd.to_datetime(acsv['Timestamp'])

sdate=min(acsv['Timestamp'])
edate=max(acsv['Timestamp'])

ltz=tzname[0].split()
if len(ltz)>1:
	ltz=[''.join([i[0] for i in ltz])]
ltz=str(ltz[0])

if args.start is not None:
	sdate=args.start[0]
if args.end is not None:
	edate=args.end[0]

acsv=(acsv.loc[(acsv['Timestamp'] > sdate) & (acsv['Timestamp'] <= edate)])

if args.remove:
	for rdate in args.remove:
		try:
			acsv=acsv[acsv['Timestamp'].dt.date != pd.to_datetime(rdate).tz_localize('UTC')]
		except ValueError as error:
			print("ValueError: Date passed was not parsable")
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
	fig = make_subplots(rows=2, cols=2, subplot_titles=("Total words", "Timeseries", "Messages per hour", "Messages per day"))

	ddf = acsv.copy()
	ddf['Timestamp'] = pd.to_datetime(ddf['Timestamp']).dt.normalize()                   #remove time, keep date
	del ddf['Contents']                                                                  #remove contents of message
	ddf['Timestamp'] = ddf['Timestamp'].dt.day_name()                                    #convert to day of week
	ddf['Count'] = ddf.groupby('Timestamp')['Timestamp'].transform('count')              #create count col
	ddf=ddf.drop_duplicates(keep="first")                                                #drop duplicate dates
	dow=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
	ddf = ddf.groupby(['Timestamp']).sum().reindex(dow).fillna(0.0)

	hdf=acsv.copy()
	hdf['Timestamp'] = pd.to_datetime(hdf['Timestamp']).dt.floor('h')                    #floor hours
	hdf['Timestamp'] = pd.to_datetime(hdf['Timestamp']).dt.tz_convert(ltz).dt.hour       #localize timestamp
	del hdf['Contents']                                                                  #remove contents of message
	hdf['Count'] = hdf.groupby('Timestamp')['Timestamp'].transform('count')              #create count col
	hdf=hdf.drop_duplicates(keep="first").sort_values('Timestamp')                       #drop duplicate dates
	hod=range(0,24)
	hdf = hdf.groupby(['Timestamp']).sum().reindex(hod).fillna(0.0)

	acsv['Timestamp'] = pd.to_datetime(acsv['Timestamp']).dt.normalize()                 #remove time, keep date
	del acsv['Contents']                                                                 #remove contents of message
	acsv['Count'] = acsv.groupby('Timestamp')['Timestamp'].transform('count')            #create count col
	acsv=acsv.drop_duplicates(keep="first").sort_values('Timestamp')                     #drop duplicate dates
	acsv['Timestamp'] = pd.to_datetime(acsv['Timestamp'])
	acsv.set_index('Timestamp', inplace=True)
	acsv=acsv.resample('D').mean().reset_index().fillna(0)

	x = [i[0] for i in twords[-nmax:]]
	y = [i[1] for i in twords[-nmax:]]
	fig.add_trace(go.Bar(x=x,y=y, name="Unique words used"), row=1, col=1)
	fig.add_trace(go.Bar(x=ddf.index.values.tolist(),y=ddf['Count'], name="Messages/Day"), row=2, col=2)
	fig.add_trace(go.Bar(x=hdf.index.values.tolist(),y=hdf['Count'], name="Messages/Hour"), row=2, col=1)
	fig.add_trace(go.Scatter(x=list(acsv['Timestamp']), y=list(acsv['Count']),name="Messages/Date"), row=1, col=2)

	fig.update_layout(xaxis3=dict(tickmode="array", tickvals=list(range(24)), ticktext=[str(i) + ':00' for i in range(24)]))

	tt=str(str(sum([i[1] for i in twords]))+' words / '+str(int(acsv['Count'].sum()))+' messages selected over '+str(len(servers))+' servers.<br>'+
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