#!/usr/bin/python3
from glob import glob
import re
import sys
import pandas as pd
import plotly.graph_objects as go
import argparse
import os
from plotly.subplots import make_subplots
from time import tzname

ps = argparse.ArgumentParser(description='Parses and presents data from Discord\'s data dump')
ps.add_argument("path", type=str, help='Path to folder in which Discord\'s data is held. Will contain a /messages/ folder')
ps.add_argument("-c", "--cloud", action="store_true", help="Present data as word cloud")
ps.add_argument("-d", "--dash", action="store_true", help="Present data with dashboard (default)")
ps.add_argument("-n", "--num", type=int, nargs="+", help="Number of words to display. Bar chart defaults to 20, WordCloud defaults to 512")
ps.add_argument("-s", "--start", type=str, nargs="+", help="Starting date (year-month-day) Defaults to beginning of data")
ps.add_argument("-e", "--end", type=str, nargs="+", help="Stop date (year-month-day) Defaults to end of data")

args=ps.parse_args()
regex = re.compile('[^a-zA-Z]')

assert os.path.isdir(args.path)
messages_path = os.path.join(args.path, "messages")

channels = glob(os.path.join(messages_path, "*", "messages.csv"))
if len(channels)<1:
	print(messages_path + " is not readable")
	exit()
servers=eval(open(os.path.join(args.path, "servers", "index.json")).read()).values()
messages=[]
uwords={}
twords=[]
acsv = pd.concat([pd.read_csv(str(i), usecols=[1, 2]) for i in channels])
acsv['Timestamp'] = pd.to_datetime(acsv['Timestamp'])

sdate=min(acsv['Timestamp'])
edate=max(acsv['Timestamp'])

if args.start is not None:
	sdate=args.start[0]
if args.end is not None:
	edate=args.end[0]
acsv=(acsv.loc[(acsv['Timestamp'] > sdate) & (acsv['Timestamp'] <= edate)])

for channel in acsv.to_numpy():
	for word in str(channel[1]).split():
		cleanedword=regex.sub('', word).strip().lower()
		if cleanedword != '':
			if cleanedword not in uwords:
				uwords[cleanedword]=1
			else:
				uwords[cleanedword]+=1
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
	ddf = ddf.set_index('Timestamp').loc[dow].reset_index()

	hdf=acsv.copy()
	hdf['Timestamp'] = pd.to_datetime(hdf['Timestamp']).dt.floor('h')                    #floor hours
	hdf['Timestamp'] = pd.to_datetime(hdf['Timestamp']).dt.tz_convert(tzname[0]).dt.hour #localize timestamp
	del hdf['Contents']                                                                  #remove contents of message
	hdf['Count'] = hdf.groupby('Timestamp')['Timestamp'].transform('count')              #create count col
	hdf=hdf.drop_duplicates(keep="first").sort_values('Timestamp')                       #drop duplicate dates
	hod=range(0,24)
	hdf = hdf.set_index('Timestamp').loc[hod].reset_index()

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
	fig.add_trace(go.Bar(x=ddf['Timestamp'],y=ddf['Count'], name="Messages/Day"), row=2, col=2)
	fig.add_trace(go.Bar(x=hdf['Timestamp'],y=hdf['Count'], name="Messages/Hour"), row=2, col=1)
	fig.add_trace(go.Scatter(x=list(acsv['Timestamp']), y=list(acsv['Count']),name="Messages/Date"), row=1, col=2)

	fig.update_layout(xaxis3=dict(tickmode="array", tickvals=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23], ticktext=['00:00', '1:00', '2:00', '3:00', '4:00', '5:00', '6:00', '7:00', '8:00', '9:00', '10:00', '11:00', '12:00', '13:00', '14:00', '15:00', '16:00', '17:00', '18:00', '19:00', '20:00', '21:00', '22:00', '23:00']))

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