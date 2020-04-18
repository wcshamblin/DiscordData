#!/usr/bin/python3
from glob import glob
import re
import sys
import pandas as pd
import plotly.graph_objects as go
import argparse
ps = argparse.ArgumentParser(description='Parses and presents data from Discord\'s data dump')
ps.add_argument("path", type=str, help='Path to folder in which Discord\'s data is held. Will contain a /messages/ folder')
ps.add_argument("-c", "--cloud", action="store_true", help="Present data as word cloud (default)")
ps.add_argument("-b", "--bar", action="store_true", help="Present data as bar chart")
ps.add_argument("-n", "--num", type=int, nargs="+", help="Number of words to display. Bar chart defaults to 40, WordCloud defaults to 512")
ps.add_argument("-s", "--start", type=str, nargs="+", help="Starting date (year-month-day) Defaults to beginning of data")
ps.add_argument("-e", "--end", type=str, nargs="+", help="Stop date (year-month-day) Defaults to end of data")
 
args=ps.parse_args()
regex = re.compile('[^a-zA-Z]')
servers=eval(open(args.path+"servers/index.json").read()).values()

channels=glob((args.path+"messages/*/messages.csv"))
if len(channels)<1:
	print("Folder is not readable")
	exit()
messages=[]
uwords={}
twords=[]
acsv = pd.concat([pd.read_csv(str(i), usecols=[1, 2]) for i in channels])
acsv['Timestamp'] = pd.to_datetime(acsv['Timestamp'])

sdate=min(acsv['Timestamp'])
edate=max(acsv['Timestamp'])

if args.start!=None:
	sdate=args.start[0]
if args.end!=None:
	edate=args.end[0]
print(sdate)
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
	if args.num[0]>len(twords) or args.num[0]==-1:
		nmax=len(twords)
else:
	if args.cloud:
		nmax=512
	else:
		nmax=40

if args.bar:
	tt=str(str(len(twords))+' words selected over '+str(len(servers))+' servers<br>'+
		   'Date range: '+str(pd.Timestamp(sdate).date())+' - '+str(pd.Timestamp(edate).date())+'<br>'+
		   'Words presented: '+str(nmax))

	x = [i[0] for i in twords[-nmax:]]
	y = [i[1] for i in twords[-nmax:]]
	np = go.Bar(
	    x=x,
	    y=y,
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
	figure = go.Figure(np, layout=layout)
	figure.show()
else:
	from wordcloud import WordCloud
	import matplotlib.pyplot as plt
	wordcloud = WordCloud(width=1920,height=1080, max_words=nmax,relative_scaling=1,normalize_plurals=False).generate_from_frequencies(dict(twords))
	plt.imshow(wordcloud, interpolation='bilinear')
	plt.axis("off")
	plt.show()