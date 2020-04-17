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
args=ps.parse_args()
regex = re.compile('[^a-zA-Z]')
channels=glob((args.path+"messages/*/messages.csv"))
if len(channels)<1:
	print("Folder is empty/not found")
	exit()
messages=[]
uwords={}
twords=[]
for i in channels:
	f=pd.read_csv(str(i), usecols=[2])
	messages.append((f.get_values()))
for channel in messages:
	for message in channel:
		for word in str(message[0]).split():
			cleanedword=regex.sub('', word).strip().lower()
			if cleanedword not in uwords:
				uwords[cleanedword]=1
			else:
				uwords[cleanedword]+=1
for tple in uwords:
	twords.append((tple, uwords[tple]))
twords=sorted(twords, key=lambda x: x[1])

if args.num is not None:
	if args.num[0]>len(twords) or args.n[0]==-1:
		nmax=len(twords)
else:
	if args.cloud:
		nmax=512
	else:
		nmax=40

if args.bar:
	x = [i[0] for i in twords[-nmax:]]
	y = [i[1] for i in twords[-nmax:]]
	np = go.Bar(
	    x=x,
	    y=y,
	    )
	layout = go.Layout(
	    title='Word vs. Count',
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