#!/usr/bin/python3
from glob import glob
import re
import sys
import pandas as pd
import plotly.graph_objects as go
regex = re.compile('[^a-zA-Z]')
channels=glob((str(sys.argv[-1])+"messages/*/messages.csv"))
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
x = [i[0] for i in twords[-20:]]
y = [i[1] for i in twords[-20:]]

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