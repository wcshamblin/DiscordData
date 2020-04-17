#!/usr/bin/python3
from glob import glob
import re
import sys
import pandas as pd
import plotly
regex = re.compile('[^a-zA-Z]')
print(sys.argv[-1])
channels=glob((str(sys.argv[-1])+"messages/*/messages.csv"))
messages=[]
for i in channels:
	f=pd.read_csv(str(i), usecols=[2])
	messages.append((f.get_values()))
uwords={}
twords=[]
for channel in messages:
	for message in channel:
		for word in str(message[0]).split():
			cleanedword=regex.sub('', word).strip().lower()
			if cleanedword not in uwords:
				uwords[cleanedword]=1
			else:
				uwords[cleanedword]+=1
for tple in uwords:
	twords.append( (tple, uwords[tple]) )
for tple in sorted(twords, key=lambda tup: tup[1])[-100:]:
	print("Word:", "\""+str(tple[0])+"\",", "Instances:", str(tple[1]))