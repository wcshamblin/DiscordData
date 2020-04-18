# DiscordData.py
This program takes Discord's data dump and visualizes it using plotly and python3

## Obtaining data
User Settings -> Privacy & Safety -> Request all of my Data

Extract the zip into a folder.

## Running
```bash
chmod 755 ./discordwords.py
./discordwords.py /path/to/discord/data/
```

### Dependencies
* Plotly
* Pandas
* Wordcloud + matplotlib (Only if using wordcloud option)


### Example commands

#### Plot a bar chart
```bash
./discordwords.py /path/to/discord/data/ -b
```

#### Plot a word cloud
```bash
./discordwords.py /path/to/discord/data/ -c
```

#### Plot a time series
```bash
./discordwords.py /path/to/discord/data/ -t
```

#### Plot a timeseries of messages between Jan 01 2018 and Jan 01 2019
```bash
./discordwords.py /path/to/discord/data/ -t -s 2018-01-01 -e 2019-01-01
```

## Example output
Word Cloud:

![Word Cloud Output](./screenshots/wordcloud.png)

Bar Chart:
![Bar Chart Output](./screenshots/barchart.png)

Time Series:
![Time Series Output](./screenshots/timeseries.png)



## TODO:
~~Implement datetime selector~~

Implement some sort of server-specific view for messages
