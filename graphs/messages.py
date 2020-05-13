import pandas as pd
import re
from pytz.exceptions import UnknownTimeZoneError
from time import tzname


ltz = ''.join(re.findall('([A-Z])', tzname[0]))


def perDay(acsv):
	ddf = acsv.copy()
	ddf['Timestamp'] = pd.to_datetime(ddf['Timestamp']).dt.normalize()                   #remove time, keep date
	ddf['Timestamp'] = ddf['Timestamp'].dt.day_name()                                    #convert to day of week

	series = ddf['Timestamp'].value_counts()
	ddf = pd.DataFrame({'Timestamp': series.index, 'Count': series.values})

	dow=('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')
	return ddf.groupby(['Timestamp']).sum().reindex(dow).fillna(0.0)

def perHour(acsv):
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
	return hdf.groupby(['Timestamp']).sum().reindex(hod).fillna(0.0)
