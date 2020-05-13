import pandas as pd
from pytz.exceptions import UnknownTimeZoneError
import re
from time import tzname
import warnings


ltz = ''.join(re.findall('([A-Z])', tzname[0]))


def tz_convert(series):
    utc_series = pd.to_datetime(series).dt.tz_localize('utc')
    return localize_utc(utc_series)


def localize_utc(utc_series):
    try:
        return utc_series.dt.tz_convert(ltz)  # localize timestamp
    except UnknownTimeZoneError as error:
        warnings.warn("Timezone could not be localized, using UTC...")
        return utc_series
