import pandas as pd
import plotly.graph_objects as go
import resources.analytics
import resources.df_tools


class Heatmap:
    ONE_DAY = pd.to_timedelta(1, unit='d')
    ONE_SEC = pd.to_timedelta(1, unit='s')

    def __init__(self, path):
        try:
            analytics = resources.analytics.load_directory(path, "analytics")
        except AssertionError:
            analytics = resources.analytics.load_directory(path, "reporting")

        self.df = resources.df_tools.count_timestamp(analytics, interval='H')
        self.df.set_index('timestamp', inplace=True)

        self.start = self.df.index[0].normalize()  # Midnight of first day
        # End of last day
        self.end = self.df.index[-1].normalize() + self.ONE_DAY - self.ONE_SEC
        self.range = pd.date_range(start=self.start, end=self.end, freq='D')

        # Pad start of first day and end of last day
        pad_range = pd.date_range(start=self.start, end=self.end, freq='H')
        self.df = self.df.reindex(pad_range).fillna(0)

    def get_go(self, **kwargs):
        return go.Heatmap(z=self._get_z(),  # Transpose
                          x=list(self.range),
                          y=[str(i) + ':00' for i in range(24)],
                          name=f"Activity/Hour", **kwargs)

    def _get_z(self):
        zt = []
        start = self.start
        end = start + self.ONE_DAY - self.ONE_SEC

        while end <= self.end:
            zt.append(self.df.Count[start:end].values)
            start += self.ONE_DAY
            end += self.ONE_DAY

        z = list(map(list, zip(*zt)))  # Transpose
        return z
