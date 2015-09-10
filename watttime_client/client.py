import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz
import logging


logger = logging.getLogger(__name__)


class LocMemCache(dict):
    """Trivial in-memory cache with Django compatibility"""
    def set(self, key, value):
        self[key] = value


class WattTimeAPI(object):
    def __init__(self, token=None):
        """Require API token"""
        # set token in header
        if token is None:
            raise ValueError('WattTime API token required')
        self.auth_header = {'Authorization': 'Token %s' % token}

        # set up cache
        try:
            from django.core.cache import caches
            self.cache = caches['default']
            logger.debug('Using Django default cache for WattTime API client.')
        except ImportError:
            self.cache = LocMemCache()
            logger.warn('Django cache unavailable to WattTime API client, falling back to local memory cache.')

    def fetch(self, start_at, end_at, ba, market, **kwargs):
        """
        Fetch data from API between start and end dates.
        Gets forecast marginal carbon impact using any kwargs.
        """
        # set up params
        params = {
            'start_at': start_at.isoformat(),
            'end_at': end_at.isoformat(),
            'ba': ba,
            'market': market,
        }
        params.update(kwargs)

        # make request
        result = requests.get('https://api.watttime.org/api/v1/marginal/',
                              params=params, headers=self.auth_header)
        data = result.json()['results']

        n_pages = 1
        while result.json()['next']:
            result = requests.get(result.json()['next'], headers=self.auth_header)
            data += result.json()['results']
            n_pages += 1
        logger.debug('Made %d requests and got %d datapoints for params %s' % (n_pages, len(data), params))

        # pull out data
        sorted_data = sorted(data, key=lambda d: d['timestamp'])
        times = [self.get_timestamp(d) for d in sorted_data]
        values = [self.get_value(d) for d in sorted_data]

        # cache
        ret_times, ret_values = [], []
        for d, v in zip(times, values):
            if v is not None:
                self.insert_to_cache(d, ba, market, v)
                ret_times.append(d)
                ret_values.append(v)

        # return
        return ret_times, ret_values

    def get_impact_at(self, ts, ba, market='RT5M'):
        """
        Get marginal carbon impact for the given timestamp and BA,
        using the RT5M market by default.
        """
        # query cache
        best_cached_time, best_cached_value = self.best_cached_value(ts, ba, market)

        # if got good data, return
        if best_cached_time:
            lag_time = ts - best_cached_time
            if lag_time < timedelta(hours=1) and market == 'DAHR':
                # acceptable lag is 1 hr for hourly data
                return best_cached_value
            elif lag_time < timedelta(minutes=15):
                # acceptable lag is 15 min otherwise
                return best_cached_value

        # if got here, no good data in cache, so fetch it
        times, values = self.fetch(ts - timedelta(hours=4), ts + timedelta(hours=4), ba, market)

        # best value is latest time before or equal to ts
        best_value = None
        for d, v in zip(times, values):
            if d <= ts:
                best_value = v
            else:
                break
        # return
        return best_value

    def get_impact_between(self, start_ts, end_ts, interval_minutes, ba,
                           market='RT5M', fill=True):
        """
        Get a pandas series of the marginal carbon impact
        for the given time range, interval length, and BA,
        using the RT5M market by default.
        By default, forward fills missing data; turn this off with fill=False.
        """
        # utcify
        try:  # aware
            utc_start = start_ts.astimezone(pytz.utc)
            utc_end = end_ts.astimezone(pytz.utc)
        except ValueError:  # naive
            try:
                utc_start = pytz.utc.localize(start_ts)
                utc_end = pytz.utc.localize(end_ts)
            except ValueError:
                raise ValueError('start_ts and end_ts must be both aware or both naive')

        # set up datetime index with correct interval
        dtidx = pd.date_range(utc_start, utc_end, freq='%dMin' % interval_minutes)

        # get cached value for every timestamp
        values = dtidx.map(lambda ts: self.best_cached_value(ts, ba, market)[0])

        # set up series
        series = pd.Series(values, index=dtidx)

        # for uncached values, fill with fetch
        for ts, value in series.where(series.isnull()).iteritems():
            series.at[ts] = self.get_impact_at(ts, ba, market)
            logger.debug('%s %s' % (ts, series.at[ts]))

        # fill any remaining null values
        if fill:
            series = series.ffill()

        # return
        return series

    def get_timestamp(self, d):
        """Extracts an aware UTC datetime from a data dict"""
        naive_dt = datetime.strptime(d['timestamp'], '%Y-%m-%dT%H:%M:%SZ')
        aware_dt = pytz.utc.localize(naive_dt)
        return aware_dt

    def get_value(self, d):
        """Extracts a marginal value from a data dict"""
        try:
            return d['marginal_carbon']['value']
        except (KeyError, TypeError):
            return None

    def cache_key(self, ts, ba, market):
        return ba.upper() + ":" + market.upper() + ":" + ts.strftime('%Y-%m-%d')

    def insert_to_cache(self, ts, ba, market, value):
        # query cache
        cached_data = self.get_from_cache(ts, ba, market)

        # update value
        cached_data.update({ts: value})

        # set cache
        self.cache.set(self.cache_key(ts, ba, market), cached_data)

    def get_from_cache(self, ts, ba, market):
        # query cache
        cache_key = self.cache_key(ts, ba, market)
        cached_data = self.cache.get(cache_key, {})

        # return
        return cached_data

    def best_cached_value(self, ts, ba, market):
        """
        Returns the best cached time/value pair for the arguments,
        or (None, None) if no good value found in cache.
        """
        # query cache
        cached_data = self.get_from_cache(ts, ba, market)

        # if no cache, no best value
        if not cached_data:
            return (None, None)

        # sort times and values
        times = sorted(cached_data.keys())
        values = [cached_data[d] for d in times]

        # best value is latest time before or equal to ts
        best_time, best_value = None, None
        for d, v in zip(times, values):
            if d <= ts:
                best_time = d
                best_value = v
            else:
                break

        # return
        return best_time, best_value
