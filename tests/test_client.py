from unittest import TestCase
from watttime_client.client import WattTimeAPI
from datetime import datetime, timedelta
import pandas as pd
import pytz
import os


class TestAPIClient(TestCase):
    def setUp(self):
        WATTTIME_API_TOKEN = os.environ.get('WATTTIME_API_TOKEN')
        self.impacter = WattTimeAPI(token=WATTTIME_API_TOKEN)
        self.start_at = datetime(2014, 9, 2, 23, tzinfo=pytz.utc)
        self.end_at = datetime(2014, 9, 3, 2, tzinfo=pytz.utc)
        self.early_date = datetime(1914, 9, 2, 23, tzinfo=pytz.utc)
        self.caiso_start = datetime(2015, 1, 2, 23, tzinfo=pytz.utc)
        self.caiso_end = datetime(2015, 1, 3, 2, tzinfo=pytz.utc)
        self.n_expected_1hr = (self.end_at - self.start_at).total_seconds() / 3600 + 1
        self.n_expected_5m = (self.end_at - self.start_at).total_seconds() / 300 + 1

    def tearDown(self):
        self.impacter.cache.clear()

    def test_init_requires_token(self):
        self.assertRaises(ValueError, WattTimeAPI)

    def test_fetch_takes_args(self):
        # error with no args
        with self.assertRaises(TypeError):
            self.impacter.fetch()

        # error with one datetime arg
        with self.assertRaises(TypeError):
            self.impacter.fetch(self.start_at)

        # error with two datetime args
        with self.assertRaises(TypeError):
            self.impacter.fetch(self.start_at, self.end_at)

        # no error with two datetime args and one BA
        result = self.impacter.fetch(self.start_at, self.end_at, ba='MISO', market='RT5M')
        self.assertIsNotNone(result)

    def test_fetch_times(self):
        """Times are between start and end"""
        times, impacts = self.impacter.fetch(self.start_at, self.end_at, ba='PJM', market='RT5M')

        # times in range
        self.assertGreaterEqual(times[0], self.start_at)
        self.assertLessEqual(times[-1], self.end_at)

        # right number of times
        self.assertEqual(len(times), self.n_expected_5m)

        # times are sorted
        self.assertEqual(sorted(times), times)

    def test_fetch_impacts(self):
        """Expected impact values are received"""
        times, impacts = self.impacter.fetch(self.start_at, self.end_at, ba='PJM', market='RT5M')

        # right number of values
        self.assertEqual(len(impacts), self.n_expected_5m)

        # values in range
        for val in impacts:
            self.assertGreater(val, 1500)
            self.assertLess(val, 2200)

    def test_fetch_impacts_notnull(self):
        """No null impact values"""
        times, impacts = self.impacter.fetch(self.caiso_start, self.caiso_end,
                                             ba='CAISO', market='RT5M')

        # values in range
        self.assertGreater(len(impacts), 0)
        for val in impacts:
            self.assertIsNotNone(val)

    def test_cache_key_unique_ba(self):
        """Cache key unique on BA"""
        key1 = self.impacter.cache_key(self.start_at, 'ba1', 'market')
        key2 = self.impacter.cache_key(self.start_at, 'ba2', 'market')
        self.assertNotEqual(key1, key2)

    def test_cache_key_unique_year(self):
        """Cache key unique on year"""
        key1 = self.impacter.cache_key(self.start_at, 'ba', 'market')
        key2 = self.impacter.cache_key(self.start_at - timedelta(days=365), 'ba', 'market')
        self.assertNotEqual(key1, key2)

    def test_cache_key_unique_month(self):
        """Cache key unique on month"""
        key1 = self.impacter.cache_key(self.start_at, 'ba', 'market')
        key2 = self.impacter.cache_key(self.start_at - timedelta(days=32), 'ba', 'market')
        self.assertNotEqual(key1, key2)

    def test_cache_key_unique_day(self):
        """Cache key unique on day"""
        key1 = self.impacter.cache_key(self.start_at, 'ba', 'market')
        key2 = self.impacter.cache_key(self.start_at - timedelta(days=1), 'ba', 'market')
        self.assertNotEqual(key1, key2)

    def test_cache_key_unique_market(self):
        """Cache key unique on day"""
        key1 = self.impacter.cache_key(self.start_at, 'ba', 'market1')
        key2 = self.impacter.cache_key(self.start_at, 'ba', 'market2')
        self.assertNotEqual(key1, key2)

    def test_get_impact_start(self):
        value = self.impacter.get_impact_at(self.start_at, 'PJM')
        times, impacts = self.impacter.fetch(self.start_at, self.end_at, ba='PJM', market='RT5M')
        self.assertEqual(value, impacts[0])

    def test_get_impact_end(self):
        value = self.impacter.get_impact_at(self.end_at, 'PJM')
        times, impacts = self.impacter.fetch(self.start_at, self.end_at, ba='PJM', market='RT5M')
        self.assertEqual(value, impacts[-1])

    def test_get_impact_date_outofrange(self):
        value = self.impacter.get_impact_at(self.early_date, 'PJM')
        self.assertIsNone(value)

    def test_get_impact_modify_cache(self):
        # set times on and off hour
        on_hr_ts = self.start_at.replace(minute=0)
        off_hr_ts = self.start_at.replace(minute=10)

        # check in same cache set
        on_hr_key = self.impacter.cache_key(on_hr_ts, 'PJM', 'RT5M')
        off_hr_key = self.impacter.cache_key(off_hr_ts, 'PJM', 'RT5M')
        self.assertEqual(on_hr_key, off_hr_key)

        # get impact
        value_pre = self.impacter.get_impact_at(off_hr_ts, ba='PJM', market='RT5M')

        # overwrite cache with just the earlier timestamp and fake value
        fake_cached_data = {on_hr_ts: -1000}
        self.impacter.cache.set(on_hr_key, fake_cached_data)

        # get again, should be correct value
        value_post = self.impacter.get_impact_at(off_hr_ts, ba='PJM', market='RT5M')
        self.assertEqual(value_pre, value_post)

    def test_get_impact_between(self):
        series = self.impacter.get_impact_between(self.start_at, self.end_at,
                                                  interval_minutes=5, ba='PJM')
        times, impacts = self.impacter.fetch(self.start_at, self.end_at,
                                             ba='PJM', market='RT5M')
        for i in range(len(times)):
            self.assertEqual(series.at[times[i]], impacts[i])

    def test_get_impact_between_ffill(self):
        # get data
        series_pre = self.impacter.get_impact_between(self.start_at, self.end_at,
                                                      interval_minutes=5, ba='PJM')
        self.assertIsNotNone(series_pre[-1])

        # fake cache with null value
        self.impacter.insert_to_cache(self.end_at, 'PJM', 'RT5M', None)

        # get data again, forward fills
        series_post = self.impacter.get_impact_between(self.start_at, self.end_at,
                                                       interval_minutes=5, ba='PJM')
        self.assertEqual(series_pre[-1], series_post[-1])

        # get data again, no fill
        series_na = self.impacter.get_impact_between(self.start_at, self.end_at,
                                                     interval_minutes=5, ba='PJM',
                                                     fill=False)
        self.assertTrue(pd.isnull(series_na[-1]))

    def test_get_impact_between_caiso(self):
        series = self.impacter.get_impact_between(self.caiso_start, self.caiso_end,
                                                  interval_minutes=5, ba='CAISO',
                                                  fill=False)

        # no null values
        self.assertFalse(pd.isnull(series).any())

    def test_get_impact_between_diff_tz(self):
        """Avoid ValueError: Start and end cannot both be tz-aware with different timezones"""
        other_tz_end = self.caiso_end.astimezone(pytz.timezone('US/Pacific'))
        series = self.impacter.get_impact_between(self.caiso_start, other_tz_end,
                                                  interval_minutes=5, ba='CAISO',
                                                  fill=False)
        self.assertIsNotNone(series)

    def test_get_impact_between_naive(self):
        naive_start = self.caiso_start.replace(tzinfo=None)
        naive_end = self.caiso_end.replace(tzinfo=None)
        series = self.impacter.get_impact_between(naive_start, naive_end,
                                                  interval_minutes=5, ba='CAISO',
                                                  fill=False)
        self.assertIsNotNone(series)

    def test_get_impact_naive_aware(self):
        naive_start = self.caiso_start.replace(tzinfo=None)
        self.assertRaises(ValueError, self.impacter.get_impact_between,
                          naive_start, self.caiso_end, interval_minutes=5, ba='CAISO', fill=False)
