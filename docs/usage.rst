Usage
=====

Set up the token
----------------

If you have set your API token as an environment variable (see :doc:`token`),
then you should import it from the environment::

   >>> import os
   >>> mytoken = os.environ.get('WATTTIME_API_TOKEN')


Create a client
---------------

Import the client class, and create an instance with your token::

   >>> from watttime_client.client import WattTimeAPI
   >>> client = WattTimeAPI(token=mytoken)

You can reuse this client for multiple requests to take advantage of
internal caching, which will make your code faster and reduce load
on the WattTime server.


Get marginal carbon data
------------------------

There are two main ways to get data.

If you want the marginal carbon value at just one time, use ``get_impact_at``.
This method takes a timezone-aware datetime, the name of a balancing authority,
and the name of a market (real-time by default).
It returns the numerical value of the marginal carbon emissions due to electricity usage
at that place and time, in lb/MWh::

   >>> from datetime import datetime
   >>> import pytz
   >>> timestamp = pytz.utc.localize(datetime(2015, 6, 1, 12, 30))
   >>> value = client.get_impact_at(timestamp, 'CAISO')
   >>> print value
   909.2

If you want the marginal carbon value at a range of times, use ``get_impact_between``.
This method takes a pair of timezone-aware datetimes for the start and end times,
the interval length between readings (in minutes),
the name of a balancing authority, and the name of a market (real-time by default).
It returns a pandas Series containing
the marginal carbon emissions values for electricity usage
at that place and times, in lb/MWh::

   >>> start_time = pytz.utc.localize(datetime(2015, 6, 1, 12, 30))
   >>> end_time = pytz.utc.localize(datetime(2015, 6, 1, 18, 30))
   >>> interval_min = 5
   >>> data = client.get_impact_between(start_time, end_time, interval_min, 'CAISO')
   >>> print data.head()
   2015-06-01 12:30:00+00:00    909.2
   2015-06-01 12:35:00+00:00    909.2
   2015-06-01 12:40:00+00:00    920.5
   2015-06-01 12:45:00+00:00    920.5
   2015-06-01 12:50:00+00:00    920.5
   Freq: 5T, dtype: float64


Analyzing data
--------------

Once you have the data in `pandas <http://pandas.pydata.org/pandas-docs/stable/>`_,
you can use its full power in your data analysis.
Or if you'd rather export it, use any of the `pandas I/O tools <http://pandas.pydata.org/pandas-docs/stable/io.html>`_.
For instance, to export to csv::

   >>> output_file_name = 'awesome_carbon_data.csv'
   >>> data.to_csv(output_file_name)
