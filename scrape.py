import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import dates
from dateutil import rrule
from pysolar import solar
from pysolar import radiation
import pytz
import time
import io

# To use this in ipython, start with command:
#
#   frameworkpython -m IPython

DAILY_HISTORY_URL = 'http://www.wunderground.com/weatherstation/WXDailyHistory.asp'
USER_AGENT = 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'

def get_rainfall_day(station, date):
    params = {
      'ID' : station,
      'day': date.day,
      'month': date.month,
      'year': date.year,
      'graphspan': 'day',
      'format': 1
    }

    headers = {
      'User-agent': USER_AGENT
    }

    response = requests.get(DAILY_HISTORY_URL, params=params, headers=headers)

    # Clean up weird wunderground formatting
    data_csv = response.text
    # remove leading newline
    data_csv = data_csv.strip('\n')
    # fix normal line endings
    data_csv = data_csv.replace('\n<br>\n', '\n')
    # fix header line ending
    data_csv = data_csv.replace('<br>\n', '\n')
    # remove trailing commas
    data_csv = data_csv.replace(',\n', '\n')

    # TODO: remove last empty line which looks like '<br>,,,,,,,,,,,,,,,\n'

    df = pd.read_csv(io.StringIO(data_csv), index_col='Time', parse_dates=True)
    return df

def get_rainfall(station, start_date, end_date):
    dfs = []
    dates = list(rrule.rrule(rrule.DAILY, dtstart=start_date, until=end_date))
    backoff_time = 10
    for date in dates:
        if date.day % 10 == 0:
            print("Working on date: {}".format(date))
        done = False
        while not done:
            try:
                weather_data = get_rainfall_day(station, date)
                done = True
            except ConnectionError as e:
                # May get rate limited by Wunderground.com, backoff if so.
                print("Got connection error on {}".format(date))
                print("Will retry in {} seconds".format(backoff_time))
                time.sleep(backoff_time)
        dfs.append(weather_data)

    df = pd.concat(dfs)
    df.to_csv("{}_rainfall_hourly.csv".format(station))
    return df

def load_csv(path, tz_name='US/Pacific'):
  df = pd.read_csv(path, parse_dates=True, index_col='DateUTC')
  df.index = df.index.tz_localize('UTC').tz_convert(tz_name)
  df.index = df.index.rename('Datetime (US/Pacific)')
  df.loc[df.TemperatureF < 0, ['TemperatureF']] = np.nan
  return df

class SolarRadiationAtPlace(object):

  def __init__(self, lat, lng):
    self.lat = lat
    self.lng = lng

  def __call__(self, dt):
    try:
      alt = solar.get_altitude(self.lat, self.lng, dt)
      if alt < 0:
        return 0
      return radiation.get_radiation_direct(dt, alt)
    except OverflowError:
      return None

def add_solar_radiation(df, lat, lng):
  df_solar = df.copy()
  df_solar['SolarRadiation'] = df.index.map(SolarRadiationAtPlace(lat, lng))
  return df_solar

def resample_hourly(df):
  # deafult take first
  agg_methods = dict(zip(df.columns, ['first'] * len(df.columns)))

  # for point samples take average
  avg_cols = [
      'DewpointF',
      'Humidity',
      'PressureIn',
      'TemperatureF',
      'WindDirectionDegrees',
      'WindSpeedMPH'
  ]
  agg_methods.update(dict(zip(avg_cols, ['mean'] * len(df.columns))))

  # handle cumulatives and maxes
  agg_methods.update({
      'HourlyPrecipIn' : 'last',
      'WindSpeedGustMPH' : 'max',
      'dailyrainin' : 'last'
  })

  return df.resample('1H').agg(agg_methods)


def pivot_day_of_year(s):
  return pd.pivot(index=s.index.date,columns=s.index.time,values=s)


def plot_hours(df_daily, start, step):
  df_daily[df_daily.columns[start:24:step]].plot(linestyle=' ', marker='o',
      grid=True, fillstyle='none')


def plot_temp_vs_day_of_year_by_hour(df):
    df_hourly = resample_hourly(df)
    plot_hours(pivot_day_of_year(df_hourly.TemperatureF), 4, 4)


def plot_temp_vs_hour_by_month(df):
    df.pivot_table(
        index=df.index.hour,
        columns=df.index.month,
        values='TemperatureF',
        aggfunc=np.mean).plot()


def plot_rain_vs_month_by_year(df, cumulative=False):
    rain_series = df.dailyrainin.resample('1d').last().resample('1M').sum()
    rain_df = pd.DataFrame(rain_series)
    rain_pivot = rain_df.pivot_table(
      index=rain_df.index.month,
      columns=rain_df.index.year,
      values='dailyrainin')

    # reorder index
    start_mo = 10 # water year starts October 1
    water_year_months = list((start_mo - 1 + x) % 12 + 1 for x in range(12))
    rain_pivot_water_year = rain_pivot.reindex(water_year_months)
    if cumulative:
        rain_pivot_water_year = rain_pivot_water_year.cumsum()
    rain_pivot_water_year.plot(kind='bar')

def plot_temp_and_solar(df_solar, num_points, plot_pressure=False,
        plot_wind=False):
    # store first axis
    df_plot = df_solar.iloc[-num_points:,:]
    ax = df_plot.SolarRadiation.plot()
    df_plot.TemperatureF.plot(secondary_y=True)
    if plot_pressure:
        df_plot.PressureIn.plot(secondary_y=True)
    if plot_wind:
        df_plot.WindSpeedMPH.plot(secondary_y=True)
    tzinfo = pytz.timezone('US/Pacific')
    ax.xaxis.set_minor_locator(dates.HourLocator(interval=6))
    ax.xaxis.grid(True, which='minor')

def subplots(df, columns, start_date, end_date):
    df_plot = df[(df.index >= start_date) & (df.index < end_date)]
    fig, axes = plt.subplots(nrows=len(columns), ncols=1, sharex=True)
    tzinfo = pytz.timezone('US/Pacific')
    for col, ax in zip(columns, axes):
        df_plot[col].plot(ax=ax, label=col)
        ax.legend(loc='upper right')
        ax.xaxis.set_minor_locator(dates.HourLocator(interval=6))
        ax.xaxis.grid(True, which='minor')

def generate_all_figs(path):
    df = load_csv(path)

    plot_temp_vs_day_of_year_by_hour(df)
    plt.savefig('temp-vs-day-of-year-by-hour.pdf')
    plt.clf()

    plot_temp_vs_hour_by_month(df)
    plt.savefig('temp-vs-hour-by-month.pdf')
    plt.clf()

    plot_rain_vs_month_by_year(df, cumulative=False)
    plt.savefig('rain-vs-month-by-year.pdf')
    plt.clf()

    plot_rain_vs_month_by_year(df, cumulative=True)
    plt.savefig('cumulative-rain-vs-month-by-year.pdf')
    plt.clf()
