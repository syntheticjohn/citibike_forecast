"""
Forecasting Citi Bike demand --
Script for data preprocessing and feature engineering
Used to create aggregated df / pickles to be used for model creation
This script is designed to be run on a remote cloud instance, 
either through a Jupyter notebook or the command line
"""

import numpy as np
import pandas as pd
from datetime import datetime
import datetime as dt
import random
from collections import Counter
import pickle
import sys
import importlib

import dask
import dask.dataframe as dd
from dask.distributed import Client, progress

from helper_functions import read_data_dask, count_missing_dates

# files to read in based on month-year from: https://s3.amazonaws.com/tripdata/index.html
months = [201501, 201502, 201503, 201504, 201505, 201506, 201507, 201508, 201509, 201510, 201511, 201512, 
          201601, 201602, 201603, 201604, 201605, 201606, 201607, 201608, 201609, 201610, 201611, 201612, 
          201701, 201702, 201703, 201704, 201705, 201706, 201707, 201708, 201709, 201710, 201711, 201712, 
          201801, 201802, 201803, 201804, 201805, 201806, 201807, 201808, 201809, 201810, 201811, 201812,
          201901, 201902, 201903, 201904, 201905, 201906, 201907, 201908, 201909, 201910, 201911, 201912]

# read in data through dask
citibike_transactions = read_data_dask(months)

### Data Cleaning
# clean column names
citibike_transactions.columns = citibike_transactions.columns.str.replace(' ', '_')
citibike_transactions.columns = citibike_transactions.columns.str.lower()
citibike_transactions.columns

# remove rows with missing start station values
citibike_transactions = citibike_transactions.dropna(subset = ['start_station_id'])

# store dataframe in parquet
citibike_transactions.to_parquet('citibike_transactions.parquet', engine='fastparquet')
#citibike_transactions = dd.read_parquet('citibike_transactions.parquet', engine='fastparquet') 

### Aggregate data into daily ride counts per station
# aggregate data by date into pandas dataframe
citibike_df = citibike_transactions.groupby([citibike_transactions.starttime.dt.date, 
                                     citibike_transactions.start_station_id, 
                                     citibike_transactions.start_station_name]).agg(
                                            {'start_station_latitude':'mean', 
                                             'start_station_longitude':'mean',
                                             'tripduration':'mean', 
                                             'start_station_id':'size'}).rename(
                                            columns={'start_station_latitude':'start_station_latitude', 
                                                     'start_station_longitude':'start_station_longitude', 
                                                     'tripduration':'mean_duration', 
                                                     'start_station_id':'ride_counts'}).reset_index().compute()

citibike_df.columns = ['date', 'station_id', 'station_name', 'station_latitude', 
                       'station_longitude', 'mean_duration', 'ride_counts']

citibike_df.date = pd.to_datetime(citibike_df.date)
citibike_df.station_id = citibike_df.station_id.astype(int)

# pickle dataframe
citibike_df.to_pickle('data/citibike_df_raw.pkl')

# top 10 stations with the most missing dates
station_missing_dates, station_missing_dates_sorted = count_missing_dates(citibike_df) 

# number of stations that existed since January 2015 and still exist in December 2019
stations_jan2015 = citibike_df[citibike_df.date < '2015-02-01'].station_id.unique()
stations_dec2019 = citibike_df[citibike_df.date > '2019-11-30'].station_id.unique()
stations = np.intersect1d(stations_jan2015, stations_dec2019)

# filter dataframe down to the stations that existed since January 2015
citibike_df = citibike_df[citibike_df.station_id.isin(stations)]

# top 10 stations with the most missing dates
station_missing_dates, station_missing_dates_sorted = count_missing_dates(citibike_df) 

# exclude top 10 stations with missing dates 
stations = station_missing_dates_sorted[11:]

# filter dataframe down to the stations that existed since January 2015
citibike_df = citibike_df[citibike_df.station_id.isin(stations)]

# top 10 stations with the most missing dates
station_missing_dates, station_missing_dates_sorted = count_missing_dates(citibike_df) 

# add in missing dates for each station with ride counts as zero
citibike_df = citibike_df.reset_index(drop=True)
citibike_df = citibike_df.set_index(['date', 'station_id']).unstack(
    fill_value=0).asfreq('D', fill_value=0).stack().sort_index(level=1).reset_index()

### Aggregate ride counts for each station by year-month
# aggregate number of missing dates for every month-year by station
citibike_grouped = citibike_df[citibike_df.ride_counts == 0].groupby(
    [citibike_df.station_id, citibike_df.date.dt.year, citibike_df.date.dt.month]).date.agg(
    count_missing_dates=('date', 'nunique'))

citibike_grouped.index.names = ['station_id', 'year', 'month']
citibike_grouped = citibike_grouped.reset_index()

# number of months where month is missing 10 or more days
stations_missing_majority_months = citibike_grouped[citibike_grouped.count_missing_dates > 10].groupby(['station_id']).agg(
    {'count_missing_dates':'size'}).sort_values(
    'count_missing_dates', ascending=False).rename(
    columns={'count_missing_dates':'months_missing_more_than_ten_dates'}).reset_index()

# stations with more than 1 month where month is missing 10 or more days
exclude_stations = stations_missing_majority_months[stations_missing_majority_months.
                                                    months_missing_more_than_ten_dates > 1].station_id

# exclude those stations
citibike_grouped = citibike_grouped[~citibike_grouped.station_id.isin(exclude_stations)]
citibike_df = citibike_df[~citibike_df.station_id.isin(exclude_stations)]

# exclude the stations with missing dates in the last month of the dataset (test set)
exclude_stations = citibike_grouped[(citibike_grouped.year==2019) & 
                                    (citibike_grouped.month.isin([11, 12]))].station_id
citibike_grouped = citibike_grouped[~citibike_grouped.station_id.isin(exclude_stations)]
citibike_df = citibike_df[~citibike_df.station_id.isin(exclude_stations)]

# create year and month columns
citibike_df.loc[:, 'year'] = citibike_df.date.dt.year
citibike_df.loc[:, 'month'] = citibike_df.date.dt.month

# reorder columns
citibike_df = citibike_df[['date', 'station_id', 'station_name', 'station_latitude', 'station_longitude',
                           'mean_duration', 'year', 'month', 'ride_counts']]

# add new counts column that replace zero ride counts with rolling 90-day median value per station
citibike_df['replace_zeroes'] =     citibike_df.groupby('station_id')['ride_counts'].rolling(
    window=90, min_periods=1).median().shift(1).apply(np.floor).reset_index(0, drop=True)

citibike_df['ride_counts_clean'] = citibike_df.ride_counts
citibike_df.loc[citibike_df.ride_counts != 0, ['replace_zeroes']] = np.nan
citibike_df.loc[citibike_df.ride_counts == 0, ['ride_counts_clean']] = citibike_df.replace_zeroes

# convert ride counts imputed to int datatype
citibike_df.ride_counts_clean = citibike_df.ride_counts_clean.astype(int) 

### Additional columns and cleanup
# add day of week in dataframe
citibike_df['day_of_week'] = citibike_df.date.dt.weekday

# add station info
station_info = citibike_df[(citibike_df.station_name != 0) & 
                           (citibike_df.station_latitude != 0) &
                           (citibike_df.station_longitude != 0)].groupby(['station_id', 'station_name', 'station_latitude', 'station_longitude']).size().reset_index().rename(columns={0:'count'}).drop('count', axis=1)

# drop duplicates
station_info.drop_duplicates(subset=['station_id'], inplace=True)

# merge with citibike df
citibike_df = citibike_df.merge(station_info, how='left', left_on='station_id', right_on='station_id')

# drop columns
citibike_df = citibike_df.drop(['station_name_x', 'station_latitude_x', 'station_longitude_x'], axis=1)

# rename columns
citibike_df.columns = ['date', 'station_id', 'mean_duration', 'year', 'month', 'ride_counts',
                         'replace_zeroes', 'ride_counts_clean', 'day_of_week', 
                         'station_name', 'station_latitude', 'station_longitude']
# reorder columns
citibike_df = citibike_df[['date', 'station_id', 'station_name', 'station_latitude', 'station_longitude',
                           'mean_duration', 'year', 'month', 'day_of_week',
                           'ride_counts','replace_zeroes', 'ride_counts_clean']]

# drop some columns and reset index
citibike_df = citibike_df.reset_index(drop=True)

# pickle dataframe
citibike_df.to_pickle('citibike_df_preprocessed.pkl')