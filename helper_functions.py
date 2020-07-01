import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.metrics import mean_squared_error

import dask.dataframe as dd
from dask.distributed import Client, progress


def read_data_dask(months):   
    """
    function to read s3 files into dask dataframe
    """
    client = Client(n_workers=4, threads_per_worker=2) #, memory_limit='1GB')
    df_list = []
    first = True

    for month in months:    
        if str(month)[:4] in ['2015', '2016']:
            url = f'https://s3.amazonaws.com/tripdata/{month}-citibike-tripdata.zip'
        else:
            url = f'https://s3.amazonaws.com/tripdata/{month}-citibike-tripdata.csv.zip'

        if first:   
            first = False
            df = dd.read_csv(url,
                             compression = 'zip',
                             parse_dates = ['starttime','stoptime'],
                             infer_datetime_format = True,
                             #usecols=cols,
                             #blocksize=int(128e6),
                             #storage_options=dict(anon=True),
                             assume_missing=True)
            cols = list(df.columns)        
        else:
            df = dd.read_csv(url,
                             compression = 'zip',
                             header = 0,
                             names=cols,
                             parse_dates = ['starttime','stoptime'],
                             infer_datetime_format = True,
                             assume_missing=True)
        df_list.append(df)

    return dd.concat(df_list)

def count_missing_dates(df):
    """
    Top 10 stations with the highest number of missing dates
    """
    
    station_missing_dates = {}
    for i in df.station_id.unique():
        count_dates = len(df[df.station_id == i].date.unique()) 
        missing_dates = (5*365+1) - count_dates # add one day for leap year in 2016
        station_missing_dates[i] = missing_dates

    station_missing_dates_sorted = sorted(station_missing_dates, key=station_missing_dates.get, reverse=True)
    # print("Top 10 stations with the highest # of missing dates:")
    # for r in station_missing_dates_sorted[0:10]:
    #    print("station id", r, ": ", station_missing_dates[r])
    
    return station_missing_dates, station_missing_dates_sorted


# def eval_metrics(y_true, y_pred):
#     """
#     Calculate MAE, MAPE and RMSE given y true and y predictions
#     """
#     return np.mean(np.abs(y_pred - y_true)) 
#     y_true, y_pred = np.array(y_true), np.array(y_pred)
#     return np.mean(np.abs((y_true - y_pred) / y_true)) * 100
#     return np.sqrt(mean_squared_error(y_true, y_pred))  


def mae(y_true, y_pred):
    """
    Calculate mean absolute error (MAE) given y true and y predictions
    """
    return np.mean(np.abs(y_pred - y_true)) 

def mape(y_true, y_pred): 
    """
    Calculate mean absolute percentage error (MAPE) given y true and y predictions
    """    
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    return np.mean(np.abs((y_true - y_pred) / y_true)) * 100

def rmse(y_true, y_pred):
    """
    Calculate root mean squared error (RMSE) given y true and y predictions
    """    
    return np.sqrt(mean_squared_error(y_true, y_pred))  

def is_weekend(ds):
    """
    Add additional seasonality as whether day is sat or sun
    """
    date = pd.to_datetime(ds)
    return (date.weekday() == 5 or date.weekday() == 6)
    
def is_warm(ds):
    """
    Add additional seasonality as whether day is in warm month (may-sept)
    """
    date = pd.to_datetime(ds)
    return (date.month in [5,6,7,8,9])

def plot_forecast(y_true, y_pred, title='Forecast of bike usage volume from station', 
                  xlabel='Day #', ylabel='# of Citibike Rides', 
                  grid=True, figsize=(10, 6)):
    """
    Plot predictions against actuals and print metric results
    """ 
    if len(y_true) != len(y_pred):
        return_str = 'Unequal lengths error: '
        return_str += '({} actual, '.format(len(y_true))
        return_str += '{} forecast)'.format(len(y_pred))
        return return_str
    
    fig, ax = plt.subplots(figsize=figsize)
    ax.plot(range(1,365+1), y_true, color='darkorange')
    ax.plot(y_pred, color='teal', linestyle='--')
    ax.set_facecolor('whitesmoke')
    ax.title.set_text(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.legend(['Actual','Predictions']);