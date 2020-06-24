def read_data_dask(months):   
    """
    function to read s3 files into dask dataframe
    """
    import dask.dataframe as dd
    from dask.distributed import Client, progress
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

def count_missing_dates():
    """
    stations with the highest number of missing dates
    """
    station_missing_dates = {}
    for i in citibike_df.station_id.unique():
        count_dates = len(citibike_df[citibike_df.station_id == i].date.unique()) 
        missing_dates = (5*365+1) - count_dates # add one day for leap year in 2016
        station_missing_dates[i] = missing_dates

    station_missing_dates_sorted = sorted(station_missing_dates, key=station_missing_dates.get, reverse=True)
    print("stations with the highest # of missing dates:")
    for r in station_missing_dates_sorted[0:10]:
        print("station id", r, ": ", station_missing_dates[r])