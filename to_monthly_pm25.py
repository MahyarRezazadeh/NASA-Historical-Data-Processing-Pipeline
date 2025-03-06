import pandas as pd
import os
from utils import getDistanceFromLatLonInKm,get4NearestPoints,findNearestCoordinatesInKm,find_the_index_of_csv_other,find_the_index_of_csv_RH
import numpy as np
import logging
import glob
from concurrent.futures import ProcessPoolExecutor
import gc
logging.basicConfig(filename='monthlyData.log',level=logging.ERROR)


def convert_daily_data_to_montly():
    if not os.path.exists('TO3_monthly'):
        os.mkdir('TO3_monthly')
    if not os.path.exists('PM25_monthly'):
        os.mkdir('PM25_monthly')

    lon_range = np.arange(-180,180,0.625)
    lat_range = np.arange(-90,90.5,0.5)
    empty_dataframe_data = []
    for lon in lon_range:
        for lat in lat_range:
            empty_dataframe_data.append([lon,lat,0])


    date_range = pd.date_range('2004-06-01','2014-01-01',freq='M').strftime('%Y%m%d') # create date range from 20140101 till 20240101
    for end_month_date in date_range:
        days_in_end_month_date = pd.date_range(end_month_date[:6]+'01',end_month_date).strftime('%Y%m%d')
        # to3_df_month = pd.DataFrame(empty_dataframe_data,columns=['lon','lat','T2M'])
        pm25_df_month = pd.DataFrame(empty_dataframe_data,columns=['lon','lat','MERRA2_CNN_Surface_PM25'])
        # to3_counter = 0
        pm25_counter = 0
        for day in days_in_end_month_date:
            try:
                # get PM25
                PM25_df = pd.read_csv(f"PM25/{day}.csv",usecols=['lon','lat','MERRA2_CNN_Surface_PM25'])
                pm25_df_month['MERRA2_CNN_Surface_PM25'] += PM25_df['MERRA2_CNN_Surface_PM25']
                pm25_counter+=1
            except FileNotFoundError:
                print(f"Error: The file 'PM25/{day}.csv' does not exist.")
                logging.error(f"Error: The file 'PM25/{day}.csv' does not exist.")



        pm25_df_month['MERRA2_CNN_Surface_PM25'] = pm25_df_month['MERRA2_CNN_Surface_PM25']/pm25_counter
        pm25_df_month.to_csv(f'PM25_monthly/{end_month_date[:6]}.csv',index=False)




def merge_cities_with_monthly_data():
    paths = glob.glob('pollution_monthly/*.csv')
    all_df = pd.DataFrame()
    for path in paths:
        df = pd.read_csv(path,index_col=0)
        all_df = pd.concat([all_df,df],axis=0)

    all_df.to_csv('pollution_monthly_data.csv')
    all_df.to_stata('pollution_monthly_data.dta')


def main():
    convert_daily_data_to_montly()


if __name__ == '__main__':
    main()
