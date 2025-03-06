import pandas as pd
import numpy as np
from main import findNearestCoordinatesInKm

df = pd.read_csv('bol_cities/bol_cities_expanded.csv')
unique_lat_lon = df[['lat','lon']].drop_duplicates()
month_range = pd.date_range('2000-01-01','2024-01-01',freq='M').strftime('%Y%m')

for month in month_range:
    df[month] = 0
    pm_df = pd.read_csv(f'PM25_monthly/{month}.csv')
    pm_df.loc[pm_df[np.abs(pm_df['lon'])<0.0001].index,'lon'] = 0
    pm_df.loc[pm_df[np.abs(pm_df['lat'])<0.0001].index,'lat'] = 0
    for lat,lon in unique_lat_lon.values:
        nearestLocation = findNearestCoordinatesInKm(lat,lon)
        index = pm_df[(pm_df['lon']==nearestLocation['lon']) & (pm_df['lat']==nearestLocation['lat'])].index[0]
        df.loc[(df['lat'] == lat) & (df['lon'] == lon),month] = pm_df.loc[index,'MERRA2_CNN_Surface_PM25']


df.to_csv('bol_cities/bol_cities_pm25.csv',index=False)


