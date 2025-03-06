import pandas as pd
import numpy as np
import xarray as xr
from pathlib import Path
import re
import glob
import math
import sys
import datetime
import tqdm
import os
import requests
import json
from typing import Dict,List,Union





def getDistanceFromLatLonInKm(lat1,lon1,lat2,lon2):
    """
        this function use  Haversine formula  to calculate the distance between two coordinates.

     Args:
        lat1: The latitude of the first coordinate.
        lon1: The longitude of the first coordinate.
        lat2: The latitude of the second coordinate.
        lon2: The longitude of the second coordinate.

    Returns:
        The distance between the two coordinates in kilometers.

    Refrences: 
     - https://www.movable-type.co.uk/scripts/latlong.html
     - https://en.wikipedia.org/wiki/Haversine_formula
    """

    radius = 6371 # radius of earth in kilometer
    dlat = math.radians(lat2 - lat1) # convert degree to radian
    dlon = math.radians(lon2 - lon1) # convert degree to radian


    # calculate a in https://www.movable-type.co.uk/scripts/latlong.html:  a = sin²(Δφ/2) + cos φ1 ⋅ cos φ2 ⋅ sin²(Δλ/2)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    # calculate c in https://www.movable-type.co.uk/scripts/latlong.html: c = 2 ⋅ atan2( √a, √(1−a) )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    distance = radius * c

    return distance





def get4NearestPoints(lat,lon,lat_range,lon_range):
    """ Returns the four nearest latitude and longitude coordinates to the given latitude and longitude.

    Args:
        lat: The latitude coordinate.
        lon: The longitude coordinate.

    Returns:
        A tuple of two lists, containing the four nearest latitude and longitude coordinates.
    """
    if not (len(lat_range) and len(lon_range)):
        lon_range = np.arange(-180,180,0.625)
        lat_range = np.arange(-90,90.5,0.5)


    closest_smaller_lat = lat_range[lat_range < lat].max() if np.any(lat_range<lat) else None
    close_bigger_lat = lat_range[lat_range > lat].min() if np.any(lat_range>lat) else None
    lats = [closest_smaller_lat.round(2),close_bigger_lat.round(2)]

    closest_smaller_lon = lon_range[lon_range < lon].max() if np.any(lon_range < lon) else None
    closest_bigger_lon = lon_range[lon_range > lon].min() if np.any(lon_range > lon) else None
    lons = [closest_smaller_lon.round(3),closest_bigger_lon.round(3)]

    return lats,lons


def findNearestCoordinatesInKm(row,lat_range,lon_range):
    # lat_range = list(nasa_df['lat'])
    # lon_range = list(nasa_df['lon'])
    lat = row['lat']
    lon = row['lng']
    """ Finds the nearest latitude and longitude coordinates to the given latitude and longitude, in kilometers.

        Args:
            lat: The latitude coordinate.
            lon: The longitude coordinate.
            lat_range: The latitude range.
            lon_range: The longitude range.

        Returns:
            A dictionary containing the nearest latitude and longitude coordinates, and the distance to those coordinates in kilometers.
    """
    lats,lons = get4NearestPoints(lat,lon,lat_range,lon_range)
    nearestLatLon = {'lat':None,'lon':None,'distance':10000000}
    for la in lats:
        for lo in lons:
            distance = getDistanceFromLatLonInKm(lat,lon,la,lo)
            if distance < nearestLatLon['distance']:
                nearestLatLon['distance'] = distance
                nearestLatLon['lat'] = la
                nearestLatLon['lon'] = lo
    return pd.Series(nearestLatLon)



def find_the_index_of_csv_other(nearestLocation):
    PM25_df = pd.read_csv('PM25/20050101.csv')
    PM25_df.loc[PM25_df[np.abs(PM25_df['lon'])<0.0001].index,'lon'] = 0
    PM25_df.loc[PM25_df[np.abs(PM25_df['lat'])<0.0001].index,'lat'] = 0
    PM25_df = PM25_df[(PM25_df['lon']==nearestLocation['lon']) & (PM25_df['lat']==nearestLocation['lat'])]
    index = PM25_df.index[0]

    return index
def find_the_index_of_csv_RH(nearestLocation):
    df = pd.read_csv('RH/20050101.csv')
    df.loc[df[np.abs(df['lon'])<0.0001].index,'lon'] = 0
    df.loc[df[np.abs(df['lat'])<0.0001].index,'lat'] = 0
    df = df[(df['lon']==nearestLocation['lon']) & (df['lat']==nearestLocation['lat'])]
    index = df.index[0]

    return index





def create_cookies(cookie_text):
    """
        This function get cookies in text format and then extracts it's keys and values in dictionary format
    """
    cookies: dict = {}
    for cookie_variable in cookie_text.split(';'):
        cookies[cookie_variable.split(
            '=')[0].strip()] = cookie_variable.split("=")[1].strip()
    return cookies

def create_pandas_dataframe_and_delete_file_O3(name, date):
    # Open the ncf4 file
    ds = xr.open_dataset(name)

    # Convert it to dataframe
    df = ds.to_dataframe()
    # get only the useful columns
    # ['PS', 'DELP', 'T', 'U', 'V', 'QV', 'O3']

    # remove time from index in order to use groupby
    df.reset_index(['time'], inplace=True)

    df = df.groupby(['lon', 'lat','lev']).mean()
    df.drop("time", inplace=True, axis=1)
    df.reset_index(['lev'], inplace=True)

    df = df.groupby(['lon', 'lat']).mean()
    df.drop("lev", inplace=True, axis=1)

    print(f'start writing O3/{date}.csv')
    df.to_csv(Path(f'O3/{date}.csv'))
    print(f'finish writing O3/{date}.csv')
    os.remove(name)
def create_pandas_dataframe_and_delete_file_T2(name, date):
    # Open the ncf4 file
    ds = xr.open_dataset(name)

    # Convert it to dataframe
    df = ds.to_dataframe()
    # get only the useful columns

    df=df[['T2MMEAN','TPRECMAX']]

    print(f'start writing T2/{date}.csv')
    df.to_csv(Path(f'T2/{date}.csv'))
    print(f'finish writing T2/{date}.csv')
    os.remove(name)
def create_pandas_dataframe_and_delete_file_PRECTOT(name, date):
    # Open the ncf4 file
    ds = xr.open_dataset(name)

    # Convert it to dataframe
    df = ds.to_dataframe()
    # get only the useful columns

    df=df[['PRECTOT']]

    df.reset_index('time',inplace=True)
    df = df.groupby(['lon','lat']).mean()
    df.drop('time',inplace=True,axis=1)

    print(f'start writing RainFallMonthly/{date}.csv')
    df.to_csv(Path(f'RainFallMonthly/{date}.csv'))
    print(f'finish writing RainFallMonthly/{date}.csv')
    os.remove(name)
def create_pandas_dataframe_and_delete_file_Rainfall(name, date):
    # Open the ncf4 file
    ds = xr.open_dataset(name)

    # Convert it to dataframe
    df = ds.to_dataframe()
    df.reset_index(level=['time','bnds'],inplace=True)
    df = df[df['bnds']==1]
    # get only the useful columns
    df=df[['Rainf_f_tavg', 'Tair_f_tavg', 'Qtotal_tavg']]
    
    print(f'start writing Rainf/{date}.csv')
    df.to_csv(Path(f'Rainf/{date}.csv'))
    print(f'finish writing Rainf/{date}.csv')
    os.remove(name)

def create_pandas_dataframe_and_delete_file_Rainfall2(name, date):
    # Open the ncf4 file
    ds = xr.open_dataset(name)

    # Convert it to dataframe
    df = ds.to_dataframe()
    df.reset_index(level=['time','bnds'],inplace=True)
    df = df[df['bnds']==1]
    # get only the useful columns
    df=df[['Rainf_f_tavg', 'Rainf_tavg']]
    print(f'start writing Rainf2/{date}.csv')
    df.to_csv(Path(f'Rainf2/{date}.csv'))
    print(f'finish writing Rainf2/{date}.csv')
    os.remove(name)
def create_pandas_dataframe_and_delete_file_PM25(name, date):

    # Open the ncf4 file
    ds = xr.open_dataset(name)

    # Convert it to dataframe
    df = ds.to_dataframe()

    df = df[['DUSMASS25','OCSMASS','BCSMASS','SSSMASS25','SO4SMASS']]
    df.reset_index(level=['time'],inplace=True)
    df = df.groupby(['lon','lat']).mean()
    df.drop('time',inplace=True,axis=1)
    df['PM25'] = df['DUSMASS25'] + df['OCSMASS'] + df['BCSMASS'] + df['SSSMASS25'] + df['SO4SMASS']*(132.14/96.06)
    df = df[['PM25']]
    print(f'start writing PM25/{date}.csv')
    df.to_csv(Path(f'PM25/{date}.csv'))
    print(f'finish writing PM25/{date}.csv')
    os.remove(name)

