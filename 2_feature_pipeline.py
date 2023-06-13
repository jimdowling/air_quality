#!/usr/bin/env python

import modal
import datetime
import time
import requests
import pandas as pd
import json
import hopsworks
from functions import *
import warnings
from urllib.request import urlopen
warnings.filterwarnings("ignore")

stub = modal.Stub("air_quality_daily")
image = modal.Image.debian_slim().pip_install(["hopsworks", "geopy"]) 


def features():
    target_url='https://repo.hops.works/dev/jdowling/target_cities.json'
    response = urlopen(target_url)
    target_cities = json.loads(response.read())
    
    today = datetime.date.today()
    hindcast_day = today - datetime.timedelta(days=1)
    forecast_day = today + datetime.timedelta(days=7)
    
    
    start_of_cell = time.time()
    
    df_aq_raw = pd.DataFrame()
    
    for continent in target_cities:
        for city_name, coords in target_cities[continent].items():
            df_ = get_aqi_data_from_open_meteo(city_name=city_name,
                                               coordinates=coords,
                                               start_date=str(hindcast_day),
                                               end_date=str(today))
            df_aq_raw = pd.concat([df_aq_raw, df_]).reset_index(drop=True)
            
    end_of_cell = time.time()
    print("-" * 64)
    print(f"Parsed new PM2.5 data for ALL locations up to {str(today)}.")
    print(f"Took {round(end_of_cell - start_of_cell, 2)} sec.\n")
    
    
    df_aq_update = df_aq_raw
    
    df_aq_update['date'] = pd.to_datetime(df_aq_update['date'])
    df_aq_update = df_aq_update.dropna()
    
    df_weather_update = pd.DataFrame()
    
    start_of_cell = time.time()
    for continent in target_cities:
        for city_name, coords in target_cities[continent].items():
            df_ = get_weather_data_from_open_meteo(city_name=city_name,
                                                   coordinates=coords,
                                                   start_date=str(today),
                                                   end_date=str(forecast_day),
                                                   forecast=True)
            df_weather_update = pd.concat([df_weather_update, df_]).reset_index(drop=True)
            
    end_of_cell = time.time()
    print("-" * 64)
    print(f"Parsed new weather data for ALL cities up to {str(today)}.")
    print(f"Took {round(end_of_cell - start_of_cell, 2)} sec.\n")
    
    
    df_aq_update.date = pd.to_datetime(df_aq_update.date)
    df_weather_update.date = pd.to_datetime(df_weather_update.date)
    
    df_aq_update["unix_time"] = df_aq_update["date"].apply(convert_date_to_unix)
    df_weather_update["unix_time"] = df_weather_update["date"].apply(convert_date_to_unix)
    
    
    df_aq_update.date = df_aq_update.date.astype(str)
    df_weather_update.date = df_weather_update.date.astype(str)
    
    return df_aq_update, df_weather_update

@stub.function(image=image, schedule=modal.Period(days=1), secret=modal.Secret.from_name("jim-hopsworks-gcp"))
def g():
    df_aq_update, df_weather_update = features()
    
    project = hopsworks.login()
    fs = project.get_feature_store() 
    
    air_quality_fg = fs.get_feature_group(
        name = 'air_quality',
        version = 1
       )
    weather_fg = fs.get_feature_group(
        name = 'weather',
        version = 1
       )
    air_quality_fg.insert(df_aq_update, write_options={"wait_for_job": False})
    weather_fg.insert(df_weather_update, write_options={"wait_for_job": False})





if __name__ == "__main__":
    stub.deploy("air_quality_daily")
    with stub.run():
        g()
       
