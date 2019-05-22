
import os
import io
import time
import datetime
import json
import logging
import gzip
import glob
import typing
import math

import urllib.error
import urllib.request
import requests
import bs4

import numpy as np
import pandas as pd

import src.constants as const

data_folder = '..' + os.sep + 'data' + os.sep
wait_time = 2  # seconds


def get_sensor_in_radius(latitude: float, longitude: float, distance: float) -> pd.DataFrame:
    """ Returns all current sensors that are within `distance` [meters] of longitude and latitude

    Could have used http://api.luftdaten.info/v1/filter/{query} instead but this is easier to cache
    """

    # helper function
    def get_distance(lat1, lon1):
        return np.abs(distance_between_two_points(lat1, lon1, lat2=latitude, lon2=longitude))

    df = maybe_get_list_of_sensors()
    df[const.distance] = get_distance(df[const.latitude], df[const.longitude])
    return df[df[const.distance] < distance]


def distance_between_two_points(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """ Calculate distance between to two points (https://en.wikipedia.org/wiki/Haversine_formula) """
    # approximate radius of earth in m
    r = 6373000.

    lat1 = np.radians(lat1)
    lon1 = np.radians(lon1)
    lat2 = np.radians(lat2)
    lon2 = np.radians(lon2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

    return r * c


def maybe_get_list_of_sensors() -> pd.DataFrame:
    midnight = datetime.datetime.combine(datetime.datetime.today(), datetime.datetime.min.time())
    save_name = os.path.join(data_folder, 'data-%s.json.gz' % midnight.strftime('%Y%m%d'))

    if os.path.exists(save_name):
        logging.debug("%s exists, loading from file" % save_name)
        with gzip.open(save_name, 'r') as f:
            data = f.read()
    else:
        url = 'http://api.luftdaten.info/static/v2/data.json'
        data = urllib.request.urlopen(url).read()

        with gzip.open(save_name, 'w') as f:
            f.write(data)

        # Delete old files
        listing = glob.glob(os.path.join(data_folder, 'data-*.json.gz'))
        for file in listing:
            date = datetime.datetime.strptime(file, data_folder + 'data-%Y%m%d.json.gz')
            if date < midnight:
                try:
                    logging.debug("Removing old data.json file %s" % file)
                    os.remove(file)
                except Exception as err:
                    logging.warn("Failed to delete old file %s with error %s" % (file, str(err)))

    data = json.loads(data)
    return convert_json_data_to_df(data)


def convert_json_data_to_df(data):
    """ Convert the JSON to a dataframe. For now, do this in an inefficient way but easy to code. """
    list_of_dicts = []
    for d in data:
        nd = {const.id: int(d[const.id]),
              const.timestamp: d[const.timestamp],
              const.longitude: float(d[const.location][const.longitude]),
              const.latitude: float(d[const.location][const.latitude]),
              const.indoor: bool(d[const.location][const.indoor]),
              const.sensor_id: int(d[const.sensor][const.id]),
              const.sensor_type_id: int(d[const.sensor][const.sensor_type][const.id]),
              const.sensor_name: d[const.sensor][const.sensor_type][const.name]}
        list_of_dicts.append(nd)
    df = pd.DataFrame(list_of_dicts)
    return df


def daterange(start_date: datetime.datetime, end_date: datetime.datetime) -> typing.Iterable[datetime.datetime]:
    """https://stackoverflow.com/questions/1060279/iterating-through-a-range-of-dates-in-python"""
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


def get_station_data_for_date_range(station_name: str, station_id: int, start_date: datetime.datetime,
                                    end_date: datetime.datetime) -> pd.DataFrame:

    df = None
    for date in daterange(start_date, end_date):
        logging.debug("Fetching %s" % str(date))
        try:
            tdf = maybe_get_station_data(station_name=station_name, station_id=station_id, date=date)
            if df is None:
                df = tdf
            else:
                df = pd.merge(left=df, right=tdf, on=list(df.columns), how='outer')
        except urllib.error.HTTPError:
            pass

    return df


def maybe_get_station_data(station_name: str, station_id: int, date: datetime.datetime) -> pd.DataFrame:
    """ raise HTTPError (urllib.error.HTTPError) if data does not exist"""
    date_str = date.strftime('%Y-%m-%d')
    filename = "data-%s-%d-%s.csv" % (station_name, station_id, date_str)
    save_name = os.path.join(data_folder, filename + '.gz')

    if os.path.exists(save_name):
        logging.debug("%s exists, loading from file" % save_name)
        with gzip.open(save_name, 'r') as f:
            data = f.read()
    else:
        logging.debug("Trying to download...")
        time.sleep(wait_time)
        url = 'https://www.madavi.de/sensor/data_csv/csv-files/%s/%s' % (date_str, filename)
        data = urllib.request.urlopen(url).read()

        with gzip.open(save_name, 'w') as f:
            f.write(data)

    return pd.read_csv(io.BytesIO(data), sep=';')


def get_sensor_data_for_date_range(sensor_name: str, sensor_id: int, start_date: datetime.datetime,
                                    end_date: datetime.datetime) -> pd.DataFrame:

    df = None
    for date in daterange(start_date, end_date):
        logging.debug("Fetching %s" % str(date))
        try:
            tdf = maybe_get_sensor_data(sensor_name=sensor_name, sensor_id=sensor_id, date=date)
            if df is None:
                df = tdf
            else:
                df = pd.merge(left=df, right=tdf, on=list(df.columns), how='outer')
        except urllib.error.HTTPError:
            pass

    return df


def maybe_get_sensor_data(sensor_name: str, sensor_id: int, date: datetime.datetime) -> pd.DataFrame:
    """ raise HTTPError (urllib.error.HTTPError) if data does not exist"""
    date_str = date.strftime('%Y-%m-%d')
    filename = "%s_%s_sensor_%d.csv" % (date_str, sensor_name, sensor_id)
    save_name = os.path.join(data_folder, filename + '.gz')

    if os.path.exists(save_name):
        logging.debug("%s exists, loading from file" % save_name)
        with gzip.open(save_name, 'r') as f:
            data = f.read()
    else:
        logging.debug("Trying to download...")
        time.sleep(wait_time)
        url = 'http://archive.luftdaten.info/%s/%s' % (date_str, filename)
        data = urllib.request.urlopen(url).read()

        with gzip.open(save_name, 'w') as f:
            f.write(data)

    df = pd.read_csv(io.BytesIO(data), sep=';')
    df[const.timestamp] = pd.to_datetime(df[const.timestamp])
    return df


def get_manchester_airqualityengland_data(start_date, end_date):
    sites=[('MAN1', 'parameter_id[]=NO&parameter_id[]=NO2&parameter_id[]=NOXasNO2&parameter_id[]=GE10'),
           ('MAN3', 'parameter_id%5B%5D=NO2&parameter_id%5B%5D=NOXasNO2&parameter_id%5B%5D=O3&parameter_id%5B%5D=GE10&parameter_id%5B%5D=PM25&parameter_id%5B%5D=SO2&parameter_id%5B%5D=M_T&parameter_id%5B%5D=M_DIR&parameter_id%5B%5D=M_SPED'),
           ('MAHG', 'parameter_id%5B%5D=SO2'),
           ('TRF2', 'parameter_id%5B%5D=NO&parameter_id%5B%5D=NO2&parameter_id%5B%5D=NOXasNO2&parameter_id%5B%5D=GE10'),
           ('TRAF', 'parameter_id%5B%5D=NO&parameter_id%5B%5D=NO2&parameter_id%5B%5D=NOXasNO2&parameter_id%5B%5D=GE10'),
           ('STK7', 'parameter_id%5B%5D=NO&parameter_id%5B%5D=NO2&parameter_id%5B%5D=NOXasNO2&parameter_id%5B%5D=GE10')]

    results_dic = {}
    for site in sites:
        print(f"doing site {site[0]}")
        results_dic[site[0]] = get_data_from_air_quality_england(site_id=site[0], parameters=site[1],
                                                                 start_date=start_date, end_date=end_date)
    return results_dic


def get_data_from_air_quality_england(site_id: str, parameters: str, start_date: datetime.datetime, end_date: datetime.datetime):
    """ Fetch data from www.airqualityengland.co.uk """

    base_url = 'https://www.airqualityengland.co.uk/site/data.php?site_id=%s' % site_id
    date_started = start_date.strftime('%Y-%m-%d')
    date_ended = end_date.strftime('%Y-%m-%d')

    save_name = os.path.join(data_folder, 'data-aqe-%s-%s-%s.csv.gz' % (site_id, date_started.replace('-', ''),
                                                                        date_ended.replace('-', '')))
    if os.path.exists(save_name):
        logging.debug("%s exists, loading from file" % save_name)
        with gzip.open(save_name, 'r') as f:
            data = f.read()
    else:
        print(f"trying to fetch site {site_id}")
        time.sleep(wait_time) # to prevent hammering the website too much

        request_url = f'https://www.airqualityengland.co.uk/site/data.php?site_id={site_id}&{parameters}&f_query_id=1066005&data=<?php+print+htmlentities($data);+?>&f_date_started={date_started}&f_date_ended={date_ended}&la_id=219&action=download&submit=Download+Data'
        print(f"request_url = {request_url}")
        with requests.Session() as s:
            time.sleep(wait_time)
            response = s.get(base_url)
            soup = bs4.BeautifulSoup(response.text, 'html.parser')

            f_query_id = soup.find(id='f_query_id').get('value')
            la_id = soup.find(id='la_id').get('value')

            details = f'&{parameters}&f_query_id={f_query_id}&data=<?php+print+htmlentities($data);+?>&f_date_started={date_started}&f_date_ended={date_ended}&la_id={la_id}'
            request_url = base_url + details + '&action=download&submit=Download+Data'
            time.sleep(wait_time)
            response = s.get(request_url)
            text = response.text

            #TODO: do this with bs4
            str_download = 'download='
            str_csv = '.csv'
            ind_d = text.find(str_download)
            ind_csv = text[ind_d:].find(str_csv)

            file_name = text[ind_d + 10:ind_d + ind_csv + 4]
            download_path = 'https://www.airqualityengland.co.uk/assets/downloads/' + file_name
            time.sleep(wait_time)  # To prevent hammering the server
            data = urllib.request.urlopen(download_path).read()

            with gzip.open(save_name, 'w') as f:
                f.write(data)

    return _convert_air_quality_england_to_dataframe(data)


def _convert_air_quality_england_to_dataframe(data):
    df = pd.read_csv(io.BytesIO(data), skiprows=5, skipfooter=1, engine='python')

    for ii, d_str in enumerate(df['End Date']):
        # TODO: use extra data we are getting!
        # TODO: very inefficient test
        try:
            datetime.datetime.strptime(d_str, '%d/%m/%Y')
        except:
            logging.warn("`%s` not date - probably some additional data in datafram that we are ignoring" % d_str)
            df = df.iloc[:ii]
            break

    df = fix_df(df)

    return df


def fix_df(df: pd.DataFrame) -> pd.DataFrame:
    # End hour can be 24 which is rejected by python datetime
    # Therefore we need to hack the time
    df[const.start_date] = pd.to_datetime(df['End Date'] + df['End Time'].apply(_hack_end_time_to_start_time),
                                          format="%d/%m/%Y %H:%M:%S")

    rename_dict = {'PM10': const.pm10, 'NOXasNO2': const.NOXasNO}
    df = df.rename(columns=rename_dict)
    df = df.drop(['End Time', 'End Date', 'Status/units', 'Status/units.1', 'Status/units.2', 'Status/units.3',
                  'Unnamed: 10'], axis=1, errors='ignore')
    return df


def _hack_end_time_to_start_time(time: str) -> str:
    """ Take strings of the form '01:00:00' and substract unity from the houre"""
    hour = int(time[:2])
    return " %02d" % (hour - 1) + time[2:]


def get_manchester_council_data():
    """ Function that downloads the data from Manchester measuring stations """
    midnight = datetime.datetime.combine(datetime.datetime.today(), datetime.datetime.min.time())

    base_url = 'https://uk-air.defra.gov.uk/data_files/site_data/'
    urls = [base_url + 'MAN3_2019.csv', base_url + 'MAHG_2019.csv', base_url + 'ECCL_2019.csv']

    for url in urls:
        ind = url.rfind('/')
        save_name = os.path.join(data_folder, 'downloaded-%s_' % midnight.strftime('%Y%m%d') + url[ind+1:] + '.gz')

        if os.path.exists(save_name):
            logging.debug("%s exists, loading from file" % save_name)
        else:
            data = urllib.request.urlopen(url).read()

            with gzip.open(save_name, 'w') as f:
                f.write(data)


def main():
    logging.basicConfig(level=logging.DEBUG)
    # data = maybe_get_list_of_sensors()
    # print(data.columns)
    # print(data.dtypes)

    # df = get_sensor_in_radius(longitude=-2.245278, latitude=53.479444, distance=6e3)
    # print(df[[const.sensor_id, const.distance]])
    # print(distance_between_two_points(52.2296756, 21.0122287, 52.406374,16.9251681))

    # get_manchester_council_data()

    # ed = datetime.datetime.now() - datetime.timedelta(1)
    # sd = ed - datetime.timedelta(2)
    # rslt_dic = get_manchester_airqualityengland_data(start_date=sd, end_date=ed)
    # print(rslt_dic)

    # df = maybe_get_station_data(station_name='esp8266', station_id=1027161, date=datetime.datetime.now() - datetime.timedelta(1))

    # df = get_station_data_for_date_range(station_name='esp8266', station_id=1027161,
    #                                      start_date=datetime.datetime.now() - datetime.timedelta(3),
    #                                      end_date=datetime.datetime.now() - datetime.timedelta(1))
    # df = maybe_get_sensor_data(sensor_name='dht22', sensor_id=13312, date=datetime.datetime.now() - datetime.timedelta(3))
    df = get_sensor_data_for_date_range(sensor_name='dht22', sensor_id=13312,
                                        start_date=datetime.datetime.now() - datetime.timedelta(3),
                                        end_date=datetime.datetime.now() - datetime.timedelta(1))
    print(df.head())
    print(df.columns)


if __name__ == '__main__':
    start_time = time.time()
    main()
    print('------------ %.3f seconds ------------' % (time.time() - start_time))
