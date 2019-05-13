
import os
import io
import time
import datetime
import json
import logging
import gzip
import glob

import urllib.request
import requests
import bs4

import pandas as pd

import src.constants as const

data_folder = '..' + os.sep + 'data' + os.sep
wait_time = 2  # seconds
wait_time

def maybe_get_list_of_sensors():
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
    return data


def convert_json_data_to_df(data):
    """ Convert the JSON to a dataframe. For now, do this in an inefficient way but easy to code. """
    dict_of_columns = {}
    for d in data:
        pass


def get_manchester_airqualityengland_data(start_date, end_date):
    sites=[('MAN1', 'parameter_id[]=NO&parameter_id[]=NO2&parameter_id[]=NOXasNO2&parameter_id[]=GE10'),
           ('MAN3', 'parameter_id%5B%5D=NO2&parameter_id%5B%5D=NOXasNO2&parameter_id%5B%5D=O3&parameter_id%5B%5D=GE10&parameter_id%5B%5D=PM25&parameter_id%5B%5D=SO2&parameter_id%5B%5D=M_T&parameter_id%5B%5D=M_DIR&parameter_id%5B%5D=M_SPED'),
           ('MAHG', 'parameter_id%5B%5D=SO2'),
           ('TRF2', 'parameter_id%5B%5D=NO&parameter_id%5B%5D=NO2&parameter_id%5B%5D=NOXasNO2&parameter_id%5B%5D=GE10'),
           ('TRAF', 'parameter_id%5B%5D=NO&parameter_id%5B%5D=NO2&parameter_id%5B%5D=NOXasNO2&parameter_id%5B%5D=GE10'),
           ('STK7', 'parameter_id%5B%5D=NO&parameter_id%5B%5D=NO2&parameter_id%5B%5D=NOXasNO2&parameter_id%5B%5D=GE10')]

    results_dic = {}
    for site in sites:
        time.sleep(wait_time) # to prevent hammering the website too much
        print(f"doing site {site[0]}")
        results_dic[site[0]] = get_data_from_air_quality_england(site_id=site[0], parameters=site[1],
                                                                 start_date=start_date, end_date=end_date)
    return results_dic


def get_data_from_air_quality_england(site_id: str, parameters: str, start_date: datetime.datetime, end_date: datetime.datetime):
    """
    https://www.airqualityengland.co.uk/site/data.php?site_id=MAN3&f_date_started=01/05/2019&f_date_ended=09/05/2019&f_query_id=1056749&la_id=219&action=step2&data=&submit=Next
    https://www.airqualityengland.co.uk/site/data.php?site_id=MAN3&parameter_id[]=CO&parameter_id[]=NO&parameter_id[]=NO2&parameter_id[]=NOXasNO2&parameter_id[]=O3&parameter_id[]=GE10&parameter_id[]=PM25&parameter_id[]=SO2&parameter_id[]=M_T&parameter_id[]=M_DIR&parameter_id[]=M_SPED&f_query_id=1056749&data=<?php+print+htmlentities($data);+?>&f_date_started=2019-05-01&f_date_ended=2019-05-09&la_id=219&action=download&submit=Download+Data
    :param site_id:
    :param parameters:
    :param start_date:
    :param end_date:
    :return:
    """
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

        # https://www.airqualityengland.co.uk/site/data.php?site_id=MAN3&parameter_id%5B%5D=CO&parameter_id%5B%5D=NO&parameter_id%5B%5D=NO2&parameter_id%5B%5D=NOXasNO2&parameter_id%5B%5D=O3&parameter_id%5B%5D=GE10&parameter_id%5B%5D=PM25&parameter_id%5B%5D=SO2&parameter_id%5B%5D=M_T&parameter_id%5B%5D=M_DIR&parameter_id%5B%5D=M_SPED&f_query_id=1056379&data=%3C%3Fphp+print+htmlentities%28%24data%29%3B+%3F%3E&f_date_started=2019-05-01&f_date_ended=2019-05-09&la_id=219&action=download&submit=Download+Data
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

    urls = ['https://uk-air.defra.gov.uk/data_files/site_data/MAN3_2019.csv',
             'https://uk-air.defra.gov.uk/data_files/site_data/MAHG_2019.csv',
             'https://uk-air.defra.gov.uk/data_files/site_data/ECCL_2019.csv']

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
    # print(type(data))
    # print(data[0])
    # get_manchester_council_data()
    ed = datetime.datetime.now()
    sd = ed - datetime.timedelta(2)
    rslt_dic = get_manchester_airqualityengland_data(start_date=sd, end_date=ed)
    print(rslt_dic)


if __name__ == '__main__':
    start_time = time.time()
    main()
    print('------------ %.3f seconds ------------' % (time.time() - start_time))
