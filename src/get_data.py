
import os
import time
import datetime
import json
import logging
import gzip
import glob

import urllib.request
import requests

data_folder = '../data/'
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


def get_data_from_air_quality_england(sensor_names_and_parameters, start_date, end_date):
    with requests.Session() as s:
        site_id = 'MAN1'
        parameters = 'parameter_id[]=NO&parameter_id[]=NO2&parameter_id[]=NOXasNO2&parameter_id[]=GE10'
        date_started = '2019-04-30'
        date_ended = '2019-05-02'
        request_url = f'https://www.airqualityengland.co.uk/site/data.php?site_id={site_id}&{parameters}&f_query_id=1066005&data=<?php+print+htmlentities($data);+?>&f_date_started={date_started}&f_date_ended={date_ended}&la_id=219&action=download&submit=Download+Data'
        response = s.get(request_url)
        with open('tmp-save.txt', 'w') as f:
            f.write(response.text)
        text = response.text
        # # with open('tmp-save.txt', 'r') as f:
        #     text = f.read()
        print(text)
        str_download = 'download='
        str_csv = '.csv'
        search_text = '.csv'
        ind_d = text.find(str_download)
        ind_csv = text[ind_d:].find(str_csv)

        print(f"ind_d = {ind_d}")
        print(f"ind_csv = {ind_csv}")
        file_name = text[ind_d + 10:ind_d + ind_csv + 4]
        print(file_name)
        download_path = 'https://www.airqualityengland.co.uk/assets/downloads/' + file_name
        data = urllib.request.urlopen(download_path).read()
        print(data)


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
    get_manchester_council_data()


if __name__ == '__main__':
    start_time = time.time()
    main()
    print('------------ %.3f seconds ------------' % (time.time() - start_time))
