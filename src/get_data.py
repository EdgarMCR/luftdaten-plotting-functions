
import os
import time
import datetime
import json
import logging
import gzip
import glob

import urllib.request


def maybe_get_list_of_sensors():
    midnight = datetime.datetime.combine(datetime.datetime.today(), datetime.datetime.min.time())
    save_name = os.path.join('../data/data-%s.json.gz' % midnight.strftime('%Y%m%d'))

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
        listing = glob.glob('../data/data-*.json.gz')
        for file in listing:
            date = datetime.datetime.strptime(file, '../data/data-%Y%m%d.json.gz')
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

def get_manchester_council_data():
    """ Function that downloads the data from Manchester measuring stations """
    'https://www.airqualityengland.co.uk/site/data.php?site_id=TRAF&parameter_id%5B%5D=NO&parameter_id%5B%5D=NO2&parameter_id%5B%5D=NOXasNO2&parameter_id%5B%5D=GE10&parameter_id%5B%5D=SO2&f_query_id=1061759&data=%3C%3Fphp+print+htmlentities%28%24data%29%3B+%3F%3E&f_date_started=2019-01-01&f_date_ended=2019-05-01&la_id=368&action=download&submit=Download+Data'

    # csv download
    # Generate file specifically for request - thats a bit disappointing
    'https://www.airqualityengland.co.uk/assets/downloads/2019-01-01-190501090739.csv'

    # better use this service
    'https://uk-air.defra.gov.uk/data/flat_files?site_id=MAN3' # piccadily


def main():
    logging.basicConfig(level=logging.DEBUG)
    data = maybe_get_list_of_sensors()
    print(type(data))
    print(data[0])


if __name__ == '__main__':
    start_time = time.time()
    main()
    print('------------ %.3f seconds ------------' % (time.time() - start_time))
