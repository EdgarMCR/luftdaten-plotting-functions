
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


def main():
    logging.basicConfig(level=logging.DEBUG)
    data = maybe_get_list_of_sensors()
    print(type(data))
    print(data[0])


if __name__ == '__main__':
    start_time = time.time()
    main()
    print('------------ %.3f seconds ------------' % (time.time() - start_time))
