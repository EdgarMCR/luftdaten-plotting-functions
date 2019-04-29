
import os
import time
import datetime
import json
import logging
import gzip
import glob

import urllib.request


def maybe_get_list_of_sensors():
    today = datetime.datetime.today()
    save_name = os.path.join('../data/data-%s.json.gz' % today.strftime('%Y%m%d'))

    if os.path.exists(save_name):
        print("%s exists" % save_name)
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
            if date < today:
                try:
                    os.remove(file)
                except Exception as err:
                    logging.warn("Failed to delete old file %s with error %s" % (file, str(err)))

    return json.loads(data)


def main():
    maybe_get_list_of_sensors()


if __name__ == '__main__':
    start_time = time.time()
    main()
    print('------------ %.3f seconds ------------' % (time.time() - start_time))
