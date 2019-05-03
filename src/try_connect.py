import os
import time

import urllib.request
import requests


time.sleep(0.8)
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
    file_name = text[ind_d+10:ind_d+ind_csv+4]
    print(file_name)
    download_path = 'https://www.airqualityengland.co.uk/assets/downloads/' + file_name
    data = urllib.request.urlopen(download_path).read()
    print(data)


'''
'https://www.airqualityengland.co.uk/site/data.php?site_id=MAN1&parameter_id[]=NO&parameter_id[]=NO2&parameter_id[]=NOXasNO2&parameter_id[]=GE10&f_query_id=1066005&data=<?php+print+htmlentities($data);+?>&f_date_started=2019-05-01&f_date_ended=2019-05-02&la_id=219&action=download&submit=Download+Data'
https://stackoverflow.com/questions/35875865/using-python-requests-module-to-submit-a-form-without-input-name

Request URL:https://www.airqualityengland.co.uk/site/data.php?site_id=MAN1&f_date_started=01/05/2019&f_date_ended=02/05/2019&f_query_id=1066005&la_id=219&action=step2&data=&submit=Next
Request method:GET
Remote address:213.251.9.44:443
Status code:
200
Version:HTTP/1.1
Referrer Policy:no-referrer-when-downgrade

Request URL:https://www.airqualityengland.co.uk/site/data.php?site_id=MAN1&parameter_id[]=NO&parameter_id[]=NO2&parameter_id[]=NOXasNO2&parameter_id[]=GE10&f_query_id=1066005&data=<?php+print+htmlentities($data);+?>&f_date_started=2019-05-01&f_date_ended=2019-05-02&la_id=219&action=download&submit=Download+Data
Request method:GET
Remote address:213.251.9.44:443
Status code:
200
Version:HTTP/1.1
Referrer Policy:no-referrer-when-downgrade

'''