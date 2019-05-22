import os
import time
import datetime
import logging

import matplotlib.pylab as plt
import numpy as np
import pandas as pd


import src.constants as const
import src.get_data as gd
import src.utility as util


chorlton_cum_hardy = util.Position(53.442, -2.277)


def get_sensors_around_chorlton():
    all_sensors_df = gd.get_sensor_in_radius(latitude=chorlton_cum_hardy.latitude_in_degrees,
                                 longitude=chorlton_cum_hardy.longitude_in_degrees,
                                 distance=8e3)

    print(all_sensors_df[[const.sensor_id, const.sensor_type_id, const.sensor_name]])
    print(all_sensors_df.columns)

    dic_of_dfs = {}
    pmdf = all_sensors_df[all_sensors_df[const.sensor_name] == 'SDS011']
    for id in pmdf[const.sensor_id]:
        df = gd.get_sensor_data_for_date_range(sensor_name='sds011', sensor_id=id,
                                               start_date=datetime.datetime.now() - datetime.timedelta(3),
                                               end_date=datetime.datetime.now() - datetime.timedelta(1))
        if df is not None:
            dic_of_dfs[id] = df

    for variable in [const.p1, const.p2]:
        plt.figure()
        for k, df in dic_of_dfs.items():
            try:
                plt.plot(df[const.timestamp], df[variable], label=k)
            except (KeyError, TypeError):
                logging.info("Site %s does not have measurements for %s" % (k, variable))
        plt.ylabel(variable)
        plt.xlabel('Time')
        plt.legend()
        plt.tight_layout()
    plt.show()


def plot_official_data():
    ed = datetime.datetime.now() - datetime.timedelta(1)
    sd = ed - datetime.timedelta(30)
    rslt_dic = gd.get_manchester_airqualityengland_data(start_date=sd, end_date=ed)

    for variable in [const.pm10, const.NO, const.NO2]:
        print("Doing %s" % variable)
        plt.figure()
        for k, df in rslt_dic.items():
            try:
                plt.plot(df[const.start_date], df[variable], label=k)
            except (KeyError, TypeError):
                logging.info("Site %s does not have measurements for %s" % (k, variable))
        plt.ylabel(variable)
        plt.xlabel('Time')
        plt.legend()
        plt.tight_layout()
    plt.show()


def main():
    plot_official_data()
    # get_sensors_around_chorlton()


if __name__ == "__main__":
    ss = datetime.datetime.now().strftime('%H:%M:%S'); start_time = time.time()
    main()
    es = datetime.datetime.now().strftime('%H:%M:%S'); l = ''.join(['-']*37); elapsed = time.time() - start_time
    print("\n%s\n-------- %s - %s --------\n--------- % 9.3f seconds ---------\n%s" % (l, ss, es, elapsed, l))