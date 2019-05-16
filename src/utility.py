
import numpy as np


class Position:
    """ There appears to be no universal standard for the order of longitude and latitude.
    c.f. http://docs.geotools.org/latest/userguide/library/referencing/order.html
    I will try to use this class to avoid making accidental mistakes """

    def __init__(self, latitude_in_degrees, longitude_in_degrees):
        self.latitude_in_degrees = latitude_in_degrees
        self.longitude_in_degrees = longitude_in_degrees
        self.latitude_in_radians = np.radians(latitude_in_degrees)
        self.longitude_in_radians = np.radians(longitude_in_degrees)

    def update_position(self, latitude_in_degrees, longitude_in_degrees):
        self.latitude_in_degrees = latitude_in_degrees
        self.longitude_in_degrees = longitude_in_degrees
        self.latitude_in_radians = np.radians(latitude_in_degrees)
        self.longitude_in_radians = np.radians(longitude_in_degrees)


