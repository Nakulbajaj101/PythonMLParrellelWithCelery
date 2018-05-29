


import pandas as pd
import numpy as np
import os
import json
import psycopg2
from urllib.parse import urlparse
from io import StringIO




def import_settings(filename):
    with open(filename) as f:
        conf = json.load(f)
    return conf


def location_type(settings_type = 'bq'):
    directory = os.getcwd()
    location = directory.replace("\\","/") + "/ConnectionAndQueries/Connection/"
    if "bq" in settings_type:
        settings = "bqconnection.json"
    else:
        settings = "connection_settings.json"
    location = location + settings
    return location


def read_file(filename):
    file = open(filename, 'r')
    query = sql = "".join(file.readline())
    return query



def config_settings_pg(settings):
    host = settings["host"]
    database = settings["database"]
    user = settings["user"]
    passw = settings["passw"]
    port = settings["port"]
    conn_str = "host={} dbname={} user={} password={} port={}".format(host, database, user, passw, port)
    return conn_str



def get_connection_pg(connection_string):
    connection = psycopg2.connect(connection_string)
    return connection


def query_location():
    directory = os.getcwd()
    location = directory.replace("\\","/") + "/ConnectionAndQueries/Queries/"
    for file in os.listdir(location):
        if file.endswith(".sql"):
            filename = os.path.join(location, file)
    return filename

if __name__ == 'main':
    location = location_type('pg')
    settings = import_settings(location)
    connection_string = config_settings_pg(settings)
    connection = get_connection_pg(connection_string)
