from settings import query_location, import_settings, read_file, config_settings_pg, get_connection_pg, location_type
import pandas as pd
import numpy as np
from io import StringIO
from google.cloud import storage
from pandas_gbq.gbq import to_gbq
from celery import Celery
from celery import task

def make_celery(app):
    celery = Celery(app.import_name, backend=app.config['CELERY_RESULT_BACKEND'],
                    broker=app.config['CELERY_BROKER_URL'])
    celery.conf.update(app.config)
    TaskBase = celery.Task
    class ContextTask(TaskBase):
        abstract = True
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery


def read_data_from_bq(query, settings = None, querylocation = "location", condition = None, table = None, columnname = None, branch = None, schema = None):
    if querylocation == 'location':
        print(querylocation)
        data = pd.read_gbq(query = query, project_id=settings["project_id"], dialect = settings["dialect"])
        return data
    elif (querylocation == None) and (schema != None) :
        if ('apple' in settings['branch'] or 'banana' in settings['branch'] or 'cherry' in settings['branch'] or 'staging' in settings['branch']) and ('integration' in schema or 'feature' in schema):
            query_format = "select * from `{}.{}` where {} = {}" .format("ccdata_" + settings["branch"] + '_'+ schema, table, columnname, condition)
        else:
            query_format = "select * from `{}.{}` where {} = {}" .format(schema, table, columnname, condition)
        print(query_format)
        data = pd.read_gbq(query = query_format, project_id=settings["project_id"], dialect = settings["dialect"])
    return data





def read_data_from_pg(connection, query = None, chunksize = None, schema = None, table = None, condition = None, columnname = None):
    df = pd.DataFrame()
    if chunksize == None:
        if condition == None and query != None:
            df = pd.read_sql(sql = query,con=connection)
        elif query == None and condition == None:
            query = "select * from {}.{}".format(schema, table)
            df = pd.read_sql(sql = query,con=connection)
        elif query == None and condition != None:
            query = "select * from {}.{} where {} = {}".format(schema, table, columnname, condition)
            df = pd.read_sql(sql = query,con=connection)
    elif chunksize != None:
        if condition == None and query != None:
            for chunk in pd.read_sql(sql = query,con=connection, chunksize=chunksize):
                df = df.append(chunk)
        elif query == None and condition == None:
            query = "select * from {}.{}".format(schema, table)
            for chunk in pd.read_sql(sql = query,con=connection, chunksize=chunksize):
                df = df.append(chunk)
        elif query == None and condition != None:
            query = "select * from {}.{} where {} = {}".format(schema, table, columnname, condition)
            for chunk in pd.read_sql(sql = query,con=connection, chunksize=chunksize):
                df = df.append(chunk)
    return df



def data_types_pg(dataframe):
    df = dataframe.copy()
    ul = [str(i) for i in df.dtypes]
    datatypes = list(map(lambda x: " INTEGER" if 'int' in x else " DECIMAL" if 'float' in x else " TEXT" if 'object' in x else " VARCHAR(255)", ul))
    return datatypes

def data_types_bq(dataframe):
    df = dataframe.copy()
    ul = [str(i) for i in df.dtypes]
    datatypes = list(map(lambda x: "INTEGER" if 'int' in x else "FLOAT" if 'float' in x else "STRING" if 'object' in x else "STRING", ul))
    schema_defination = [{"name" : i, "type" : j} for i,j in zip(df.columns, datatypes)]
    return schema_defination




def write_data_to_pg(data, tablename, connection_string = None, if_exists = "append", schema = "credit_card_feature"):
    df = data.copy()
    sio = StringIO()
    sio.write(df.to_csv(index=None, header=None))
    sio.seek(0)
    connection = get_connection_pg(connection_string)
    destination = "{}.{}".format(schema, tablename)
    datatypes = data_types(df)
    print(datatypes)
    columns = ", ".join([(i + " " + j) for i,j in zip(df.columns, datatypes)])
    command = """create table IF NOT EXISTS {}({}); alter table {} owner to gcppoc;""".format(destination,columns, destination, destination)
    print(command)
    cur = connection.cursor()
    cur.execute(command)
    connection.commit()
    with connection.cursor() as c:
        c.copy_from(sio, destination, columns=df.columns, sep=',')
        connection.commit()
    return print("transfer successful")



def write_data_to_bq(data, settings = None, destination_table = None, schema = None):
    df = data.copy()
    if ('apple' in settings['branch'] or 'banana' in settings['branch'] or 'cherry' in settings['branch'] or 'staging' in settings['branch']) and ('integration' in schema or 'feature' in schema):
        destination_table = "{}.{}".format("ccdata_" + settings["branch"] + '_'+ schema, destination_table)
    else:
        destination_table = "{}.{}".format(schema, destination_table)
        print(destination_table)
    data_schema = data_types_bq(df)
    to_gbq(df, destination_table, project_id=settings["project_id"], chunksize = 10000, if_exists = "append", table_schema = data_schema)



def write_data_to_gcs(data, settings, fileid, location = None, analysis = None):
    df = data.copy()
    client = storage.Client(settings["project_id"])
    bucket = client.get_bucket(settings["bucket"])
    blob = bucket.blob(location + '/' + analysis + '/' + str(fileid) + '.csv')
    sio = StringIO()
    sio.write(df.to_csv(index=None, header=True))
    sio.seek(0)
    blob.upload_from_string(sio.read())
    print("data uploaded to {}".format(settings["bucket"] + '/' + location + '/' + analysis + '/' + str(fileid) + '.csv'))



def executing_reading_data(location = query_location(), querylocation = "location", condition = None, table = None, columnname = None, branch = None, schema = None, chunksize = None):
    if '/bq' in location:
        print(location)
        settings_location = location_type('bq')
        settings = import_settings(settings_location)
        query = read_file(location)
        data = read_data_from_bq(query,  settings)
    elif location == 'bq':
        settings_location = location_type('bq')
        settings = import_settings(settings_location)
        data = read_data_from_bq(query = None,  settings = settings, querylocation = querylocation, condition = condition, table = table, columnname = columnname, branch = branch, schema = schema)
    elif '/pg' in location:
        settings_location = location_type('pg')
        settings = import_settings(settings_location)
        connection_string = config_settings_pg(settings)
        connection = get_connection_pg(connection_string)
        query = read_file(location)
        data = read_data_from_pg(connection, query, chunksize = None)
    elif location == 'pg':
        settings_location = location_type('pg')
        settings = import_settings(settings_location)
        connection_string = config_settings_pg(settings)
        connection = get_connection_pg(connection_string)
        data = read_data_from_pg(connection, None, chunksize, schema,table, condition, columnname)
    return data



def executing_writing_data(data, location = 'bq', tablename = None, schema = None, fileid = None, gcs_location=None, analysis = None):
    df = data.copy()
    if location == 'bq':
        print("You have selected big query as destination")
        settings_location = location_type('bq')
        settings = import_settings(settings_location)
        write_data_to_bq(df, settings, tablename, schema)
    elif location == 'pg':
        print("You have selected cloud sql as destination")
        settings_location = location_type('pg')
        settings = import_settings(settings_location)
        connection_string = config_settings_pg(settings)
        write_data_to_pg(df, tablename, connection_string, schema)
    elif location == 'gs':
        print("You have selected cloud storage as destination")
        settings_location = location_type('bq')
        settings = import_settings(settings_location)
        write_data_to_gcs(df, settings, fileid, gcs_location, analysis)




if __name__ == "__main__":
    data = executing_reading_data(location = 'bq', querylocation = None, table = 'entity', columnname = 'entity_id', schema = "integration", condition = 58000)
    executing_writing_data(data, 'pg', 'entity_new','credit_card_feature')
