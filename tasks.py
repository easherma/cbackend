# Luigi is a framework for building data pipelines and managing workflows.:
# It also gives us some visualization tools and a nifty command line interface
# Data pipelines are built through defined 'Task' instances. Each Task can be dependent
# on a previous task, and has a defined output for each task that defines how the task is completed and where the results are written

import luigi
import itertools
import datetime
import csv
import pandas as pd
import requests
import json
import geojson
import ogr
import os
import subprocess
from pandas.io.json import json_normalize
import uuid
import sqlalchemy as sq
import time

def get_null_response(potential_matches):
    matches = pd.DataFrame.from_records(potential_matches, index=['no_result'])
    null_response = matches
    return null_response

def make_schema():
    from sqlalchemy.schema import CreateSchema
    timestamp = str(datetime.datetime.utcnow()).replace (" ", "_")
    username = str(os.getlogin())
    schema_name = username + timestamp
    engine.execute(CreateSchema(schema_name))

def get_null_response(potential_matches):
    matches = pd.DataFrame.from_records(potential_matches, index=['no_result'])
    null_response = matches
    return null_response

class FetchFiles(luigi.Task):
    """
    Lets fetch those client files. This is likely to be replaced by a config file.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    row_limit = 5
    file_limit = 1
    file_target = '../temp/out6_file3_address_10_clean.csv'

    def output(self):
        return luigi.LocalTarget('in/selected-%s.csv' % self.date)

    def run(self):
        def pick_columns():
            sample = pd.read_csv(self.file_target, nrows=5)
            for i, v in enumerate(sample.columns): #print out columns with index
                print i, v
            col_indexes = [1,3,4,5]
            sample_columns = raw_input("Which columns?")
            #sample_columns = sample.columns[[1,3,4,5]]
            chosen = sample_columns.tolist()
            #print "Your chosen columns are: " + chosen
            return chosen

        def load_selected_columns(chosen):
            picked = pd.read_csv(self.file_target, usecols = chosen)
            return picked

        def return_selected_columns():
            print self.output().path
            columns_chosen = pick_columns()
            print columns_chosen
            selected_columns = load_selected_columns(columns_chosen)
            selected_columns.to_csv(self.output().path)

        return_selected_columns()

class CleanFiles(luigi.Task):
    """
    atm this just removes dupes, but could be extended as part of the address cleaning process
    """

    source_file = luigi.Parameter() #input file named on the command line call

    def run(self):
        in_file = self.source_file #pass the class parm into the run function
        df = pd.read_csv(in_file)
        del df['Unnamed: 0'] #remote index column, this shouldn't be needed
        print len(df)
        df2 = df.drop_duplicates()
        print "dropped dupes, new length:", len(df2)
        with self.output().open('w') as fd:
            df2.to_csv(fd, index_label = False)
        # write to file targert

    def output(self):
        return luigi.LocalTarget('./in/deduped.csv')

class prepURL(luigi.Task):
    """
    prepping URLs for geocoder. should take a list of addresses/address fields, source file defined in luigi.cfg
    """
    source_file = luigi.Parameter()
    usecols=luigi.ListParameter()
    #uniqueid= luigi.Parameter()

    def run(self):
        url = 'http://localhost:3100/v1/search?'
        print list(self.usecols)
        source_file = pd.read_csv(self.source_file, dtype= 'str')
        req = requests.Request('GET', url = url)
        paths = []
        for i, row in source_file.iterrows():
            #params = {"text": str(row[self.usecols].values)}
            params = {"text": ','.join(str(n) for n in row[self.usecols])}
            req = requests.Request('GET', url = url, params = params)
            prepped = req.prepare()
            path = str(prepped.url)
            paths.append(path)
            path = []
        #source_file['url_endpoint'] = url
        source_file['path'] =  paths
        #source_file['path'] = source_file['path'].str[0]
        with self.output().open('w') as f:
            source_file.to_csv(f, index_label='pandas_index')
        # urls = self.output().open('w')
        # for row in df2.values:
        #     params= {'text': str(",".join([str(i) for i in row]))}
        #     req = requests.Request('GET', url = url, params = params)
        #     prepped = req.prepare()
        #     print >> urls, prepped.url
        # urls.close()

    def output(self):
        return luigi.LocalTarget('./in/geocoded/prepped_urls.csv')

class pipeToDB(luigi.Task):
    """
    uses pandas annd sqlalchemey, fed with our generated URLS
    """
    db_connect_info= luigi.Parameter()
    def requires(self):
        return prepURL()
    # using these to name output tables, ideally this should be the schema name instead
    table_name = luigi.Parameter()
    original_id = luigi.Parameter()

    def run(self):

        engine = sq.create_engine(self.db_connect_info)
        from sqlalchemy.schema import CreateSchema
        from sqlalchemy import DDL
        from sqlalchemy import event
        from sqlalchemy.sql import text

        timestamp = str(datetime.datetime.utcnow()).replace (" ", "_")
        username = str(os.getlogin())
        schema_name = username
        engine.execute(text("CREATE SCHEMA IF NOT EXISTS %s"% (schema_name)).execution_options(autocommit=True))
        out_named_table = timestamp + '_'+ self.table_name
        failLog = []

        with self.input().open('r') as file:
            in_file = pd.read_csv(file, usecols=['path', 'id'])

            for row in in_file.values:

                #url = in_file['path'][row]
                print "TESTING: ", row[0]
                r = requests.get(row[1])
                uniqueid = uuid.uuid4()
                original_id = row[original_id]
                output = json.loads(r.text)
                #use pandas to parse elements of geojson
                try:
                    #:wqprint url, '   ', uniqueid
                    features = json_normalize(output['features'])
                    features['id'] = uniqueid
                    features['geomjson'] = json_normalize(r.json(), 'features')['geometry']
                    features['bbox'] = json.dumps(output['bbox'])
                    if 'properties.localadmin' not in features:
                        features['properties.localadmin'] = 'none'
                    if 'properties.locality' not in features:
                        features['properties.locality'] = 'none'
                    if 'properties.postalcode' not in features:
                        features['properties.postalcode'] = 'none'
                    if 'properties.borough' not in features:
                        features['properties.borough'] = 'none'
                    if 'properties.match_type' not in features:
                        features['properties.match_type'] = 'none'
                    if 'properties.borough_gid' not in features:
                        features['properties.borough_gid'] = 'none'
                    features.to_sql(name=out_named_table + '_features', con=engine, if_exists='append', dtype={'geomjson': sq.types.JSON, 'properties.confidence': sq.types.FLOAT}, schema=username)
                except Exception as ex:
                    template = "A features exception of type {0} occured. Arguments:\n{1!r}"
                    message = template.format(type(ex).__name__, ex.args)
                    failed = {}
                    failed['url'] = url
                    failed['message'] = message
                    failLog.append(failed)
                    columns = features.columns.values.tolist()
                    for column in columns:
                        features[column] = 'none'
                    features['id'] = uniqueid

                    features['properties.confidence'] = 0
                    if 'properties.localadmin' not in features:
                        features['properties.localadmin'] = 'none'
                    if 'properties.localadmin_gid' not in features:
                        features['properties.localadmin_gid'] = 'none'
                    if 'properties.locality' not in features:
                        features['properties.locality'] = 'none'
                    if 'properties.postalcode' not in features:
                        features['properties.postalcode'] = 'none'
                    if 'properties.borough' not in features:
                        features['properties.borough'] = 'none'
                    if 'properties.match_type' not in features:
                        features['properties.match_type'] = 'none'
                    if 'properties.borough_gid' not in features:
                        features['properties.borough_gid'] = 'none'
                    try:
                        features.to_sql(name=out_named_table + '_features', con=engine, if_exists='append', dtype={'geomjson': sq.types.JSON, 'properties.confidence': sq.types.FLOAT}, schema=username)
                    except (Exception, RuntimeError, TypeError, NameError):
                        pass
                    query = json_normalize(output['geocoding'])
                    query['original_id'] = original_id
                    query['id'] = uniqueid
                    try:
                        query.to_sql(name=out_named_table + '_query', con=engine, if_exists='append', schema=username)
                        pass
                    except (Exception, RuntimeError, TypeError, NameError) as e:
                        print message
                        pass

                    print message
                    pass
                try:
                    query = json_normalize(output['geocoding'])
                    query['id'] = uniqueid
                    query['original_id'] = original_id
                    query['bbox'] = json.dumps(output['bbox'])
                    query.to_sql(name=out_named_table + '_query', con=engine, if_exists='append', schema=username)
                except Exception as ex:
                    template = "A query exception of type {0} occured. Arguments:\n{1!r}"
                    message = template.format(type(ex).__name__, ex.args)
                    pass
                try:
                    merged = features.merge(query, how ='outer', on='id')


                    merged_name= None
                    merged.to_sql(name=out_named_table + '_merged', con=engine, if_exists='append', dtype={'geomjson': sq.types.JSON, 'properties.confidence': sq.types.FLOAT}, schema=username)

                except Exception as ex:
                    template = "A merge  exception of type {0} occured. Arguments:\n{1!r}"
                    message = template.format(type(ex).__name__, ex.args)
                    pass

        with engine.connect() as con:
            try:
                con.execute(sq.text('ALTER TABLE {}."{}" ADD COLUMN geom geometry(Point, 4326);'.format(username, out_named_table + '_features')))
            except Exception as ex:
                template = "An exception of type {0} occured. Arguments:\n{1!r}"
                message = template.format(type(ex).__name__, ex.args)
            try:
                con.execute(sq.text('UPDATE {}."{}" SET geom = ST_SetSRID(ST_GeomFromGeoJSON(geomjson::text), 4326);'.format(username, out_named_table + '_features')))
            except Exception as ex:
                template = "An exception of type {0} occured. Arguments:\n{1!r}"
                message = template.format(type(ex).__name__, ex.args)
                pass
            try:
                con.execute(sq.text('ALTER TABLE {}."{}" ADD COLUMN geom geometry(Point, 4326);'.format(username, out_named_table + '_merged')))
            except Exception as ex:
                template = "An exception of type {0} occured. Arguments:\n{1!r}"
                message = template.format(type(ex).__name__, ex.args)
            try:
                con.execute(sq.text('UPDATE {}."{}" SET geom = ST_SetSRID(ST_GeomFromGeoJSON(geomjson::text), 4326);'.format(username, out_named_table + '_merged')))
            except Exception as ex:
                template = "An exception of type {0} occured. Arguments:\n{1!r}"
                message = template.format(type(ex).__name__, ex.args)
                pass
        with self.output().open('w') as out_file:
            out_file.write(json.dumps(failLog))

    def output(self):
        return luigi.LocalTarget('./in/geocoded/failed.json')

class BulkGeo(luigi.WrapperTask):
    """
    the intention here is to eventually have a 'master' task that runs needed tasks.
    Might be needed more as complexity of individual steps.
    """

    def requires(self):
        yield prepURL(prepURL.f)
        yield pipeToDB()

if __name__ == '__main__':
    luigi.run()

            #def prepare_requests()

            #def send_requests()

            #def write_results()
