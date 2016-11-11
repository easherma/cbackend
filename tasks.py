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

# def fixgeom(self):
#     engine = sq.create_engine(self.db_connect_info)
#     with engine.connect() as con:
#         tables = con.execute(sq.text("SELECT tablename FROM pg_tables WHERE schemaname = 'dmcquown'AND tablename LIKE '%merged%'"))
#         for table in tables:
#             #import pdb; pdb.set_trace()
#             #@TODO steps are here, need to clean up for current tables and set up the flow to work from scratch. some uneeded steps here if done from the get go, check name of geom column created, add the column then update it, schema name should be a parameter
#             try:
#                 con.execute(sq.text('ALTER TABLE dmcquown."{}" RENAME geom TO geomjson;'.format(bytes(table.values()[0]))))
#             except Exception as ex:
#                 template = "An exception of type {0} occured. Arguments:\n{1!r}"
#                 message = template.format(type(ex).__name__, ex.args)
#                 pass
#             try:
#                 con.execute(sq.text('ALTER TABLE {}."{}" ADD COLUMN geom geometry(Point, 4326);'.format(bytes(table.values()[0]))))
#             except Exception as ex:
#                 template = "An exception of type {0} occured. Arguments:\n{1!r}"
#                 message = template.format(type(ex).__name__, ex.args)
#                 pass
#             try:
#                 con.execute(sq.text('UPDATE dmcquown."{}" SET geom = ST_SetSRID(ST_GeomFromGeoJSON(geomjson::text), 4326);'.format(bytes(table.values()[0]))))
#             except Exception as ex:
#                 template = "An exception of type {0} occured. Arguments:\n{1!r}"
#                 message = template.format(type(ex).__name__, ex.args)
#                 pass

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
   
    usecols= luigi.ListParameter()

    
    def run(self):
        url = 'http://localhost:3100/v1/search?'
	print list(self.usecols)	
        df2 = pd.read_csv(self.source_file, dtype= 'str', usecols=self.usecols)
        req = requests.Request('GET', url = url)
        urls = self.output().open('w')
        for row in df2.values:
            params= {'text': str(",".join([str(i) for i in row]))}
            req = requests.Request('GET', url = url, params = params)
            prepped = req.prepare()
            print >> urls, prepped.url
        urls.close()
        #with self.output().open('wb') as fd:
        #    fd.write(urls)

    def output(self):
        return luigi.LocalTarget('./in/geocoded/urls.json')

class pipeToDB(luigi.Task):
    """
    uses pandas annd sqlalchemey, fed with our generated URLS
    """
    db_connect_info= luigi.Parameter()
    def requires(self):
        return prepURL()
    # using these to name output tables, ideally this should be the schema name instead
    table_name = luigi.Parameter()

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

        with self.input().open('r') as in_file:
            for url in in_file:
                #print url
		
                r = requests.get(url)
                uniqueid = uuid.uuid4()
                output = json.loads(r.text)
                #use pandas to parse elements of geojson
                try:
                    print url, '   ', uniqueid
                    features = json_normalize(output['features'])
                    features['id'] = uniqueid
                    features['geomjson'] = json_normalize(r.json(), 'features')['geometry']
                    features.to_sql(name=out_named_table + '_features', con=engine, if_exists='append', dtype={'geomjson': sq.types.JSON}, schema=username)
                except Exception as ex:
                    template = "A features exception of type {0} occured. Arguments:\n{1!r}"
                    message = template.format(type(ex).__name__, ex.args)
                    failed = {}
		    failed['url'] = url
                    failed['message'] = message
		    failLog.append(failed)
		    print message
		    
                #except:
                #    print "FEATURES ERROR!"
                #    print output
                #    print uniqueid
                #    features.to_sql(name='features_errors', con=engine, if_exists='replace', dtype={'geom': sq.types.JSON})
                try:
                    query = json_normalize(output['geocoding'])
                    query['id'] = uniqueid
                    query['bbox'] = json.dumps(output['bbox'])
                    query.to_sql(name=out_named_table + '_query', con=engine, if_exists='append', schema=username)
                except Exception as ex:
                    template = "A query exception of type {0} occured. Arguments:\n{1!r}"
                    message = template.format(type(ex).__name__, ex.args)
                #except:
                #    print "QUERY ERROR!"
                #    print output
                #    print uniqueid
                #    query.to_sql(name='query_errors', con=engine, if_exists='replace', dtype={'geom': sq.types.JSON})
                try:
                    merged = features.merge(query, on='id')
                    merged_name= None
                    merged.to_sql(name=out_named_table + '_merged', con=engine, if_exists='append', dtype={'geomjson': sq.types.JSON}, schema=username)

                except Exception as ex:
                    template = "A merge  exception of type {0} occured. Arguments:\n{1!r}"
                    message = template.format(type(ex).__name__, ex.args)
                #except:
                #    print "MERGE ERROR"
                #add columns to dataframes, uuids for linking, bbox to the query metadata just in case its useful
                #features['id'] = uniqueid
                #query['id'] = uniqueid
                #print query

        #add geom

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
        with self.output().open('w') as out_file:
            out_file.write(json.dumps(failLog))         

    def output(self):
        return luigi.LocalTarget('./in/gecoded/failed.json')

# class addGeom(luigi.Task):
#     db_connect_info= luigi.Parameter()
#     schemaname
#     tablename
#
#     from sqlalchemy import text
#     #username view
#     #table(s)
#     #stgeom
#     #threshold/where clauses
#     def requires(self):
#         return  pipeToDB()
#     def run(self):
#         engine = sq.create_engine(self.db_connect_info)
#         with engine.connect() as con:
#             try:
#                 con.execute(sq.text('ALTER TABLE {}."{}" ADD COLUMN geom geometry(Point, 4326);'.format(schemaname, tablename))))
#             except Exception as ex:
#                 template = "An exception of type {0} occured. Arguments:\n{1!r}"
#                 message = template.format(type(ex).__name__, ex.args)
#             try:
#                 con.execute(sq.text('UPDATE {}."{}" SET geom = ST_SetSRID(ST_GeomFromGeoJSON(geomjson::text), 4326);'.format(schemaname, tablename))))
#             except Exception as ex:
#                 template = "An exception of type {0} occured. Arguments:\n{1!r}"
#                 message = template.format(type(ex).__name__, ex.args)
#                 pass
#     def output(self):
#         return luigi.LocalTarget('./in/gecoded/complete.json')

# class createView(luigi.Task):
#     db_connect_info= luigi.Parameter()
#     from sqlalchemy import text
#     #threshold
#     #schema name
#     #table name
#     #limit
#     #def requires(self):
#     #    return  prepToDB()
#     def run(self):
#         engine = sq.create_engine(self.db_connect_info)
#         with engine.connect() as con:
#             tables = con.execute(sq.text("SELECT *  FROM schema+tablename WHERE "properties.confidence" > threshold #try other params too, not just the confidence score ORDER BY "properties.confidence"))
#
#
#                 #geoms = con.execute(sq.text('SELECT * , ST_GeomFromGeoJSON(geomjson::text) AS geom FROM dmcquown."{}"'.format(bytes(table.values()[0]))))
#     def output(self):
#         return luigi.LocalTarget('./in/gecoded/complete.json')

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
