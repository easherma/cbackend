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
    atm this just removes dupes
    """

    f = luigi.Parameter() #input file named on the command line call

    def run(self):
        in_f = self.f #pass the class parm into the run function
        df = pd.read_csv(in_f)
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
    """ prepping URLs for geocoder. should take a list of addresses/address fields"""
    f = luigi.Parameter()

    def run(self):
        url = 'http://localhost:3100/v1/search?'
        df2 = pd.read_csv(self.f, dtype= 'str', usecols= [1, 2, 3, 4])
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
        return luigi.LocalTarget('./in/gecoded/urls.json')

class pipeToDB(luigi.Task):
    """ uses pandas annd sqlalchemey, fed with our generated URLS """
    def requires(self):
        return prepURL()

    def run(self):
        engine = sq.create_engine('postgresql://esherman:Deed2World!@localhost:5432/geotemp')
        #data = pd.read_csv(self.input())
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
                    features['geom'] = json_normalize(r.json(), 'features')['geometry']
                    features.to_sql(name='features_new', con=engine, if_exists='append', dtype={'geom': sq.types.JSON})
                    print 

                except:
                    print "FEATURES ERROR!"
                    print output
                    print uniqueid


                try:
                    query = json_normalize(output['geocoding'])
                    query['id'] = uniqueid
                    query['bbox'] = json.dumps(output['bbox'])
                    query.to_sql(name='query_new', con=engine, if_exists='append')                
                except:
                    print "QUERY ERROR!"
                    print output
                    print uniqueid
                #add columns to dataframes, uuids for linking, bbox to the query metadata just in case its useful
                #features['id'] = uniqueid
                #query['id'] = uniqueid
                #print query
                


    def output(self):
        return luigi.LocalTarget('./in/gecoded/complete.json')

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
