# Luigi is a framework for building data pipelines and managing workflows.
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

class FetchFiles(luigi.Task):
    """
    Lets fetch those client files
    """
    date = luigi.DateParameter(default=datetime.date.today())
    row_limit = 5
    file_limit = 1
    directory_target = 'path/to/folder'
    file_target = '../temp/out6_file3_address_10_clean.csv'

    def output(self):
        return luigi.LocalTarget('in/selected-%s.csv' % self.date)

    def run(self):
#    for i in itertools.islice(csv.DictReader(open('../temp/out6_file3_address_10_clean.csv')),5 ):
#	print i
     #       with self.output().open('w+') as f:
     #           f.write(i)
        def pick_columns():
            sample = pd.read_csv(self.file_target, nrows=5)
            for i, v in enumerate(sample.columns): #print out columns with index
                print i, v
            col_indexes = [1,3,4,5]
            sample_columns = sample.columns[[1,3,4,5]]
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
           # with self.output().open('w') as outfile:
            selected_columns.to_csv(self.output().path)




        return_selected_columns()



class CleanFiles(luigi.Task):

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


#class NormalizeAddys(luigi.Task):

#	def output(self):
#        	return luigi.LocalTarget('in/normalized/normalized-%s.csv' % self.date)

#        def run(self):


class GeocodeAddys(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())
    row_limit = 5
    file_limit = 1
    directory_target = 'path/to/folder'


    def requires(self):
        return CleanFiles()

    def output(self):
        return luigi.LocalTarget('./in/geocoded-%s.csv' % self.date)

    def run(self):
        url = 'https://search.mapzen.com/v1/search?api_key=search-iv_vGuI'
        results = []
        urls = []
        df2 = pd.read_csv(self.input().open('r'), dtype= 'str')

        for row in df2.values:
            params= {'text': str(", ".join([str(i) for i in row]))}
            print params
            #params['text'] = (", ".join(params['text']))
            #print params
            r = requests.get(url, params)
            urls.append(r.url) #urls to look at full results later
            results.append(r.json()['features'][0]) # most confident result
            geo = geojson.FeatureCollection(results)

        with self.output().open('wb') as fd:
            fd.write(json.dumps(results))

        with open('./in/test.geojson', 'wb') as fd:
            fd.write(geojson.dumps(geo))

        with open('./in/urls.json', 'wb') as fd:
            fd.write(json.dumps(urls))

class BulkGeo(luigi.WrapperTask):
    date = luigi.DateParameter(default=datetime.date.today())
    def requires(self):
        yield CleanFiles(self)
        yield GeocodeAddys(self.date)


if __name__ == '__main__':
    luigi.run()




        	#def prepare_requests()

        	#def send_requests()

        	#def write_results()
