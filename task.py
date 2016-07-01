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



#class CleanFiles(luigi.Task):

#class NormalizeAddys(luigi.Task):
	
#	def output(self):
#        	return luigi.LocalTarget('in/normalized/normalized-%s.csv' % self.date)
        
#        def run(self):
        	

class GeocodeAddys(luigi.Task):
	date = luigi.DateParameter(default=datetime.date.today())
	row_limit = 5
	file_limit = 1
	directory_target = 'path/to/folder'
	file_target = '/home/esherman/cbackend/in/selected/selected-2016-06-23.csv'

	def output(self):
        	return luigi.LocalTarget('in/geocoded/geocoded-%s.csv' % self.date)
        
        def run(self):
        	def get_address_from_row():
        		results = []
        		urls = []
        		with open('selected-2016-06-23.csv','rb') as f:
			    in_csv1= csv.reader(f)
			    for row in in_csv1:
		            	params= {"text": (row[1:])}
		            	params['text'] = (", ".join(params['text']))
		            	r = requests.get(url, params)
		            	urls.append(r.url)
		            	results.append(r.json())
		        panda_results = pd.DataFrame.read_json(results)
		        with open('/test.json', 'wb') as fd:
		        	fd.write(json.dumps(results))
if __name__ == '__main__':
    luigi.run()

        		
        		
        	
        	#def prepare_requests()
        	
        	#def send_requests()
        	
        	#def write_results()
        	
