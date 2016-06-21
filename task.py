# Luigi is a framework for building data pipelines and managing workflows.
# It also gives us some visualization tools and a nifty command line interface
# Data pipelines are built through defined 'Task' instances. Each Task can be dependent
# on a previous task, and has a defined output for each task that defines how the task is completed and where the results are written

import luigi
import itertools
import datetime
import csv

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
        return luigi.LocalTarget('temp/fetched-%s.txt' % self.date)

    def run(self):
        for i in itertools.islice(csv.DictReader(open('../temp/out6_file3_address_10_clean.csv')),5 ):
		print i
     #       with self.output().open('w+') as f:
     #           f.write(i)

if __name__ == '__main__':
    luigi.run()



#class CleanFiles(luigi.Task):

#class NormalizeAddys(luigi.Task):

#class GeocodeAddys(luigi.Task):



<<<<<<< HEAD
#with open(address_list, 'rb') as f:
=======
#ith open(address_list, 'rb') as f:
>>>>>>> 74090f666455541028d0fde1b796931238aca305
#    in_csv1 = csv.DictReader(f)
#    for row in in_csv1:
