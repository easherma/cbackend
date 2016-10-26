# cbackend

Running a batch geocode process:

* Clone this repo to your home directory

* Edit luigi.cfg, follow the comments
 * _Note: a schema will be created based on your username. This is where your tables will be saved.
 
* Run a batch from the command line:
```
python -m luigi --module tasks pipeToDB --local-scheduler
```






