# cbackend

Notes on libpostal for parsing/normalizing addresses:

[At the moment, libpostal does not attempt to resolve ambiguities in addresses, and often produces multiple potential expansions. Some may be nonsensical (“Main St” expands to both “Main Street” and “Main Saint”), but the correct form will be among them. The outputs of libpostal’s expand_address can be treated as a set and address matching can be seen as a doing a set intersection, or a JOIN in SQL parlance. In the search setting, one should index all of the strings produced, and use the same code to normalize user queries before sending them to the search server/database.](https://medium.com/@albarrentine/statistical-nlp-on-openstreetmap-b9d573e6cc86#.3k21scg7o)

Running the task:
'python task.py FetchFiles --local-scheduler'

## ogr2ogr example 
ogr2ogr -f "PostgreSQL" PG:"dbname=geotemp user=esherman" "test2.json" -nln full_test2 -append
for python/luigi:

ogr2ogr -f "PostgreSQL" PG:"dbname=geotemp user=esherman" r.json -nln response -append

ogr2ogr -f "PostgreSQL" PG:"dbname=geotemp user=esherman" http://localhost:3100/v1/search?text=2505+HOFFMAN+ST%2CBRONX%2C10458-6047%2CNY -nln response -append

## Luigi examples/current steps and 'gotchas'

params to modify in the code:

https://github.com/easherma/cbackend/blob/master/tasks.py#L91 pick the index numbers of the columns you want to use

_I seem to recall an issue with handiling columns being out of order, but I've got to catch back up after being sick for so long_

### table names
These are hard-coded atm, that means you'll want to go in and change the names unless you want to append results to the same table. 


 python -m luigi --module tasks prepURL --f '/your/client/file' --local-scheduler
 
 python -m luigi --module tasks pipeToDB --prepURL-f '/your/client/file' --local-scheduler



