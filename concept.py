# proof of concept for using pandas and sqlalchemey to join data

import pandas as pd
import json
import requests
from pandas.io.json import json_normalize
import uuid
import sqlalchemy as sq

r = requests.get('https://search.mapzen.com/v1/autocomplete?text=30%20W%2026th%20Street%2C%20New%20York%2C%20NY')
#create uniqueid to use as a foreign key for entries
uniqueid = uuid.uuid4()
output = json.loads(r.text)
#use pandas to parse elements of geojson
features = json_normalize(output['features'])
query = json_normalize(output['geocoding'])
#add columns to dataframes, uuids for linking, bbox to the query metadata just in case its useful
features['id'] = uniqueid
query['id'] = uniqueid
query['bbox'] = json.dumps(output['bbox'])
features['geom'] = json_normalize(r.json(), 'features')['geometry']

engine = sq.create_engine('postgresql://postgres:@localhost:5432/geo')
features.to_sql(name='features', con=engine, if_exists='replace', dtype={'geom': sq.types.JSON})
query.to_sql(name='query', con=engine, if_exists='replace')
