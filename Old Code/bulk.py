import requests
import csv
import os
import json
import io
import itertools
import pandas as pd
import geojson



def get_address_json_from_row():
    test = pd.read_csv('out2.csv', usecols = [1,2,3,4])
    reqs = []
    for k, v in test.iterrows():
        params = {"text": str(v.tolist()).strip('[]').replace("'", '')}
        print params
        req = requests.get('https://search.mapzen.com/v1/search?api_key=search-iv_vGuI', params = params)
        reqs.append(req)
    return reqs

#fetch files
def fetch():
    
    with open('out2.csv', 'rb') as f:
        reader = csv.DictReader(f)
        for row in reader:
            r = get_address_json_from_row(row)
#geocode
def bulk(urls):
    results = []
    urls = get_address_json_from_row()
    for i in urls: 
        #address = [val][0][0]
        #params= {'text':address}
        #url = 'http://localhost:3100/v1/search?'
        #r = requests.get(url + 'text=' + address)
        #rjson = r.json()['features'][0]
        #rjson['properties']['query'] = address
        results.append(i.json()['features'][0])
    return results

results = bulk(get_address_json_from_row())
geojson.FeatureCollection(results)
