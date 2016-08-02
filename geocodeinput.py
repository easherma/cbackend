import requests
import csvkit
import os
import json
import io

#fetch files
with open('./out6_file3_address_3_clean.csv', 'rb') as f:
    reader = csvkit.reader(f)
    your_list = list(reader)
    print your_list[0][0]

#geocode
results = []
for i, val in enumerate(your_list):
    address = [val][0][0]
    params= {'text':address}
    url = 'http://localhost:3100/v1/search?'
    r = requests.get(url + 'text=' + address)
    rjson = r.json()['features'][0]
    rjson['properties']['query'] = address
    results.append(rjson)

with open('./out6_file3_address_3_clean.json', 'wb') as fd:
    fd.write(json.dumps(results))
~
#from postal.parser import parse_address
#parse_address('The Book Club 100-106 Leonard St Shoreditch London EC2A 4RH, United Kingdom')
