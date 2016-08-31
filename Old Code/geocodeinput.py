import requests
import csvkit
import os
import json
import io

#fetch files
def openFile():
	with open('./out6_file3_address_3_clean.csv', 'rb') as f:
    		reader = csvkit.reader(f)
    		your_list = list(reader)
    		print your_list[0][0]
    
# input address

#print "Enter address to geocode: "
#address = raw_input()

#geocode
def geocodeInput(address):
    results = []
    url = 'http://localhost:3100/v1/search?'
    r = requests.get(url + 'text=' + address)
    print r.json()
    #rjson = r.json()['features'][0]
    #rjson['properties']['query'] = address
    #results.append(rjson)

def writeOutput(filename):
    with open('./' + filename+ '.json', 'wb') as fd:
        fd.write(json.dumps(results))

