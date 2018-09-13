# Sample script that reads a JSON configuration file for ESGF quality control
# flags and publishes the information to the Solr index

import logging
from esgfpy.update.utils import update_solr
from pprint import pprint
import json
from urllib.request import urlopen

logging.basicConfig(level=logging.INFO)

# constants
# SOLR_URL = 'http://esgf-node.jpl.nasa.gov:8984/solr'
SOLR_URL = 'http://localhost:8984/solr'
INDICATORS_URL = ('https://raw.githubusercontent.com/EarthSystemCoG/'
                  'esgfpy-publish/master/esgfpy/obs4mips/'
                  'obs4mips_indicators.json')
# INDICATORS_URL = ('https://raw.githubusercontent.com/PCMDI/'
#                   'obs4MIPs-cmor-tables/master/src/tt/obs4MIPs-indicators.json')'
# INDICATORS_URL = ('file:///Users/cinquini/tmp/obs4mips_indicators.json')

# read climate indicators file
response = urlopen(INDICATORS_URL)
html = response.read()
json_data = json.loads(html)
pprint(json_data)
response.close()  # best practice to close the file

# publish to Solr
update_solr(json_data, update='set', solr_url=SOLR_URL, solr_core='datasets')
