import logging
logging.basicConfig(level=logging.DEBUG)

from esgfpy.update.utils import updateSolr

# must target the Solr master
SOLR_URL = 'http://localhost:8984/solr'

# SET a field values (i.e. will override values for fields with the same name, or insert new ones if not existing already)
myDict = { 'project:obs4MIPs': {'location':['Pasadena'], 'realm':['atmosphere'] } }          
updateSolr(myDict, update='set', solr_url=SOLR_URL, solr_core='datasets')

# ADD values to existing fields, or add new fields with values
#myDict = { 'project:obs4MIPs': {'location':['Boulder'], 'stratus':['cumulus'] } }          
#updateSolr(myDict, update='add', solr_url=SOLR_URL, solr_core='datasets')

# REMOVE existing fields - set their values to None or empty list
#myDict = { 'project:obs4MIPs': {'location':[], 'stratus':None } }          
#updateSolr(myDict, update='set', solr_url=SOLR_URL, solr_core='datasets')

# transfer the value of a field yo a new named field
#myDict = { 'project:obs4MIPs': {'activity_id':['$project'] } }          
#updateSolr(myDict, update='set', solr_url=SOLR_URL, solr_core='datasets')
