import logging
logging.basicConfig(level=logging.INFO)

from esgfpy.update.utils import update_solr

# must target the Solr master
solr_url = 'http://localhost:8984/solr'

# SET a field values (i.e. will override values for fields with the same name, or insert new ones if not existing already)
update_dict = { 'project:obs4MIPs': {'location':['Pasadena'], 'realm':['atmosphere'] } }          
update_solr(update_dict, update='set', solr_url=solr_url, solr_core='datasets')

# ADD values to existing fields, or add new fields with values
update_dict = { 'project:obs4MIPs&source_id:MLS': {'location':['Boulder'], 'stratus':['cumulus'] } }          
update_solr(update_dict, update='add', solr_url=solr_url, solr_core='datasets')

# REMOVE existing fields - set their values to None or empty list
update_dict = { 'project:obs4MIPs': {'location':[], 'stratus':None } }          
update_solr(update_dict, update='set', solr_url=solr_url, solr_core='datasets')

# transfer the value of a field yo a new named field
update_dict = { 'project:obs4MIPs': {'activity_id':['$project'] } }          
update_solr(update_dict, update='set', solr_url=solr_url, solr_core='datasets')

# change version, replica, latest
update_dict = { 'project:obs4MIPs': {'replica':['true'], 'version':['1'], 'latest':['true'] } }          
update_solr(update_dict, update='set', solr_url=solr_url, solr_core='datasets')
