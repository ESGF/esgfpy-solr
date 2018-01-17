# Script to test metadata updates for ESGF Data Challenge #1
import logging
logging.basicConfig(level=logging.INFO)

from esgfpy.update.utils import update_solr

# must target the Solr master
solr_url = 'http://localhost:8984/solr'

# add new field 'data_challange=1' to all dataset records
update_dict = { '*:*': {'data_challange':['1'] } }          
update_solr(update_dict, update='set', solr_url=solr_url, solr_core='datasets')

# copy the value of 'data_node' to a new field 'host'
update_dict = { '*:*': {'host':['$data_node'] } }          
update_solr(update_dict, update='set', solr_url=solr_url, solr_core='datasets')

# add the value 'localhost' to the field 'host'
update_dict = { '*:*': {'host':['localhost'] } }          
update_solr(update_dict, update='add', solr_url=solr_url, solr_core='datasets')

# remove the field 'product'
update_dict = { '*:*': {'product':None } }          
update_solr(update_dict, update='set', solr_url=solr_url, solr_core='datasets')