import logging
from utils import query_solr, update_solr

logging.basicConfig(level=logging.DEBUG)

# local master Solr that will be checked and updated
local_master_solr_url = 'http://localhost:8984/solr'

# Fields to query to Solr
fields = ['id', 'master_id', 'version']

logging.debug('Get affected dataset id from: {}'.format(local_master_solr_url))
query = 'replica:false'
query += '&mip_era:CMIP6'
query += '&grid:regular*1/2*lat-lon*grid'
res = query_solr(query, fields, solr_url=local_master_solr_url, solr_core='datasets')
logging.info('{} datasets found at {}'.format(len(res), local_master_solr_url))
affected_ids = [i['id'] for i in res]

# Update grid attribute
for d in affected_ids:
    print d
    update_dict = {'id:%s' % d: {'grid': ['regular 1/2 degree lat-lon grid']}}
    update_solr(update_dict,
                update='set',
                solr_url=local_master_solr_url,
                solr_core='datasets')
    update_dict = {'dataset_id:%s' % d: {'grid': ['regular 1/2 degree lat-lon grid']}}
    update_solr(update_dict,
                update='set',
                solr_url=local_master_solr_url,
                solr_core='files')
    update_solr(update_dict,
                update='set',
                solr_url=local_master_solr_url,
                solr_core='aggregations')
