# Script to check and fix the 'latest' status of local replica data
# Usage: check_replicas.py <project> <optional start_date as YYYY-MM-DD> <optional stop_date as YYYY-MM-DD>

import logging
logging.basicConfig(level=logging.INFO)
import datetime
import sys
import urllib2
from esgfpy.update.utils import query_solr, update_solr, query_esgf

# URLs
# local master Solr that will be checked and updated
local_master_solr_url = 'http://localhost:8984/solr'
#local_master_solr_url = 'https://esgf-node.jpl.nasa.gov/solr'

# any ESGF index node used to retrieve the full list of the index nodes in the federation
esgf_index_node_url = 'https://esgf-node.llnl.gov/esg-search/search/'

# by default, the script will check data that changed in the last N days
LAST_NUMBER_OF_DAYS = 7

def check_replicas(project,
                   start_datetime=datetime.datetime.strftime(
                                                             datetime.datetime.now() - datetime.timedelta(days=LAST_NUMBER_OF_DAYS), 
                                                             '%Y-%m-%dT%H:%M:%SZ'), 
                   stop_datetime=datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%dT%H:%M:%SZ')):
    '''
    Checks replicas for a specific project.
    By default it will check datasets that have changed in the past week.
    start_datetime, stop_datetime must be string in the format "2017-01-07T00:00:00.831Z"
    '''
    
    logging.info("Checking replicas start datetime=%s stop datetime=%s" % (start_datetime, stop_datetime))
        
    # 0) retrieve the latest list of ESGF index nodes
    # query: https://esgf-node.jpl.nasa.gov/esg-search/search/?offset=0&limit=0&type=Dataset&facets=index_node&format=application%2Fsolr%2Bjson
    query_params = [ ("offset","0"), ("limit","0"), ("type","Dataset"), ("facets","index_node"), ("format","application/solr+json") ]
    jobj = query_esgf(query_params, esgf_index_node_url)
    # select the even elements of the list "index_node":["esg-dn1.nsc.liu.se", 78954, "esg.pik-potsdam.de",66899, "esgdata.gfdl.noaa.gov",5780,...]
    index_nodes = jobj['facet_counts']['facet_fields']['index_node'][0::2]
    logging.debug("Querying index nodes: %s" % index_nodes)
    
    
    # 1) query all remote index nodes for the latest primary datasets that have changed in the given time period
    fields = ['id','master_id','version']
    for index_node in index_nodes:  

        try:
            
            remote_slave_solr_url = 'https://%s/solr' % index_node
            logging.info("Querying Solr=%s for datasets with project=%s start_datetime=%s stop_datetime=%s" % (remote_slave_solr_url, project, start_datetime, stop_datetime) )
            query1 = 'project:%s&replica:false&latest:true&_timestamp:[%s TO %s]' % (project, start_datetime, stop_datetime)
            docs1 = query_solr(query1, fields, solr_url=remote_slave_solr_url, solr_core='datasets')
            
        except urllib2.HTTPError:
            logging.error("Error querying index node %s" % remote_slave_solr_url)
            docs1 = []
            
        # 2) query local index for replicas of the same datasets that are flagged with latest='true'
        for doc1 in docs1:
            v1 = int( doc1['version'] )
            master_id = doc1['master_id']
            #dataset_id1 = doc1['id']
            logging.info("\tChecking local Solr %s for copy of dataset with master_id=%s" % (local_master_solr_url, master_id) )

            query2 = 'master_id:%s&replica:true&latest:true' % master_id
            docs2 = query_solr(query2, fields, solr_url=local_master_solr_url, solr_core='datasets')
            
            # check local 'latest' replica
            for doc2 in docs2:
                
                # compare versions
                v2 = int( doc2['version'] )
                #master_id2 = doc2['master_id']
                dataset_id2 = doc2['id']

                if v1 > v2: # remote primary has newer version --> local replica must be updated
                    logging.warn("Found newer version: %s for dataset: %s at site: %s" % (v2, master_id, remote_slave_solr_url) )
                    logging.warn("Updating status of local dataset: %s to latest=false" % dataset_id2 )
                    
                    # FIXME
                    # 3) set latest flag of local replica to false for dataset, files, aggregations
                    update_dict = { 'id:%s' % dataset_id2 : {'latest':['false'] } }
                    #update_solr(update_dict, update='set', solr_url=local_master_solr_url, solr_core='datasets')
                    update_dict = { 'dataset_id:%s' % dataset_id2 : {'latest':['false'] } }
                    #update_solr(update_dict, update='set', solr_url=local_master_solr_url, solr_core='files')
                    #update_solr(update_dict, update='set', solr_url=local_master_solr_url, solr_core='aggregations')
        
 
if __name__ == '__main__':
    
    if len(sys.argv) < 2 or len(sys.argv) >4:
        logging.error("Usage: check_replicas.py <project> <optional start_date as YYYY-MM-DD> <optional stop_date as YYYY-MM-DD>")
        sys.exit(-1)
        
    elif len(sys.argv) == 2:
        check_replicas( sys.argv[1] )
        
    elif len(sys.argv) == 3:
        
        check_replicas( sys.argv[1], start_datetime="%sT00:00:00.000Z"  % sys.argv[2] )
        
    elif len(sys.argv) == 4:
        
        check_replicas( sys.argv[1], start_datetime="%sT00:00:00.000Z"  % sys.argv[2],  stop_datetime="%sT00:00:00.000Z"  % sys.argv[3],)
    