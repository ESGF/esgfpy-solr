# module to check and fix the 'latest' status of local replica data

import logging
logging.basicConfig(level=logging.DEBUG)
import datetime
import sys
from esgfpy.update.utils import query_solr, update_solr, query_esgf

# URLs
local_master_solr_url = 'http://localhost:8984/solr'
#local_index_node_url = 'http://localhost/esg-search/search/'

# any index node used to retrieve the full list of the index nodes in the federation
esgf_index_node_url = 'https://esgf-node.llnl.gov/esg-search/search/'

DEFAULT_NUM_DAYS_BACKWARD = 7

def check_replicas(project,
                   start_datetime=datetime.datetime.strftime(
                                                             datetime.datetime.now() - datetime.timedelta(days=DEFAULT_NUM_DAYS_BACKWARD), 
                                                             '%Y-%m-%dT%H:%M:%SZ'), 
                   stop_datetime=datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%dT%H:%M:%SZ')):
    '''
    Checks replicas for a specific project.
    By default it will check datasets that have changed in the past week.
    start_datetime, stop_datetime must be string in the format "2017-01-07T00:00:00.831Z"
    '''
    
    logging.info("Checking replicas start datetime=%s stop datetime=%s" % (start_datetime, stop_datetime))
    
    # read last date from file
    '''
    try:
        with open(LAST_DATE_FILE,'r') as myfile: 
            last_time = myfile.readline()
        logging.info("Last run at: %s" % last_time)
    # or query N days backward
    except IOError:
        logging.info("Last run file not found, querying %s days backward" % DEFAULT_NUM_DAYS_BACKWARD)
        last_time = datetime.datetime.now() - datetime.timedelta(days=DEFAULT_NUM_DAYS_BACKWARD)
        last_time = datetime.datetime.strftime(last_time, '%Y-%m-%dT%H:%M:%SZ')
    '''
    
    
    # 0) retrieve the latest list of ESGF index nodes
    # https://esgf-node.jpl.nasa.gov/esg-search/search/?offset=0&limit=0&type=Dataset&facets=index_node&format=application%2Fsolr%2Bjson
    query_params = [ ("offset","0"), ("limit","0"), ("type","Dataset"), ("facets","index_node"), ("format","application/solr+json") ]
    jobj = query_esgf(query_params, esgf_index_node_url)
    # select the even elements of the list "index_node":["esg-dn1.nsc.liu.se", 78954, "esg.pik-potsdam.de",66899, "esgdata.gfdl.noaa.gov",5780,...]
    index_nodes = jobj['facet_counts']['facet_fields']['index_node'][0::2]
    logging.debug("Querying index nodes: %s" % index_nodes)
    
    
    # 1) query all remote index nodes for the latest primary datasets that have changed since last time
    fields = ['id','master_id','version']
    for index_node in index_nodes:  
        # FIXME
        if 'esgf-node.jpl.nasa.gov' in index_node:
            remote_slave_solr_url = 'https://%s/solr' % index_node
            logging.info("Querying Solr=%s" % remote_slave_solr_url)
            query1 = 'replica:false&latest:true&_timestamp:[%s TO %s]&project:%s' % (start_datetime, stop_datetime, project)
            docs1 = query_solr(query1, fields, solr_url=remote_slave_solr_url, solr_core='datasets')
            
            # 2) query local index for replicas of the same datasets that are flagged with latest='true'
            for doc1 in docs1:
                v1 = int( doc1['version'] )
                master_id = doc1['master_id']
                #dataset_id1 = doc1['id']
                logging.info("\n")
                logging.info("Checking local Solr %s for copy of dataset with master_id=%s" % (local_master_solr_url, master_id) )

                # FIXME: replica:true&latest:true
                query2 = 'master_id:%s&replica:false&latest:false' % master_id
                docs2 = query_solr(query2, fields, solr_url=local_master_solr_url, solr_core='datasets')
                
                for doc2 in docs2:
                    
                    # compare versions
                    v2 = int( doc2['version'] )
                    #master_id2 = doc2['master_id']
                    dataset_id2 = doc2['id']

                    if v1 >= v2: # FIXME: v1 > v2
                        logging.warn("Found newer version: %s for dataset: %s at site: %s" % (v2, master_id, remote_slave_solr_url) )
                        logging.warn("Updating status of local dataset: %s to latest=false" % dataset_id2 )
                        
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
    