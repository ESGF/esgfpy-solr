# module to check and fix the 'latest' status of local replica data

import logging
logging.basicConfig(level=logging.DEBUG)
import datetime
from esgfpy.update.utils import query_solr, update_solr, query_esgf

# URLs
local_master_solr_url = 'http://localhost:8984/solr'
#local_index_node_url = 'http://localhost/esg-search/search/'
local_index_node_url = 'https://esgf-node.llnl.gov/esg-search/search/'

LAST_DATE_FILE = '/tmp/check_replicas_last_run.txt'
PROJECT = 'obs4MIPs' # FIXME
DEFAULT_NUM_DAYS_BACKWARD = 10

def main():
    
    # current date
    this_time = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%dT%H:%M:%SZ')
    logging.info("Checking replicas at %s" % this_time)
    
    # read last date from file
    try:
        with open(LAST_DATE_FILE,'r') as myfile: 
            last_time = myfile.readline()
        logging.info("Last run at: %s" % last_time)
    # or query N days backward
    except IOError:
        logging.info("Last run file not found, querying %s days backward" % DEFAULT_NUM_DAYS_BACKWARD)
        last_time = datetime.datetime.now() - datetime.timedelta(days=DEFAULT_NUM_DAYS_BACKWARD)
        last_time = datetime.datetime.strftime(last_time, '%Y-%m-%dT%H:%M:%SZ')
    
    
    # 0) retrieve the latest list of ESGF index nodes
    # https://esgf-node.jpl.nasa.gov/esg-search/search/?offset=0&limit=0&type=Dataset&facets=index_node&format=application%2Fsolr%2Bjson
    query_params = [ ("offset","0"), ("limit","0"), ("type","Dataset"), ("facets","index_node"), ("format","application/solr+json") ]
    jobj = query_esgf(query_params, local_index_node_url)
    # select the even elements of the list "index_node":["esg-dn1.nsc.liu.se", 78954, "esg.pik-potsdam.de",66899, "esgdata.gfdl.noaa.gov",5780,...]
    index_nodes = jobj['facet_counts']['facet_fields']['index_node'][0::2]
    logging.debug("Querying index nodes: %s" % index_nodes)
    
    # FIXME
    last_time = "2017-01-07T00:00:00.831Z"
    
    # 1) query all remote index nodes for the latest primary datasets that have changed since last time
    fields = ['id','master_id','version']
    for index_node in index_nodes:  
        # FIXME
        #if not index_node in local_index_node_url: # don't query the local index node
        if 'esgf-node.jpl.nasa.gov' in index_node:
            remote_slave_solr_url = 'https://%s/solr' % index_node
            logging.info("Querying Solr=%s" % remote_slave_solr_url)
            query1 = 'replica:false&latest:true&_timestamp:[%s TO *]&project:%s' % (last_time, PROJECT)
            docs1 = query_solr(query1, fields, solr_url=remote_slave_solr_url, solr_core='datasets')
            #print docs1
            
            # 2) query local index for replicas of the same datasets that are flagged with latest='true'
            for doc1 in docs1:
                v1 = int( doc1['version'] )
                master_id = doc1['master_id']
                dataset_id1 = doc1['id']
                logging.info("\n")
                logging.info("Checking local Solr %s for copy of dataset with master_id=%s" % (local_master_solr_url, master_id) )

                # FIXME: replica:true
                query2 = 'master_id:%s&replica:false&latest:true' % master_id
                docs2 = query_solr(query2, fields, solr_url=local_master_solr_url, solr_core='datasets')
                
                for doc2 in docs2:
                    
                    # compare versions
                    v2 = int( doc2['version'] )
                    #master_id2 = doc2['master_id']
                    dataset_id2 = doc2['id']

                    if v1 >= v2: # FIXME
                        logging.warn("Found newer version: %s for dataset: %s at site: %s" % (v2, master_id, remote_slave_solr_url) )
                        logging.warn("Updating status of local dataset: %s to latest=false" % dataset_id2 )
                        
                        # 3) set latest flag of local replica to false for dataset, files, aggregations
                        update_dict = { 'id:%s' % dataset_id2 : {'latest':['false'] } }
                        update_solr(update_dict, update='set', solr_url=local_master_solr_url, solr_core='datasets')
                        update_dict = { 'dataset_id:%s' % dataset_id2 : {'latest':['false'] } }
                        update_solr(update_dict, update='set', solr_url=local_master_solr_url, solr_core='files')
                        update_solr(update_dict, update='set', solr_url=local_master_solr_url, solr_core='aggregations')
 
    
    # write out current date to file
    with open(LAST_DATE_FILE,'w') as myfile: 
        myfile.write( this_time )    
    
    ''''
    
    # 0) retrieve the latest list of ESGF index nodes
    # https://esgf-node.jpl.nasa.gov/esg-search/search/?offset=0&limit=0&type=Dataset&facets=index_node&format=application%2Fsolr%2Bjson
    query_params = [ ("offset","0"), ("limit","0"), ("type","Dataset"), ("facets","index_node"), ("format","application/solr+json") ]
    jobj = query_esgf(query_params, local_index_node_url)
    # select the even elements of the list "index_node":["esg-dn1.nsc.liu.se", 78954, "esg.pik-potsdam.de",66899, "esgdata.gfdl.noaa.gov",5780,...]
    index_nodes = jobj['facet_counts']['facet_fields']['index_node'][0::2]
    logging.debug("Querying index nodes: %s" % index_nodes)

    # 1) query for all local datasets that are replicas and marked as latest
    query1 = 'replica:true&latest:true'
    fields = ['id','master_id','version']
    docs1 = query_solr(query1, fields, solr_url=local_master_solr_url, solr_core='datasets')
    
    # 2) query all other slave Solrs for the primary copy
    for doc1 in docs1:
        
        v1 = int( doc1['version'] )
        master_id = doc1['master_id']
        dataset_id = doc1['id']
        logging.info("\n")
        logging.info("Checking dataset id=%s master_id=%s version=%s" % (dataset_id, master_id, v1) )
        doc_updated = False # flag to avoid additional queries after a dataset has been updated
        for index_node in index_nodes:  
            if not doc_updated and not index_node in local_index_node_url: # don't query the local index node
                remote_slave_solr_url = 'https://%s/solr' % index_node
                logging.info("Querying Solr=%s" % remote_slave_solr_url)
                query2 = 'master_id:%s&replica:false&latest:true' % master_id
                docs2 = query_solr(query2, fields, solr_url=remote_slave_solr_url, solr_core='datasets')
                for doc2 in docs2:
                    
                    # compare versions
                    v2 = int( doc2['version'] )
                    if v2 > v1:
                        logging.warn("Found newer version: %s for dataset: %s at site: %s" % (v2, master_id, remote_slave_solr_url) )
                        logging.warn("Updating status of local dataset: %s to latest=false" % dataset_id )
                        
                        # 3) set latest flag of local replica to false for dataset, files, aggregations
                        update_dict = { 'id:%s' % dataset_id : {'latest':['false'] } }
                        update_solr(update_dict, update='set', solr_url=local_master_solr_url, solr_core='datasets')
                        update_dict = { 'dataset_id:%s' % dataset_id : {'latest':['false'] } }
                        update_solr(update_dict, update='set', solr_url=local_master_solr_url, solr_core='files')
                        update_solr(update_dict, update='set', solr_url=local_master_solr_url, solr_core='aggregations')
                        doc_updated = True # no more index node querues for this dataset
                
    '''
    
if __name__ == '__main__':
    main()
