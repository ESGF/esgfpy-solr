# module to check and fix the 'latest' status of local replica data

from esgfpy.update.utils import query_solr, update_solr, query_esgf
import logging
import datetime


# URLs
local_master_solr_url = 'http://localhost:8984/solr'
local_index_node_url = 'http://localhost/esg-search/search/'
#local_index_node_url = 'https://esgf-node.llnl.gov/esg-search/search/'

def main():
    
    now = datetime.datetime.now()
    logging.info("Checking replicas at %s" % str(now))
    
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
                
    
if __name__ == '__main__':
    main()
