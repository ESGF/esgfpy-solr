# module to check and fix the 'latest' status of local replica data

from esgfpy.update.utils import query_solr, update_solr
import logging


# Solr URLs
local_master_solr_url = 'http://localhost:8984/solr'
# TODO: read list of slave Solr URLs from config file
remote_slave_solr_urls = ['https://esgf-node.jpl.nasa.gov/solr']

def main():

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
        for remote_slave_solr_url in remote_slave_solr_urls:        
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
                
    
if __name__ == '__main__':
    main()
