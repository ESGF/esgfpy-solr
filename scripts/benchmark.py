'''
Script to benchmark Solr query performance
'''

import logging
import ssl
import json
from urllib.request import urlopen
from urllib.parse import urlencode

logging.basicConfig(level=logging.INFO)

# NOTE: PROTOCOL_TLSv1_2 support requires Python 2.7.13+
ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)


class SolrEndpoint():
    '''
    Class that captures the properties of a Solr endpoint.
    '''

    def __init__(self, name, url, shards=""):

        self.name = name
        self.url = url
        if shards:
            self.shards = shards.split(",")
        else:
            self.shards = []

SOLR_ENDPOINTS = [SolrEndpoint("AWS", 
                                "http://a49158e4fb14f11e88f5e02bc8106d18-263704447.us-west-2.elb.amazonaws.com/solr",
                                ""),
                    #SolrEndpoint("GCP",
                    #             "http://35.193.32.14/solr",
                    #             ""),
                    SolrEndpoint("LLNL",
                                 "http://esgf-node.llnl.gov/solr",
                                 "localhost:8983/solr,localhost:8985/solr,localhost:8987/solr,localhost:8988/solr,localhost:8990/solr,localhost:8991/solr,localhost:8992/solr,localhost:8993/solr,localhost:8994/solr,localhost:8995/solr,localhost:8996/solr"),
                    SolrEndpoint('IPSL',
                                 'https://esgf-node.ipsl.upmc.fr/solr',
                                 'localhost:8982/solr,esgf-node.jpl.nasa.gov/solr,esgf-node.llnl.gov/solr,esgdata.gfdl.noaa.gov/solr,esgf.nccs.nasa.gov/solr,esgf.nci.org.au/solr,esgf-data.dkrz.de/solr,esgf-node.ipsl.upmc.fr/solr,esg-dn1.nsc.liu.se/solr,esgf-index1.ceda.ac.uk/solr,esgf-index3.ceda.ac.uk/solr,esg.pik-potsdam.de/solr')
                    ]

#FILE_QUERIES = ["indent=true&q=*%3A*&fq=type%3AFile&facet=true&start=0&rows=10&wt=json"]

QUERIES = [ {'fq':'project:CMIP5', 'fq':'variable:abs550aer', 'fq':'model:CSIRO-Mk3.6.0'} ]

COMMON_PARAMS = {'indent':'true', 'q':'{!cache=false}*:*', 'fq':'type:File', 'facet':'true', 'start':'0', 'rows':'10', 'wt':'json'}

def main():

    for query in QUERIES:

        for solr_endpoint in SOLR_ENDPOINTS:

            params = query.copy()
            params.update(COMMON_PARAMS)
            url = solr_endpoint.url + "/files/select?" + urlencode(params, doseq=True)
            if solr_endpoint.shards:
                shards = ",".join([x+"/files" for x in solr_endpoint.shards])
                print(shards)
                url += "&shards=%s" % shards
        
            logging.info('Executing ESGF query URL=%s' % url)
            fh = urlopen(url)
            response = fh.read().decode("UTF-8")
            jobj = json.loads(response)
            logging.info("Response time=%s Number of records=%s" % (jobj['responseHeader']['QTime'], jobj['response']['numFound']))

        
    

if __name__ == '__main__':
    main()