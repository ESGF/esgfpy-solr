'''
Script to benchmark Solr query performance
'''

import logging
import json
from urllib.request import urlopen
from urllib.parse import urlencode

logging.basicConfig(level=logging.INFO)


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


SOLR_ENDPOINTS = [
                    SolrEndpoint("AWS",
                                 "http://a1f27a255dea211e8b60502bd31928e2-1418248443"
                                 ".us-west-2.elb.amazonaws.com/solr",
                                 ""),
                    SolrEndpoint("LLNL",
                                 "http://esgf-node.llnl.gov/solr",
                                 "localhost:8983/solr,"
                                 "localhost:8985/solr,"
                                 "localhost:8987/solr,"
                                 "localhost:8988/solr,"
                                 "localhost:8990/solr,"
                                 "localhost:8991/solr,"
                                 "localhost:8992/solr,"
                                 "localhost:8993/solr,"
                                 "localhost:8994/solr,"
                                 "localhost:8995/solr,"
                                 "localhost:8996/solr"),
                    SolrEndpoint('IPSL',
                                 'https://esgf-node.ipsl.upmc.fr/solr',
                                 "esgf-node.jpl.nasa.gov/solr,"
                                 "esgf-node.llnl.gov/solr,"
                                 "esgdata.gfdl.noaa.gov/solr,"
                                 "esgf.nccs.nasa.gov/solr,"
                                 "esgf.nci.org.au/solr,"
                                 "esgf-data.dkrz.de/solr,"
                                 "esgf-node.ipsl.upmc.fr/solr,"
                                 "esg-dn1.nsc.liu.se/solr,"
                                 "esgf-index1.ceda.ac.uk/solr,"
                                 "esgf-index3.ceda.ac.uk/solr,"
                                 "esg.pik-potsdam.de/solr"),
                    SolrEndpoint('CEDA',
                                 'https://esgf-index1.ceda.ac.uk/solr',
                                 "esgf-index2.ceda.ac.uk:8997/solr,"
                                 "esgf-index2.ceda.ac.uk:8998/solr,"
                                 "esgf-index2.ceda.ac.uk:9001/solr,"
                                 "esgf-index2.ceda.ac.uk:9003/solr,"
                                 "esgf-index2.ceda.ac.uk:8996/solr,"
                                 "esgf-index2.ceda.ac.uk:8999/solr,"
                                 "esgf-index2.ceda.ac.uk:9000/solr,"
                                 "esgf-index2.ceda.ac.uk:9005/solr,"
                                 "esgf-index2.ceda.ac.uk:9004/solr,"
                                 "localhost:8983/solr,"
                                 "esgf-index3.ceda.ac.uk:8983/solr"),
                    SolrEndpoint('DKRZ',
                                 "https://esgf-data.dkrz.de/solr",
                                 "localhost:8983/solr,"
                                 "localhost:8982/solr,"
                                 "localhost:8986/solr,"
                                 "localhost:8987/solr,"
                                 "localhost:8988/solr,"
                                 "localhost:8989/solr,"
                                 "localhost:8990/solr,"
                                 "localhost:8993/solr,"
                                 "localhost:8994/solr,"
                                 "localhost:8995/solr,"
                                 "localhost:8997/solr,"
                                 "esgdata.gfdl.noaa.gov/solr,"
                                 "localhost:8998/solr"),
                    SolrEndpoint('JPL',
                                 'https://esgf-node.jpl.nasa.gov/solr',
                                 "localhost/solr,"
                                 "localhost:8982/solr,"
                                 "localhost:8990/solr,"
                                 "localhost:8985/solr,"
                                 "localhost:8991/solr,"
                                 "localhost:8993/solr,"
                                 "localhost:8986/solr,"
                                 "localhost:8987/solr,"
                                 "localhost:8988/solr,"
                                 "esgf-index1.ceda.ac.uk/solr,"
                                 "localhost:8995/solr,"
                                 "localhost:8994/solr")
                    ]
#SOLR_ENDPOINTS = [
#                    SolrEndpoint("GCP",
#                                 "http://35.193.32.14/solr",
#                                 ""),
#                    ]

# Dataset queries
'''
CORE = "datasets"
QUERIES = [{"fq": "type:Dataset"},
           {"fq": ["project:CMIP5", "experiment:rcp60",
                   "replica:false", "latest:true"]},
           {"fq": ["project:CMIP5", "experiment:rcp60",
                   "replica:false", "latest:true",
                   "cf_standard_name:mole_concentration_of_calcite"
                   "_expressed_as_carbon_in_sea_water",
                   "data_node:aims3.llnl.gov", "time_frequency:yr",
                   "institute:MIROC"]},
           {"fq": "id:cmip5.output1.MIROC.MIROC-ESM-CHEM.rcp60."
                  "mon.aerosol.aero.r1i1p1.v20120514|aims3.llnl.gov"},
           {"fq": "id:*MIROC*"},
           {"fq": ["datetime_start:[* TO 2001-12-31T23:59:59Z]",
                   "datetime_stop:[2001-01-01T00:00:00Z TO *]"]}
           ]
'''

# File queries
CORE = "files"
QUERIES = [{"fq": "type:File"},
           {"fq": ["project:CMIP5", "experiment:decadal2001",
                   "replica:false", "latest:true"]},
           {"fq": ["project:CMIP5", "experiment:decadal2001",
                   "replica:false", "latest:true",
                   "cf_standard_name:air_temperature",
                   "data_node:esgfcog.cccma.ec.gc.ca",
                   "time_frequency:day",
                   "realm:atmos"]},
           {"fq": "id:cmip5.output.CCCma.CanCM4.decadal2001.day.atmos.r1i1p1."
                  "v20130331.tas_day_CanCM4_decadal2001_r1i1p1_20020101-"
                  "20111231.nc|esgfcog.cccma.ec.gc.ca"},
           #{"fq": "id:*MIROC*"}, # NOT RETURNING FOR LLNL
           {"fq": ["_timestamp:[* TO 2015-12-31T23:59:59Z]",
                   "_timestamp:[2015-01-01T00:00:00Z TO *]"]}
           ]

# common parameters for all queries
COMMON_PARAMS = {'indent': ' true',
                 'q': '{!cache=false}*:*',
                 'facet': 'true',
                 'start': '0',
                 'rows': '10',
                 'wt': 'json'}

OUTPUT_FILE = "/tmp/solr_benchmarking_output.txt"


def main():

    with open(OUTPUT_FILE, "w") as the_file:
        the_file.write("\t".join([x.name for x in SOLR_ENDPOINTS])+"\n")

        for query in QUERIES:
            qTimes = []

            for solr_endpoint in SOLR_ENDPOINTS:

                params = query.copy()
                params.update(COMMON_PARAMS)
                url = solr_endpoint.url + (
                    "/%s/select?" % CORE) + urlencode(params, doseq=True)
                if solr_endpoint.shards:
                    shards = ",".join(
                        [x + ("/%s" % CORE) for x in solr_endpoint.shards])
                    url += "&shards=%s" % shards

                logging.info('Executing ESGF query URL=%s' % url)
                fh = urlopen(url)
                response = fh.read().decode("UTF-8")
                jobj = json.loads(response)
                qTime = jobj['responseHeader']['QTime']
                numFound = jobj['response']['numFound']
                logging.info("Response time=%s Number of records=%s" % (
                    qTime, numFound))
                qTimes.append(qTime)

            # write out the results for this query
            the_file.write("\t".join([str(qt) for qt in qTimes]) + "\n")


if __name__ == '__main__':
    main()
