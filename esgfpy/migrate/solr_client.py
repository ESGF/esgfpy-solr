from esgfpy.migrate.utils import http_get_json, http_post_json


class SolrClient(object):
    '''
    Class to issue query and post requests to a Solr server.
    '''

    def __init__(self, solr_base_url='http://localhost:8983/solr'):
        self._solr_base_url = solr_base_url

    def query(self, solr_core, query, start, rows, fq):
        '''Method to execute a generic Solr query, return all fields.'''

        url = self._solr_base_url + "/" + solr_core + "/select"

        # send request
        params = {"q": query,
                  "fq": fq,
                  "wt": "json",
                  "indent": "true",
                  "start": "%s" % start,
                  "rows": "%s" % rows
                  }

        jdoc = http_get_json(url, params)
        return jdoc['response']

    def post(self, metadata, solr_core):

        url = "%s/%s/update" % (self._solr_base_url, solr_core)
        http_post_json(url, metadata)

    def commit(self, solr_core):
        url = "%s/%s/update" % (self._solr_base_url, solr_core)
        return http_get_json(url, {'commit': 'true'})

    def optimize(self, solr_core):

        url = "%s/%s/update" % (self._solr_base_url, solr_core)
        return http_get_json(url, {'optimize': 'true'})
