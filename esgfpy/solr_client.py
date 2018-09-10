import json
import urllib
import logging


class SolrClient(object):

    def __init__(self, solr_base_url='http://localhost:8983/solr'):
        self._solr_base_url = solr_base_url

    def post(self, metadata, solr_core):

        print("POSTING METADATA=%s" % metadata)
        json_data_str = self._to_json(metadata)
        self._post_json(json_data_str, solr_core)

    def _to_json(self, data):
        '''
        Converts a Python dictionary or list to json format
        (with unicode encoding).
        '''
        datastr = json.dumps(
            data,
            indent=4,
            sort_keys=True,
            separators=(',', ': '),
            ensure_ascii=False
        )
        return datastr.encode('utf8')

    def _post_json(self, json_data_str, solr_core):

        req = urllib.request.Request(self._get_solr_post_url(solr_core))
        logging.info("Solr update url=%s" % self._get_solr_post_url(solr_core))
        req.add_header('Content-Type', 'application/json')
        logging.info("Publishing JSON data: %s" % json_data_str)
        response = urllib.request.urlopen(req, json_data_str)
        return response

    def _get_solr_post_url(self, solr_core):
        return "%s/%s/update?commit=true" % (
            self._solr_base_url, solr_core)
