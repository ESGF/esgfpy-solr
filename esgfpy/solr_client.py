import json
import urllib.parse
import urllib.request
import logging


class SolrClient(object):
    '''
    Class to issue query and post requests to a remote Solr server.
    '''

    def __init__(self, solr_base_url='http://localhost:8983/solr'):
        self._solr_base_url = solr_base_url

    def query(self, solr_core, query, start, rows, fq):
        '''
        Method to execute a generic Solr query, return all fields.
        '''

        solr_core_url = self._solr_base_url + "/" + solr_core

        # send request
        params = {"q": query,
                  "fq": fq,
                  "wt": "json",
                  "indent": "true",
                  "start": "%s" % start,
                  "rows": "%s" % rows
                  }
        url = solr_core_url + "/select?" + urllib.parse.urlencode(
            params, doseq=True)
        logging.debug("Solr request: %s" % url)
        fh = urllib.request.urlopen(url)
        jdoc = fh.read().decode("UTF-8")
        jobj = json.loads(jdoc)
        print(jobj)
        return jobj['response']

    def post(self, metadata, solr_core):

        json_data_str = self._to_json(metadata)
        self._post_json(json_data_str, solr_core)

    def _to_json(self, data, indent=0):
        '''
        Converts a Python dictionary or list to json format
        (with unicode encoding).
        '''
        datastr = json.dumps(
            data,
            indent=indent,
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
