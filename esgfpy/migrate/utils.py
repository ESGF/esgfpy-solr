'''
Common Solr utilities.
'''
import logging
import json
import urllib


def http_get_json(url, params):
    '''
    Sends a GET request to the URL to retrieve a JSON response.
    '''

    if params:
        query_string = urllib.parse.urlencode(params, doseq=True)
        url = url + "?" + query_string

    logging.debug("HTTP GET request: %s" % url)

    try:
        with urllib.request.urlopen(url) as response:
            response_text = response.read().decode("UTF-8")
            response = json.loads(response_text)
            return response

    except Exception as e:
        logging.warning(e)
        return None


def to_json(data):

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


def http_post_json(url, data_dict):

    json_data_str = to_json(data_dict)
    logging.debug("Publishing JSON data: %s" % json_data_str)

    req = urllib.request.Request(url)
    req.add_header('Content-Type', 'application/json')

    with urllib.request.urlopen(req, json_data_str) as response:
            response_text = response.read().decode("UTF-8")
            response = json.loads(response_text)
            return response


def get_timestamp_query(datetime_start, datetime_stop):
    '''Builds the Solr timestamp query between a start and stop datetimes.'''

    datetime_start_string = datetime_start.strftime(
        '%Y-%m-%dT%H:%M:%S.%fZ')
    datetime_stop_string = datetime_stop.strftime(
        '%Y-%m-%dT%H:%M:%S.%fZ')
    timestamp_query = "_timestamp:[%s TO %s]" % (datetime_start_string,
                                                 datetime_stop_string)
    return timestamp_query
