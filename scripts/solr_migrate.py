'''
Script to drive the migration of Solr records
from a source Solr to a target Solr
'''

import argparse
from urllib.parse import urlencode, urlparse
from urllib.request import urlopen
import logging
import json
from dateutil.parser import parse as dt_parse
from esgfpy.migrate.utils import get_timestamp_query
from esgfpy.migrate.solr2solr import migrate
import time

SLEEP_TIME_SECS = 120

logging.basicConfig(level=logging.INFO)


def list_months_and_years(start_month, start_year, end_month, end_year):
    '''
    Function to list the (month, year) pairs between two extremes.
    '''
    month, year = start_month, start_year
    while True:
        yield month, year
        if (month, year) == (end_month, end_year):
            return
        month += 1
        if (month > 12):
            month = 1
            year += 1


def get_datetime_bins(sourceSolrUrl, collection):
    '''
    Return the "_timestamp" bins in a month for a given Solr, collection
    '''

    datetime_bins = []

    # query Solr for the full _timestamp range
    params = {"q": "*:*",
              "wt": "json",
              "indent": "true",
              "stats": "true",
              "stats.field": "_timestamp",
              "rows": "0"}

    url = (sourceSolrUrl + "/%s/select?" % collection) + urlencode(params)
    logging.debug("Solr request: %s" % url)
    fh = urlopen(url)
    jdoc = fh.read().decode("UTF-8")
    response = json.loads(jdoc)
    minDateTime = response['stats']['stats_fields']['_timestamp']['min']
    maxDateTime = response['stats']['stats_fields']['_timestamp']['max']

    minDateTime = dt_parse(minDateTime).replace(microsecond=0)
    minDateTime = minDateTime.replace(second=0).replace(minute=0)
    minDateTime = minDateTime.replace(hour=0).replace(day=1)
    maxDateTime = dt_parse(maxDateTime).replace(microsecond=0)
    maxDateTime = maxDateTime.replace(second=0).replace(minute=0)
    maxDateTime = maxDateTime.replace(hour=0).replace(day=1)

    mmyys = list_months_and_years(minDateTime.month, minDateTime.year,
                                  maxDateTime.month, maxDateTime.year)
    _minDateTime = minDateTime
    for (mm, yy) in mmyys:
        _minDateTime = _minDateTime.replace(year=yy).replace(month=mm)
        if mm < 12:
            _maxDateTime = _minDateTime.replace(month=mm+1)
        else:
            _maxDateTime = _minDateTime.replace(year=yy+1).replace(month=1)

        datetime_bins.append((_minDateTime, _maxDateTime))

    return datetime_bins


def main(sourceSolrUrl, targetSolrUrl, collections):

    # extract index node from sourceSolrUrl
    parsed_uri = urlparse(sourceSolrUrl)
    hostname = '{uri.netloc}'.format(uri=parsed_uri)
    query = "index_node:%s" % hostname

    # loop over collection
    for collection in collections:

        numRecordsMigrated = 0
        datetime_bins = get_datetime_bins(sourceSolrUrl, collection)

        # loop over datetime intervals
        for (startDateTime, stopDateTime) in datetime_bins:
            logging.info("Migrating %s records from: %s to %s" % (
                collection, startDateTime, stopDateTime))

            fq = get_timestamp_query(startDateTime, stopDateTime)
            _numRecordsMigrated = migrate(
                sourceSolrUrl, targetSolrUrl, collection, query=query, fq=fq)
            numRecordsMigrated += _numRecordsMigrated
            if _numRecordsMigrated > 0:
                time.sleep(SLEEP_TIME_SECS)

        logging.info("Final number of %s records migrated: %s" % (
            collection, numRecordsMigrated))


if __name__ == '__main__':

    # parse command line arguments
    parser = argparse.ArgumentParser(description="Solr migration")

    parser.add_argument('sourceSolrUrl', type=str,
                        help="URL of source Solr "
                        "(example: http://localhost:8983/solr)")

    parser.add_argument('targetSolrUrl', type=str,
                        help="URL of target Solr "
                        "(example: http://localhost:8984/solr)")

    parser.add_argument("collections", nargs='*',
                        help='one or more collections '
                        '(example: datasets files aggregations')
    args_dict = vars(parser.parse_args())

    main(args_dict['sourceSolrUrl'],
         args_dict['targetSolrUrl'],
         args_dict['collections'])
