'''
Python module for bulk migration of records
from a source Solr to a target Solr.
'''

import argparse
import datetime
import logging

from esgfpy.solr_client import SolrClient


MAX_RECORDS_PER_REQUEST = 100

# total number of records to be migrated
MAX_RECORDS_TOTAL = 9999999
DEFAULT_QUERY = '*:*'

logging.basicConfig(level=logging.INFO)


def migrate(sourceSolrUrl, targetSolrUrl, core,
            query=DEFAULT_QUERY, fq=None,
            start=0, maxRecords=MAX_RECORDS_TOTAL,
            replace=None, suffix='', commit=True, optimize=True):
    '''
    By default, it commits the changes and optimizes the index
    when all records have been migrated, but note that these client directives
    will be disregarded by an ESGF SolrCloud cluster.
    '''

    replacements = {}
    if replace is not None and len(replace) > 0:
        replaces = replace.split(":\s+")
        for _replace in replaces:
            (oldValue, newValue) = _replace.split(':')
            replacements[oldValue] = newValue
            logging.debug("Replacing metadata: "
                          "%s --> %s" % (oldValue, newValue))
    t1 = datetime.datetime.now()

    # Solr clients
    s1 = SolrClient(sourceSolrUrl)
    s2 = SolrClient(targetSolrUrl)

    # number of records migrated so far <= maxRecords
    numRecords = 0
    numFound = start+1
    while start < numFound and numRecords < maxRecords:

        # try migrating MAX_RECORDS_PER_REQUEST records at once
        try:
            # do NOT migrate more records than this number
            _maxRecords = min(maxRecords-numRecords, MAX_RECORDS_PER_REQUEST)
            (_numFound, _numRecords) = _migrate(s1, s2, core, query, fq,
                                                start, _maxRecords,
                                                replacements, suffix,
                                                commit=commit)
            numFound = _numFound
            start += _numRecords
            numRecords += _numRecords

        # in case of error, migrate 1 record at a time
        except Exception:
            for i in range(MAX_RECORDS_PER_REQUEST):
                if start < numFound and numRecords < maxRecords:
                    try:
                        (_numFound, _numRecords) = _migrate(
                            s1, s2, core, query, fq, start, 1,
                            replacements, suffix)
                    except Exception as e:
                        logging.warn('ERROR migrating record %s: %s' % (i, e))
                    start += 1
                    numRecords += 1

    # optimize the full index (optimize=True implies commit=True)
    if optimize:
        logging.debug("Optimizing the index for core=%s ..." % core)
        s2.optimize(core)
        logging.debug("...done")

    # just commit the changes but do not optimize
    elif commit:
        logging.debug("Committing changes to the index for core=%s ..." % core)
        s2.commit(core)
        logging.debug("...done")

    t2 = datetime.datetime.now()
    logging.info("Total number of records migrated: %s" % numRecords)
    logging.info("Total elapsed time: %s" % (t2-t1))

    return numRecords


def _migrate(s1, s2, core, query, fq, start, howManyMax, replacements, suffix,
             commit=True):
    '''
    Migrates 'howManyMax' records starting at 'start'.
    By default, it commits the changes after this howManyRecords
    have been migrated, but does NOT optimize the index.
    '''

    logging.info("Migrating records: start record=%s max records per request="
                 "%s" % (start, howManyMax))

    # query records from source Solr
    fquery = []
    if fq is not None:
        fquery = [fq]
    logging.info("Querying: query=%s start=%s rows=%s "
                 "fq=%s" % (query, start, howManyMax, fquery))
    response = s1.query(core, query, start=start, rows=howManyMax, fq=fquery)
    _numFound = response['numFound']
    _numRecords = len(response['docs'])
    logging.info("Query returned numFound=%s numRecords=%s" % (
        _numFound, _numRecords))

    # post records to target Solr
    for result in response['docs']:

        # remove "_version_" field otherwise Solr will return
        # an HTTP 409 error (Conflict)
        # by design, "_version_" > 0 will only insert the document
        # if it exists already with the same _version_
        if result.get("_version_", None):
            del result['_version_']

        # append suffix to all ID fields
        result['id'] = result['id'] + suffix
        result['master_id'] = result['master_id'] + suffix
        result['instance_id'] = result['instance_id'] + suffix
        if result.get("dataset_id", None):
            result['dataset_id'] = result['dataset_id'] + suffix

        # apply replacement patterns
        if len(replacements) > 0:
            for key, value in result.items():
                # multiple values
                if hasattr(value, "__iter__"):
                    result[key] = []
                    for _value in value:
                        result[key].append(_replaceValue(_value, replacements))
                # single value
                else:
                    result[key] = _replaceValue(value, replacements)

        # Fix broken dataset records
        if core == 'datasets':
            for field in ['height_bottom', 'height_top']:
                value = result.get(field, None)
                if value:
                    try:
                        result[field] = float(value)
                    except ValueError:
                        result[field] = 0.

    logging.debug("Adding %s results..." % len(response['docs']))
    # post all records at once
    s2.post(response['docs'], core)
    logging.debug("...done adding")

    logging.info("Response: current number of records=%s total number of "
                 "records=%s" % (start+_numRecords, _numFound))
    return (_numFound, _numRecords)


def _replaceValue(value, replacements):
    '''Apply dictionary of 'replacements' patterns to the string 'value'.'''

    if isinstance(value, str):
        for oldValue, newValue in replacements.items():
            value = value.replace(oldValue, newValue)

    return value


if __name__ == '__main__':

    # parse command line arguments
    parser = argparse.ArgumentParser(description="Migration tool "
                                     "for Solr indexes")
    parser.add_argument('sourceSolrUrl', type=str,
                        help="URL of source Solr "
                        "(example: http://localhost:8983/solr)")
    parser.add_argument('targetSolrUrl', type=str,
                        help="URL of target Solr "
                        "(example: http://localhost:8984/solr)")
    parser.add_argument('--core', dest='core', type=str,
                        help="URL of target Solr "
                        "(example: --core datasets)",
                        default=None)
    parser.add_argument('--query', dest='query', type=str,
                        help="Optional query to sub-select records "
                        "(example: --query project:xyz)",
                        default=DEFAULT_QUERY)
    parser.add_argument('--start', dest='start', type=int,
                        help="Optional first record to be migrated "
                        "(example: --start 1000)",
                        default=0)
    parser.add_argument('--replace', dest='replace', type=str,
                        help="Optional string replacements for all field "
                        "(example: --replace old_value_1:new_value_1,"
                        "old_value_2:new_value_2)",
                        default=None)
    parser.add_argument('--max', dest='max', type=int,
                        help="Optional maxRecords number of records to be "
                        "migrated (example: --max 1000)",
                        default=MAX_RECORDS_TOTAL)
    parser.add_argument('--suffix', dest='suffix', type=str,
                        help="Optional suffix string to append to all record "
                        "ids (example: --suffix abc)",
                        default='')
    args_dict = vars(parser.parse_args())

    # execute migration
    migrate(args_dict['sourceSolrUrl'],
            args_dict['targetSolrUrl'],
            core=args_dict['core'],
            query=args_dict['query'],
            start=args_dict['start'],
            replace=args_dict['replace'],
            maxRecords=args_dict['max'],
            suffix=args_dict['suffix'])
