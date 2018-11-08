'''
Python module to synchronize a source and target Sorl servers.
Records are synchronized by splitting time in progressively smaller
intervals (months, days, hours) and looping backward
since the records most likely to have changed were the latest to be published.
For each interval, the synchronization process checks the total number of records
and the (min, max, mean) of the timestamp distribution.
'''

import logging
import argparse
import urllib
import json
import dateutil.parser
from datetime import timedelta
from monthdelta import monthdelta
from esgfpy.migrate.solr2solr import migrate
from esgfpy.migrate.utils import (
    get_timestamp_query, http_post_json, http_get_json
    )

logging.basicConfig(level=logging.INFO)

DEFAULT_QUERY = "*:*"

CORE_DATASETS = 'datasets'
CORE_FILES = 'files'
CORE_AGGREGATIONS = 'aggregations'
CORES = [CORE_DATASETS, CORE_FILES, CORE_AGGREGATIONS]

DELTA_MONTH = monthdelta(1)
DELTA_DAY = timedelta(days=1)
DELTA_HOUR = timedelta(hours=1)
MAX_DATASETS_PER_HOUR = 10000


class Synchronizer(object):
    '''
    Class that synchronizes records from a source
    Solr server into a target Solr server.
    '''

    def __init__(self, source_solr_base_url, target_solr_base_url):

        self.source_solr_base_url = source_solr_base_url
        self.target_solr_base_url = target_solr_base_url
        logging.info("Synchronizing: %s --> %s" % (source_solr_base_url,
                                                   target_solr_base_url))

    def sync(self, query=DEFAULT_QUERY):
        '''Main method to sync from the source Solr to the target Solr.'''

        logging.info("\tQuery: %s" % query)

        # flag to trigger commit/harvest
        synced = False
        numRecordsSynced = {CORE_DATASETS: 0,
                            CORE_FILES: 0,
                            CORE_AGGREGATIONS: 0}

        # loop over cores
        for core in CORES:

            retDict = self._check_sync(core=core, query=query)

            if not retDict:
                logging.warning("Error synchronizing core=%s, "
                                "skipping it" % core)
                # skip the rest of this iteration
                continue

            if retDict['status']:
                logging.info("Solr cores '%s' are in sync, "
                             "no further action necessary" % core)

            else:

                # must issue commit/optimize before exiting
                synced = True

                # 0) full datetime interval to synchronize
                # aka maximum time interval spanning the two Solrs
                (dt_min,
                 dt_max) = self._get_sync_dt_interval(retDict)
                logging.info("SYNCING: core=%s start=%s stop=%s # records="
                             "%s --> %s" % (core, dt_min, dt_max,
                                            retDict['source']['counts'],
                                            retDict['target']['counts']))

                # 1) loop over MONTHS - backward because it is more likely that
                # the records that changed are the latest
                dt_stop_month = dt_max
                dt_start_month = dt_max
                while dt_stop_month >= (dt_min + DELTA_MONTH):

                    dt_stop_month = dt_start_month
                    dt_start_month = dt_stop_month - DELTA_MONTH
                    logging.info("\tMONTH check: start=%s stop=%s" % (
                        dt_start_month, dt_stop_month))
                    timestamp_query_month = get_timestamp_query(
                        dt_start_month, dt_stop_month)

                    retDict = self._check_sync(core=core, query=query,
                                               fq=timestamp_query_month)

                    # migrate records source_solr --> target_solr
                    if not retDict['status']:
                        logging.info("\tMONTH sync=%s start=%s stop=%s # "
                                     "records=%s --> %s" % (
                                         core, dt_start_month,
                                         dt_stop_month,
                                         retDict['source']['counts'],
                                         retDict['target']['counts']))

                        # 2) loop over DAYS - backward
                        dt_stop_day = dt_stop_month
                        dt_start_day = dt_stop_month
                        while dt_stop_day >= (dt_start_month + DELTA_DAY):

                            dt_stop_day = dt_start_day
                            dt_start_day = (
                                dt_stop_day - DELTA_DAY)
                            logging.info("\t\tDAY check: start=%s stop=%s" % (
                                dt_start_day, dt_stop_day))
                            timestamp_query_day = get_timestamp_query(
                                dt_start_day, dt_stop_day)

                            retDict = self._check_sync(core=core, query=query,
                                                       fq=timestamp_query_day)

                            # migrate records source_solr --> target_solr
                            if not retDict['status']:
                                logging.info("\t\tDAY sync=%s start=%s stop=%s"
                                             " # records=%s --> %s" % (
                                                 core, dt_start_day,
                                                 dt_stop_day,
                                                 retDict['source']['counts'],
                                                 retDict['target']['counts']))

                                # 3) loop over HOURS - backward
                                dt_stop_hour = dt_stop_day
                                dt_start_hour = dt_stop_day
                                while dt_stop_hour >= (dt_start_day + DELTA_HOUR):

                                    dt_stop_hour = dt_start_hour
                                    dt_start_hour = (
                                        dt_stop_hour - DELTA_HOUR)
                                    logging.info("\t\t\tHOUR check start=%s "
                                                 "stop=%s" % (
                                                     dt_start_hour,
                                                     dt_stop_hour))
                                    timestamp_query_hour = get_timestamp_query(
                                        dt_start_hour, dt_stop_hour)

                                    retDict = self._check_sync(core=core,
                                                               query=query,
                                                               fq=timestamp_query_hour)

                                    # migrate records source_solr
                                    # --> target_solr
                                    if not retDict['status']:
                                        logging.info(
                                            "\t\t\tHOUR sync=%s "
                                            "start=%s stop=%s # "
                                            "records=%s --> %s" % (
                                                core,
                                                dt_start_hour,
                                                dt_stop_hour,
                                                retDict['source']['counts'],
                                                retDict['target']['counts']))

                                        # synchronize by dataset id
                                        if core == CORE_DATASETS:
                                            (numDatasets, numFiles,
                                             numAggregations) = (
                                                 self._sync_all_cores_by_dataset_id(
                                                     query,
                                                     timestamp_query_hour))
                                            numRecordsSynced[CORE_DATASETS] += numDatasets
                                            numRecordsSynced[CORE_FILES] += numFiles
                                            numRecordsSynced[CORE_AGGREGATIONS] += numAggregations

                                        # synchronize by datetime interval
                                        else:
                                            numRecordsSynced[core] += (
                                                self._sync_records_by_time(
                                                    core, query,
                                                    timestamp_query_hour))

                                        # check DAY sync again to determine
                                        # whether the hour loop can be stopped
                                        retDict = self._check_sync(
                                            core=core, query=query,
                                            fq=timestamp_query_day)
                                        if retDict['status']:
                                            logging.info(
                                                "\t\tSolr servers are now in "
                                                "sync for DAY: "
                                                "%s" % timestamp_query_day)
                                            # break out of the HOUR bin loop
                                            break

                                # check MONTH sync again to determine whether
                                # the day loop can be stopped
                                retDict = self._check_sync(
                                    core=core, query=query,
                                    fq=timestamp_query_month)
                                if retDict['status']:
                                    logging.info("\tSolr servers are now in "
                                                 "sync for MONTH: "
                                                 "%s" % timestamp_query_month)
                                    # break out of the DAY bin loop
                                    break

                            # check FULL sync again to determine whether
                            # the month loop can be stopped
                            retDict = self._check_sync(core=core, query=query)
                            if retDict['status']:
                                logging.info("Solr servers are now in sync "
                                             "for FULL DATETIME INTERVAL")
                                # break out of the MONTH bin loop
                                break

        # if any synchronization took place
        if synced:

            # commit changes and optimize the target index
            # note that these instructions will be disregarded on Solr Cloud
            self._commit_solr(self.target_solr_base_url)
            self._optimize_solr(self.target_solr_base_url)

            # check status before existing
            for core in CORES:
                logging.info("Core=%s number of records migrated=%s" % (
                    core, numRecordsSynced[core]))
                retDict = self._check_sync(core=core, query=query)
                logging.info("Core=%s sync status=%s number of source "
                             "records=%s number of target records=%s" % (
                                 core,
                                 retDict['status'],
                                 retDict['source']['counts'],
                                 retDict['target']['counts']))

    def _get_sync_dt_interval(self, retDict):
        '''
        Method to compute the full datetime interval
        over which to synchronize the two Solrs.
        '''

        # use largest possible datetime interval
        if retDict['target']['timestamp_max'] is not None:
            dt_max = max(retDict['source']['timestamp_max'],
                         retDict['target']['timestamp_max'])
        else:
            dt_max = retDict['source']['timestamp_max']
        if retDict['target']['timestamp_min'] is not None:
            dt_min = min(retDict['source']['timestamp_min'],
                         retDict['target']['timestamp_min'])
        else:
            dt_min = retDict['source']['timestamp_min']

        # enlarge [dt_min, dt_max] to an integer number of months
        dt_max = dt_max + DELTA_MONTH
        # beginning of month after dt_max
        dt_max = dt_max.replace(day=1, hour=0, minute=0,
                                second=0, microsecond=0)
        # beginning of dt_min month
        dt_min = dt_min.replace(day=1, hour=0, minute=0,
                                second=0, microsecond=0)

        return (dt_min, dt_max)

    def _check_sync(self, core=None, query=DEFAULT_QUERY,
                    fq="_timestamp:[* TO *]"):
        '''
        Method that asserts whether the source and target Solrs are
        synchronized between the datetimes included in the query,
        or over all times if no specific datetime query is provided.
        The method implementation relies on the total number of counts,
        minimum, maximum and mean of timestamp in the given interval.
        '''

        [counts1, timestamp_min1, timestamp_max1, timestamp_mean1] = (
            self._query_solr_stats(self.source_solr_base_url, core, query, fq))
        if counts1 == -1:
            logging.warning("Error querying URL: %s" % self.source_solr_base_url)
            return None
        [counts2, timestamp_min2, timestamp_max2, timestamp_mean2] = (
            self._query_solr_stats(self.target_solr_base_url, core, query, fq))
        if counts2 == -1:
            logging.warning("Error querying URL: %s" % self.target_solr_base_url)
            return None
        logging.debug("SOURCE: counts=%s time stamp min=%s max=%s mean=%s" % (
            counts1, timestamp_min1, timestamp_max1, timestamp_mean1))
        logging.debug("TARGET: counts=%s time stamp min=%s max=%s mean=%s" % (
            counts2, timestamp_min2, timestamp_max2, timestamp_mean2))

        retDict = {'source': {'counts': counts1,
                              'timestamp_min': timestamp_min1,
                              'timestamp_max': timestamp_max1,
                              'timestamp_mean': timestamp_mean1},
                   'target': {'counts': counts2,
                              'timestamp_min': timestamp_min2,
                              'timestamp_max': timestamp_max2,
                              'timestamp_mean': timestamp_mean2}}

        if ((counts1 == counts2
             ) and (timestamp_min1 == timestamp_min2
                    ) and (timestamp_max1 == timestamp_max2
                           ) and (timestamp_mean1 == timestamp_mean2)):
            retDict['status'] = True
        else:
            retDict['status'] = False

        return retDict

    def _query_solr_stats(self, solr_base_url, core, query, fq):
        '''
        Method to query the Solr stats.
        Note: cannot use solrpy because it does not work with 'stats'.
        '''

        url = solr_base_url + "/" + core + "/select"

        # send request
        params = {"q": query,
                  "fq": fq,
                  "wt": "json",
                  "indent": "true",
                  "stats": "true",
                  "stats.field": "_timestamp",
                  "rows": "0"}

        # default values if HTTP response cannot be retrieved
        (counts, timestamp_min, timestamp_max,
         timestamp_mean) = (-1, None, None, None)

        try:
            response = http_get_json(url, params)

            # parse response
            # logging.debug("Solr Response: %s" % response)
            counts = response['response']['numFound']
            try:
                timestamp_min = (
                    response['stats']['stats_fields']['_timestamp']['min'])
            except KeyError:
                timestamp_min = None
            try:
                timestamp_max = (
                    response['stats']['stats_fields']['_timestamp']['max'])
            except KeyError:
                timestamp_max = None
            try:
                timestamp_mean = (
                    response['stats']['stats_fields']['_timestamp']['mean'])
            except KeyError:
                timestamp_mean = None

            # convert strings into datetime objects
            # ignore microseconds for comparison
            if timestamp_min is not None:
                timestamp_min = dateutil.parser.parse(timestamp_min).replace(
                    microsecond=0)
            if timestamp_max is not None:
                timestamp_max = dateutil.parser.parse(timestamp_max).replace(
                    microsecond=0)
            if timestamp_mean is not None:
                timestamp_mean = dateutil.parser.parse(timestamp_mean).replace(
                    microsecond=0)

        except urllib.error.URLError as e:
            logging.warning(e)

        # return output
        return [counts, timestamp_min, timestamp_max, timestamp_mean]

    def _sync_all_cores_by_dataset_id(self, query, timestamp_query):
        '''
        Method that executes synchronization of all cores
        based on the dataset id (within a given time interval).
        '''

        # number of records copied from source Solr --> target Solr
        numDatasets = 0
        numFiles = 0
        numAggregations = 0

        # query for dataset ids from source, target Solrs
        print("Querying source")
        source_dataset_ids = self._query_dataset_ids(self.source_solr_base_url,
                                                     CORE_DATASETS, query,
                                                     timestamp_query)
        print("Querying target")
        target_dataset_ids = self._query_dataset_ids(self.target_solr_base_url,
                                                     CORE_DATASETS, query,
                                                     timestamp_query)

        # synchronize source Solr --> target Solr
        # commit after every core query
        for source_dataset_id in source_dataset_ids.keys():
            if source_dataset_id not in target_dataset_ids or source_dataset_ids[source_dataset_id] != target_dataset_ids[source_dataset_id]:
                logging.info("\t\t\t\tCopying source dataset="
                             "%s" % source_dataset_id)

                numDatasets += migrate(self.source_solr_base_url,
                                       self.target_solr_base_url,
                                       CORE_DATASETS,
                                       query='id:%s' % source_dataset_id,
                                       commit=True,
                                       optimize=False)
                numFiles += migrate(self.source_solr_base_url,
                                    self.target_solr_base_url,
                                    CORE_FILES,
                                    query='dataset_id:%s' % source_dataset_id,
                                    commit=True,
                                    optimize=False)
                numAggregations += migrate(
                    self.source_solr_base_url,
                    self.target_solr_base_url,
                    CORE_AGGREGATIONS,
                    query='dataset_id:%s' % source_dataset_id,
                    commit=True, optimize=False)

        # synchronize target Solr <-- source Solr
        # must delete datasets that do NOT longer exist at the source
        for target_dataset_id in target_dataset_ids.keys():
            if target_dataset_id not in source_dataset_ids:
                # check whether dataset still exists at the source: if yes,
                # it will be updated; if not, must delete
                exists = self._check_record(
                    self.source_solr_base_url,
                    CORE_DATASETS,
                    target_dataset_id)
                if not exists:
                    logging.info("\t\t\t\tDeleting dataset="
                                 "%s" % target_dataset_id)
                    self._delete_solr_records(
                        self.target_solr_base_url,
                        core=CORE_DATASETS,
                        query='id:%s' % target_dataset_id)
                    self._delete_solr_records(
                        self.target_solr_base_url,
                        core=CORE_FILES,
                        query='dataset_id:%s' % target_dataset_id)
                    self._delete_solr_records(
                        self.target_solr_base_url,
                        core=CORE_AGGREGATIONS,
                        query='dataset_id:%s' % target_dataset_id)

        return (numDatasets, numFiles, numAggregations)

    def _check_record(self, solr_base_url, core, record_id):
        '''Checks for the existence of a record with a given id.'''

        solr_url = solr_base_url + "/" + core + "/select"
        response = http_get_json(solr_url, {'q': 'id:%s' % record_id, 'wt': 'json'})

        if int(response["response"]['numFound']) > 0:
            return True
        else:
            return False


    def _sync_records_by_time(self, core, query, timestamp_query):
        '''
        Method that executes synchronization of all records
        for a given core within given time interval.
        '''

        # first delete all records in timestamp bin from target solr
        # will NOT commit the changes yet
        delete_query = "(%s)AND(%s)" % (query, timestamp_query)
        self._delete_solr_records(self.target_solr_base_url,
                                  core,
                                  delete_query)

        # then migrate records from source solr
        # commit but do NOT optimize the index yet
        numRecords = migrate(self.source_solr_base_url,
                             self.target_solr_base_url,
                             core, query=query, fq=timestamp_query,
                             commit=True, optimize=False)
        logging.info("\t\t\tNumber or records migrated=%s" % numRecords)
        return numRecords

    def _delete_solr_records(self, solr_base_url, core, query=DEFAULT_QUERY):

        solr_url = solr_base_url + "/" + core + "/update?commit=true"
        post_dict = {"delete": {"query": query}}
        response = http_post_json(solr_url, post_dict)
        logging.debug("Solr delete response=%s" % response)

    def _query_dataset_ids(self, solr_base_url, core, query, timestamp_query):
        '''
        Method to query for dataset ids within a given datetime interval.
        '''

        datasets = {}
        solr_url = solr_base_url + "/" + core

        # send request
        params = {"q": query,
                  "fq": timestamp_query,
                  "wt": "json",
                  "indent": "true",
                  "start": "0",
                  "rows": "%s" % MAX_DATASETS_PER_HOUR,
                  "fl": ["id", "_timestamp"]
                  }
        url = solr_url + "/select?" + urllib.parse.urlencode(params,
                                                             doseq=True)
        logging.debug("Solr request: %s" % url)
        fh = urllib.request.urlopen(url)
        jdoc = fh.read().decode("UTF-8")
        response = json.loads(jdoc)
        if int(response['response']['numFound']) > 0:
            for doc in response['response']['docs']:
                datasets[doc['id']] = doc['_timestamp']

        return datasets

    def _optimize_solr(self, solr_base_url):

        for core in CORES:

            solr_url = solr_base_url + "/" + core + "/update"
            params = {"optimize": "true", "wt": "json"}
            logging.info("Optimizing the Solr index: %s" % solr_url)
            response = http_get_json(solr_url, params)
            logging.debug(response)

    def _commit_solr(self, solr_base_url):

        for core in CORES:

            solr_url = solr_base_url + "/" + core + "/update"
            params = {"commit": "true", "wt": "json"}
            logging.info("Committing the Solr index: %s" % solr_url)
            response = http_get_json(solr_url, params)
            logging.debug(response)


if __name__ == '__main__':
    '''
    Example invocation:
    python esgfpy/migrate/synchronizer.py \
        'https://esgf-node.jpl.nasa.gov:8983/solr' \
        'http://esgf-cloud.jpl.nasa.gov:8983/solr' \
        --query 'index_node:esgf-node.jpl.nasa.gov'

    '''

    # parse command line arguments
    parser = argparse.ArgumentParser(
        description="Synchronizing tool for ESGF Solr-Cloud")
    parser.add_argument('source', type=str,
                        help="URL of source Solr (example: "
                        "'https://esgf-node.jpl.nasa.gov:8983/solr')",
                        default=None)
    parser.add_argument('target', type=str,
                        help="URL of target Solr (example: "
                        "'http://solr-load-balancer:8983/solr')",
                        default='http://localhost:8983/solr')
    parser.add_argument('--query', dest='query', type=str,
                        help="Query to subset the records namespace in both "
                             "the source and targer Solrs"
                             "(example: 'index_node:esgf-node.jpl.nasa.gov'",
                             default=DEFAULT_QUERY)

    args_dict = vars(parser.parse_args())
    harvester = Synchronizer(args_dict['source'], args_dict['target'])
    harvester.sync(query=args_dict['query'])
