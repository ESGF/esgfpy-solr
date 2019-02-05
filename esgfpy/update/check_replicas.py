# Script to check and fix the 'latest' status of local replica data
# Usage: check_replicas.py <project> <optional start_date as YYYY-MM-DD>
# <optional stop_date as YYYY-MM-DD>

import logging
import datetime
import sys
import urllib
from esgfpy.update.utils import query_solr, update_solr, query_esgf

logging.basicConfig(level=logging.INFO)

# URLs
# local master Solr that will be checked and updated
local_master_solr_url = 'http://localhost:8984/solr'
# local_master_solr_url = 'http://localhost:8983/solr'

# any ESGF index node used to retrieve the full list
# of the index nodes in the federation
esgf_index_node_url = 'https://esgf-node.ipsl.upmc.fr/esg-search/search/'

# by default, the script will check data that changed in the last N days
LAST_NUMBER_OF_DAYS = 7


def check_replicas(project,
                   start_datetime=datetime.datetime.strftime(
                       datetime.datetime.now() - datetime.timedelta(
                           days=LAST_NUMBER_OF_DAYS), '%Y-%m-%dT%H:%M:%SZ'),
                   stop_datetime=datetime.datetime.strftime(
                       datetime.datetime.now(), '%Y-%m-%dT%H:%M:%SZ'),
                   dry_run=False):
    """
    Checks replicas for a specific project.
    By default it will check datasets that have changed in the past week.
    start_datetime, stop_datetime must be string in the format
    "2017-01-07T00:00:00.831Z".
    """
    msg = 'Checking replicas published from {} to {}'.format(start_datetime, stop_datetime)
    if dry_run:
        msg += ' :: DRY RUN ::'
    logging.info(msg)

    # 0) retrieve the latest list of ESGF index nodes
    # query: https://esgf-node.jpl.nasa.gov/esg-search/search/?offset=0
    #  &limit=0&type=Dataset&facets=index_node&format=application%2Fsolr%2Bjson
    query_params = [("offset", "0"),
                    ("limit", "0"),
                    ("type", "Dataset"),
                    ("facets", "index_node"),
                    ("format", "application/solr+json")]
    jobj = query_esgf(query_params, esgf_index_node_url)
    # select the even elements of the list (starting at inde x0):
    # "index_node":["esg-dn1.nsc.liu.se", 78954, "esg.pik-potsdam.de",66899,
    #               "esgdata.gfdl.noaa.gov",5780,...]
    index_nodes = jobj['facet_counts']['facet_fields']['index_node'][0::2]
    logging.debug('Querying index nodes: {}'.format(index_nodes))

    # counter
    num_datasets_updated = 0

    # Fields to query to Solr
    fields = ['id', 'master_id', 'version', '_timestamp']

    # 1) query local index for replicas list
    # that are flagged with latest=True
    logging.debug('Get local replicas from: {}'.format(local_master_solr_url))
    query = ('project:%s&replica:true&latest:true' % (project))
    replicas = query_solr(query, fields, solr_url=local_master_solr_url, solr_core='datasets')
    replicas_ids = [i['master_id'] for i in replicas]

    # Assert the list of replicas flagged as latest is unique
    assert len(replicas_ids) == len(set(replicas_ids))

    # 2) query all remote index nodes for the latest primary datasets
    # that have changed in the given time period
    logging.debug('Starting retrieval of primaries dataset')
    for index_node in index_nodes:
        remote_slave_solr_url = 'https://{}/solr'.format(index_node)
        try:
            msg = 'Querying {} for {} datasets published from {} to {}'.format(remote_slave_solr_url,
                                                                               project,
                                                                               start_datetime,
                                                                               stop_datetime)
            logging.info(msg)
            query = ('project:%s&replica:false&latest:true'
                      '&_timestamp:[%s TO %s]' % (
                          project, start_datetime, stop_datetime))
            primaries = query_solr(query, fields, solr_url=remote_slave_solr_url, solr_core='datasets')
        except urllib.error.HTTPError:
            logging.error('Error querying {}'.format(remote_slave_solr_url))
            primaries = []

        primaries_ids = [(i['master_id'], i['version']) for i in primaries]
        # Assert the list of primaries flagged as latest is unique
        assert len(primaries_ids) == len(set(primaries_ids))

        # iterate over common datasets between replicas and primaries
        logging.debug('Compare local replicas with primaries from {}'.format(remote_slave_solr_url))
        for d in set(zip(*replicas_ids)[0]).intersection(zip(*primaries_ids)[0]):
            replicas_versions = sorted([int(r['version']) for r in replicas if r['master_id'] == d])
            primaries_versions = sorted([int(p['version']) for p in primaries if p['master_id'] == d])

            # If version history is different between local replicas and primaries
            if replicas_versions != primaries_versions:
                # Compare latest version
                if replicas_versions[-1] < primaries_versions[-1]:
                    msg = 'Found newer latest version {} for dataset {} at site {}'.format(primaries_versions[-1],
                                                                                           d,
                                                                                           remote_slave_solr_url)
                    logging.warn(msg)
                    # Update solr latest flag metadata
                    # 3) set latest flag of local replica to false for datasets, files, aggregations
                    if not dry_run:
                        update_dict = {'id:%s' % d: {'latest': ['false']}}
                        update_solr(update_dict,
                                    update='set',
                                    solr_url=local_master_solr_url,
                                    solr_core='datasets')
                        update_dict = {'dataset_id:%s' % d: {'latest': ['false']}}
                        update_solr(update_dict,
                                    update='set',
                                    solr_url=local_master_solr_url,
                                    solr_core='files')
                        update_solr(update_dict,
                                    update='set',
                                    solr_url=local_master_solr_url,
                                    solr_core='aggregations')
                    # increase counter
                    num_datasets_updated += 1
                else:
                    logging.warn("\t\tFound older latest master version: %s for dataset: %s at site: %s" % (
                        max(replicas_versions), d, remote_slave_solr_url))
                    # TODO: Trigger unpublication ??

    msg = 'Total number of local replicas updated: {}'.format(num_datasets_updated)
    logging.info(msg)

if __name__ == '__main__':

    if len(sys.argv) < 2 or len(sys.argv) > 5:
        logging.error("Usage: check_replicas.py <project> "
                      "<optional start_date as YYYY-MM-DD> "
                      "<optional stop_date as YYYY-MM-DD> "
                      "<optional '--dry_run'>")
        sys.exit(-1)

    elif len(sys.argv) == 2:
        check_replicas(sys.argv[1])

    elif len(sys.argv) == 3:
        check_replicas(sys.argv[1],
                       start_datetime="%sT00:00:00.000Z" % sys.argv[2])

    elif len(sys.argv) == 4:
        check_replicas(sys.argv[1],
                       start_datetime="%sT00:00:00.000Z" % sys.argv[2],
                       stop_datetime="%sT00:00:00.000Z" % sys.argv[3])

    elif len(sys.argv) == 5:
        if 'dry_run' in sys.argv[4]:
            dry_run = True
        else:
            dry_run = False

        check_replicas(sys.argv[1],
                       start_datetime="%sT00:00:00.000Z" % sys.argv[2],
                       stop_datetime="%sT00:00:00.000Z" % sys.argv[3],
                       dry_run=dry_run)
