import argparse
import datetime
import logging

from utils import query_solr, update_solr, query_esgf

logging.basicConfig(level=logging.INFO)

# URLs
# local master Solr that will be checked and updated
local_master_solr_url = 'http://localhost:8984/solr'
# local_master_solr_url = 'https://esgf-fedtest.dkrz.de/solr'

# any ESGF index node used to retrieve the full list
# of the index nodes in the federation
esgf_index_node_url = 'https://esgf-node.ipsl.upmc.fr/esg-search/search/'


def check_replicas(project, dry_run, start, end, ndays):
    """
    Checks replicas for a specific project.
    By default it will check datasets that have changed in the past week.
    start_datetime, stop_datetime must be string in the format
    "2017-01-07T00:00:00.831Z".

    """
    if dry_run:
        logging.info('=========== DRY RUN ===========')
    if ndays:
        if not start:
            start_datetime = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(days=ndays),
                                                        '%Y-%m-%dT%H:%M:%SZ')
        else:
            start_datetime = start
        if not end:
            stop_datetime = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%dT%H:%M:%SZ')
        else:
            stop_datetime = end
    else:
        start_datetime = None
        stop_datetime = None

    msg = 'Checking replicas published'
    if start_datetime and stop_datetime:
        msg += ' from {} to {}'.format(start_datetime, stop_datetime)
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
    num_datasets_unpublish = 0

    # Fields to query to Solr
    fields = ['id', 'master_id', 'version', '_timestamp']

    # 1) query local index for replicas list
    # that are flagged with latest=True
    logging.debug('Get local replicas from: {}'.format(local_master_solr_url))
    query = 'replica:true&latest:true'
    if project == 'CMIP6':
        query += '&mip_era:{}'.format(project)
    else:
        query += '&project:{}'.format(project)
    replicas = query_solr(query, fields, solr_url=local_master_solr_url, solr_core='datasets')
    logging.info('{} replicas found at {}'.format(len(replicas), local_master_solr_url))
    replicas_ids = [(i['master_id'], i['version']) for i in replicas]

    # Assert the list of replicas flagged as latest is unique
    assert len(replicas_ids) == len(set(replicas_ids))

    # 2) query all remote index nodes for the latest primary datasets
    # that have changed in the given time period
    logging.debug('Starting retrieval of primaries dataset')
    for index_node in index_nodes:
        remote_slave_solr_url = 'https://{}/solr'.format(index_node)
        try:
            msg = 'Querying {} for {} datasets published'.format(remote_slave_solr_url, project)
            if start_datetime and stop_datetime:
                msg += ' from {} to {}'.format(start_datetime, stop_datetime)
            logging.info(msg)

            query = 'replica:false&latest:true'
            if project == 'CMIP6':
                query += '&mip_era:{}'.format(project)
            else:
                query += '&project:{}'.format(project)
            if start_datetime and stop_datetime:
                query += '&_timestamp:[{} TO {}]'.format(start_datetime, stop_datetime)
            primaries = query_solr(query, fields, solr_url=remote_slave_solr_url, solr_core='datasets')
            logging.info('{} primaries found at {}'.format(len(primaries), remote_slave_solr_url))
        except:
            logging.error('Error querying {}'.format(remote_slave_solr_url))
            primaries = []

        primaries_ids = [(i['master_id'], i['version']) for i in primaries]
        # Assert the list of primaries flagged as latest is unique
        assert len(primaries_ids) == len(set(primaries_ids))

        # iterate over common datasets between replicas and primaries
        if replicas_ids and primaries_ids:
            logging.info('Compare local replicas with primaries from {}'.format(remote_slave_solr_url))
            for d in set(zip(*replicas_ids)[0]).intersection(zip(*primaries_ids)[0]):
                replicas_versions = sorted([int(r['version']) for r in replicas if r['master_id'] == d])
                primaries_versions = sorted([int(p['version']) for p in primaries if p['master_id'] == d])

                # If version history is different between local replicas and primaries
                if replicas_versions != primaries_versions:
                    # Compare latest version
                    if replicas_versions[-1] < primaries_versions[-1]:
                        msg = 'Found newer'
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
                        msg = 'Found older'
                        with open('to_unpublished.txt', 'a') as f:
                            f.write('{}#{}\n'.format(d, replicas_versions[-1]))
                        # increase counter
                        num_datasets_unpublish += 1
                    msg += ' latest version {} for dataset {} at site {}'.format(primaries_versions[-1],
                                                                                 d,
                                                                                 remote_slave_solr_url)
                    logging.warn(msg)
        else:
            logging.info('Any local replicas match primaries from {}'.format(remote_slave_solr_url))

    logging.info('Total number of local replicas updated: {}\n'.format(num_datasets_updated))
    logging.info('Total number of local replicas to unpublish: {}'.format(num_datasets_unpublish))
    logging.info(msg)


def get_args():
    """
    Returns parsed command-line arguments.

    :returns: The argument parser
    :rtype: *argparse.Namespace*

    """
    main = argparse.ArgumentParser(
        prog='check_replicas',
        description="""Script to check and fix the 'latest' status of local replica data
        Usage: check_replicas.py <project> <optional start_date as YYYY-MM-DD>
        <optional stop_date as YYYY-MM-DD>
        """)
    main.add_argument(
        '-p', '--project',
        metavar='PROJECT',
        type=str,
        help="Project ID")
    main.add_argument(
        '-d', '--dry-run',
        action='store_true',
        default=False,
        help="Dry run mode")
    main.add_argument(
        '-s', '--start',
        metavar='START_DATE',
        type=str,
        default=None,
        help="Start date of the discovery as YYYY-MM-DD")
    main.add_argument(
        '-e', '--end',
        metavar='END_DATE',
        type=str,
        default=None,
        help="End date of the discovery as YYYY-MM-DD")
    main.add_argument(
        '-n', '--ndays',
        metavar='LAST_NUMBER_OF_DAYS',
        type=int,
        default=None,
        help="Check data that changed in the last N days. Default set to one week.")
    return main.parse_args()


if __name__ == '__main__':
    args = get_args()
    check_replicas(project=args.project,
                   dry_run=args.dry_run,
                   start=args.start,
                   end=args.end,
                   ndays=args.ndays)
