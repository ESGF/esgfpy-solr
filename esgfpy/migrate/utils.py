'''
Common Solr utilities.
'''


def get_timestamp_query(datetime_start, datetime_stop):
    '''Builds the Solr timestamp query between a start, stop datetime.'''

    datetime_start_string = datetime_start.strftime(
        '%Y-%m-%dT%H:%M:%S.%fZ')
    datetime_stop_string = datetime_stop.strftime(
        '%Y-%m-%dT%H:%M:%S.%fZ')
    timestamp_query = "_timestamp:[%s TO %s]" % (datetime_start_string,
                                                 datetime_stop_string)
    return timestamp_query
