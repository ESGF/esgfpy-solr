#!/bin/bash

# Script to migrate all records from a remote Solr server to a local Solr server.
# Example invocation:
# ./solr_migrate.sh https://esgf-node.jpl.nasa.gov/solr http://localhost:8983/solr datasets files aggregations

set -e

# maximum number of records to migrate in one session
MAX_RECORDS_PER_SESSION=10000

# time to wait in seconds between session
WAIT_SECONDS_BETWEEN_SESIONS=120

# parse command line arguments
solr_source_url=$1
shift
solr_target_url=$1
shift
collections="$@"
echo "Migrating records from Solr: ${solr_source_url} to Solr: ${solr_target_url} collections: ${collections}"

# root directory of this source code repository
SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PARENT_DIR="$(dirname $SOURCE_DIR)"
cd $PARENT_DIR

# loop over collections
for collection in $collections
do
  url="${solr_source_url}/${collection}"'/select/?q=*:*&wt=json&rows=0'
  numTotal=`curl -s "$url" | python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["response"]["numFound"])'`
  echo ""
  echo "Migrating collection=${collection} total number of records=$numTotal"
  
  # migrate at most $maxRecords at a time, continue until all records have been migrated
  startRecord=0
  maxRecords=$MAX_RECORDS_PER_SESSION
  while [ $startRecord -lt $numTotal ]; do
     echo "	Starting record=$startRecord max records=$maxRecords"
     python esgfpy/migrate/solr2solr.py ${solr_source_url} ${solr_target_url} --core ${collection} --start ${startRecord} --max ${maxRecords}
     startRecord=$((startRecord + maxRecords))
     # leave Solr time to issue a soft commit and perhaps a hard commit and optimization
     sleep $WAIT_SECONDS_BETWEEN_SESIONS
  done

done
