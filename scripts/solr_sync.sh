#!/bin/bash

# Script to sync all records from a remote Solr server to a local Solr server.
# Example invocation:
# ./solr_sync.sh http://esgdata.gfdl.noaa.gov/solr http://localhost:8983/solr

set -e

# parse command line arguments
solr_source_url=$1
solr_target_url=$2
index_node=`echo $solr_source_url | awk -F[/:] '{print $4}'`
echo "Syncing records from Solr: ${solr_source_url} to Solr: ${solr_target_url} (all collections) with constraint: index_node=$index_node"

# root directory of this source code repository
SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PARENT_DIR="$(dirname $SOURCE_DIR)"
cd $PARENT_DIR

python esgfpy/harvest/harvester.py "${solr_source_url}" "${solr_target_url}" --query=index_node:${index_node}
