#!/bin/sh
# Example script that can be run as a crontab to check local CMIP5 replicas
# that have changed in the last 5 days

export CDAT_HOME=/usr/local/conda
source ${CDAT_HOME}/bin/activate esgf-pub

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PYTHONPATH=${SOURCE_DIR}
python ${SOURCE_DIR}/esgfpy/update/check_replicas.py CMIP5 `date +%Y-%m-%d --date='5 days ago'`
