#!/bin/sh
# Example script that can be run as a crontab to check local CMIP5 replicas
# that have changed in the last 8 days
# Symlink into /etc/cron.weekly

export CDAT_HOME=/usr/local/conda
source ${CDAT_HOME}/bin/activate esgf-pub

# SOURCE_DIR is parent dir of the 'scripts' dir containing this script (after resolving any symlink e.g. from cron.weekly)
script=${BASH_SOURCE[0]}
SOURCE_DIR=$(python -c "import os.path as P; print P.dirname(P.dirname(P.realpath('$script')))")
export PYTHONPATH=${SOURCE_DIR}

logfile=/esg/log/check_replicas_cron.log
date >> $logfile

# example with --dry-run
#python ${SOURCE_DIR}/esgfpy/update/check_replicas.py CMIP5 $(date -d '8 days ago' +%Y-%m-%d) $(date +%Y-%m-%d) --dry-run

python ${SOURCE_DIR}/esgfpy/update/check_replicas.py CMIP5 $(date -d '8 days ago' +%Y-%m-%d) $(date +%Y-%m-%d) >> $logfile 2>&1
