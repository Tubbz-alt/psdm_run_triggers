#!/bin/bash

#export PATH=/reg/g/psdm/sw/conda/inst/miniconda2-prod-rhel7/bin:${PATH}
#source activate ana-1.2.12-py3

[ -z "$LOGBOOK_DATABASE_HOST" ] && export LOGBOOK_DATABASE_HOST="localhost"
[ -z "$LOGBOOK_DATABASE_DB" ] && export LOGBOOK_DATABASE_DB="ROLES"
[ -z "$LOGBOOK_DATABASE_USER" ] && export LOGBOOK_DATABASE_USER="test"
[ -z "$LOGBOOK_DATABASE_PASSWORD" ] && export LOGBOOK_DATABASE_PASSWORD="test"


PRNT_DIR=`dirname $PWD`
G_PRNT_DIR=`dirname $PRNT_DIR`;
GG_PRNT_DIR=`dirname $G_PRNT_DIR`;
GGG_PRNT_DIR=`dirname $GG_PRNT_DIR`;
EXTERNAL_CONFIG_FILE="${GGG_PRNT_DIR}/appdata/psdm_run_triggers_config/psdm_run_triggers_config.sh"


if [[ -f "${EXTERNAL_CONFIG_FILE}" ]]
then
   echo "Sourcing deployment specific configuration from ${EXTERNAL_CONFIG_FILE}"
   source "${EXTERNAL_CONFIG_FILE}"
else
   echo "Did not find external deployment specific configuration - ${EXTERNAL_CONFIG_FILE}"
fi

export ACCESS_LOG_FORMAT='%(h)s %(l)s %({REMOTE_USER}i)s %(t)s "%(r)s" "%(q)s" %(s)s %(b)s %(D)s'

export SERVER_IP_PORT="0.0.0.0:9646"

gunicorn run_triggers:app -b ${SERVER_IP_PORT} --worker-class eventlet --reload \
       --log-level=DEBUG --env DEBUG=TRUE --capture-output --enable-stdio-inheritance \
       --access-logfile - --access-logformat "${ACCESS_LOG_FORMAT}"
