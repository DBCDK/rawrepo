#!/bin/bash
#set -x
SOLR_PATH=${SOLR_PATH:-WHAT} # silencing annoying intellij syntax quibble
SOLR_HOST=${SOLR_HOST:-WHAT}
SOLR_PORT=${SOLR_PORT:-WHAT}

result=`curl -s ${SOLR_HOST}:${SOLR_PORT}/solr/RawRecordRepositoryIndex/admin/ping`
cc=$?
echo ${cc}
if [ ${cc} -eq 0 ]
then
		${SOLR_PATH}/bin/solr stop -p ${SOLR_PORT}
fi

${SOLR_PATH}/bin/solr start -m 2g -h devel8.dbc.dk -p ${SOLR_PORT} -Dsolr.autoSoftCommit.maxTime=100
cc=$?
echo ${cc}
if [ ${cc} -ne 0 ]
then
		echo "could not start solr server at devel8.dbc.dk port ${SOLR_PORT}"
		exit 1
fi
