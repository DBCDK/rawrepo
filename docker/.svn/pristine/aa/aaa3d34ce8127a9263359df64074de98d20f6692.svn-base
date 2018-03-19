FROM docker.dbc.dk/dbc-payara
MAINTAINER iscrum

ENV KAFKA_LOG_APPNAME rawrepo-solr-indexer

ADD pre/* $PAYARA_CFG/pre/
ADD post/* $PAYARA_CFG/post/
ADD res/rawrepo-solr-indexer.war $PAYARA_CFG/pre/
RUN payara-setup $PAYARA_CFG/pre/

LABEL RAWREPO_URL="Raw repo jdbc url. (required). Ex.: 'user:pass@host:1234/dbname'" \
      OPENAGENCY_URL="URL to openagency (defaults to 'http://openagency.addi.dk/2.33/')" \
      SOLR_URL="(required)" \
      MAX_CONCURRENT="Default is 2 (optional)" \
      WORKER_NAME="Default is solr-sync (optional)" \
      TIMEOUT="Default is 5 (optional)"
