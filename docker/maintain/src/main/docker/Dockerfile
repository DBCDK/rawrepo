FROM docker.dbc.dk/dbc-payara
MAINTAINER iscrum

ENV KAFKA_LOG_APPNAME rawrepo-maintain

ADD pre/* $PAYARA_CFG/pre/
ADD post/* $PAYARA_CFG/post/
ADD res/rawrepo-maintain.war $PAYARA_CFG/pre/
RUN payara-setup $PAYARA_CFG/pre/

LABEL NAME="The name/alias to be a header on the webpage (required)" \
      RAWREPO_URL="Raw repo jdbc url. (required). Ex.: 'user:pass@host:1234/dbname'" \
      OPENAGENCY_URL="Url to openagency (required)" \
