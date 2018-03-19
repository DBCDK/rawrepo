FROM docker.dbc.dk/dbc-payara
MAINTAINER iscrum

ENV KAFKA_LOG_APPNAME rawrepo-content-service



ADD pre/* $PAYARA_CFG/pre/
ADD post/* $PAYARA_CFG/post/
ADD res/rawrepo-content-service.war $PAYARA_CFG/pre/
RUN payara-setup $PAYARA_CFG/pre/

LABEL RAWREPO_URL="Raw repo jdbc url. (required). Ex.: 'user:pass@host:1234/dbname'" \
      X_FORWARDED_FOR=" default is '' (optional)" \
      FORSRIGHTS_URL="Url to forsrights (required)" \
      FORSRIGHTS_CONNECT_TIMEOUT="Connect timeout in ms. Default is '2500' (optional)" \
      FORSRIGHTS_REQUEST_TIMEOUT="Request timeout in ms. Default is '10000' (optional)" \
      FORSRIGHTS_DISABLED="Specify whether authentication is disabled. Default is 'false' (optional)" \
      FORSRIGHTS_NAME="Default is '*' (optional)" \
      FORSRIGHTS_RIGHT="Default is '*' (optional)" \
      OPENAGENCY_URL="Url to openagency (required)" \
      OPENAGENCY_CONNECT_TIMEOUT="Default is '2500' (optional)" \
      OPENAGENCY_REQUEST_TIMEOUT="Default is '10000' (optional)"
