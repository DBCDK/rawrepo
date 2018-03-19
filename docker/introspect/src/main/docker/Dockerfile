FROM docker.dbc.dk/dbc-payara
MAINTAINER iscrum

ENV KAFKA_LOG_APPNAME rawrepo-introspect

ADD rawrepo-url.sh /docker-entrypoint-confd.d/

ADD pre/* $PAYARA_CFG/pre/
ADD post/* $PAYARA_CFG/post/
ADD res/rawrepo-introspect.war $PAYARA_CFG/pre/
RUN payara-setup $PAYARA_CFG/pre/

LABEL RAWREPO_URL[0..i]="Raw repo jdbc url. (At least RAWREPO_URL0 required). Ex.: 'rawrepo-staging user:pass@host:1234/dbname'"
