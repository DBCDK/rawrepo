FROM docker-io.dbc.dk/dbc-solr6-1.0-snapshot:latest
MAINTAINER iscrum

ENV RAWREPO_HOME $SOLR_BASE/server/solr/rawrepo

ADD confd /etc/confd
ADD docker-entrypoint-confd.d/* /docker-entrypoint-confd.d/
ADD res/rawrepo-solr-indexer-solr-config-zip/ $RAWREPO_HOME/conf/

USER root
RUN chmod +x /docker-entrypoint-confd.d/*.sh
RUN chown -R $SOLR_USER:$SOLR_USER $RAWREPO_HOME
USER $SOLR_USER

LABEL DBC_AUTOCOMMIT_MAXTIME="Default is to use solrconfig file default - NB doesnt have any effect pt"
LABEL DBC_AUTOCOMMIT_MAXDOCS="Default is to use solrconfig file default - NB doesnt have any effect pt"
LABEL DBC_AUTOCOMMIT_OPENSEARCHER="Default is to use solrconfig file default - NB doesnt have any effect pt"
LABEL SOLR_AUTOSOFTCOMMIT_MAXTIME="Default is to use solrconfig file default"
LABEL DBC_AUTOSOFTCOMMIT_MAXDOCS="Default is to use solrconfig file default - NB doesnt have any effect pt, probably never will"
