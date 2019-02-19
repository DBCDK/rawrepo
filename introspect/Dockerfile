FROM docker.dbc.dk/payara-micro:latest

ADD docker/config.d/* config.d
ADD target/rawrepo-introspect-*-SNAPSHOT.war wars/

LABEL OPENAGENCY_URL="Url to openagency (required)"
LABEL MAINTAINER="metascrum"