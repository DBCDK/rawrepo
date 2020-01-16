FROM docker.dbc.dk/dbc-postgres:9.5

ENV POSTGRES_DB=rawrepo
ENV POSTGRES_USER=rawrepo
ENV POSTGRES_PASSWORD=rawrepo

USER root
ADD schema/rawrepo.sql /docker-entrypoint.d/10-rawrepo.sql
ADD schema/queuerules.sql /docker-entrypoint.d/11-queuerules.sql
RUN chown -R $POSTGRES_LINUX_USER:$POSTGRES_LINUX_USER /docker-entrypoint.d/
USER $POSTGRES_LINUX_USER

# Enable XA transactions
RUN echo "max_prepared_transactions=40" >> /etc/postgresql/9.5/main/postgresql.conf

LABEL POSTGRES_DB="Name of db (optional). Default is 'rawrepo'" \
      POSTGRES_USER="(optional). Default is 'rawrepo'" \
      POSTGRES_PASSWORD="(optional). Default is 'rawrepo'"
LABEL MAINTAINER="metascrum"