FROM docker-dbc.artifacts.dbccloud.dk/dbc-postgres-14

ENV POSTGRES_DB=rawrepo
ENV POSTGRES_USER=rawrepo
ENV POSTGRES_PASSWORD=rawrepo

USER root
ADD schema/rawrepo.sql /docker-entrypoint.d/10-rawrepo.sql
ADD schema/queuerules.sql /docker-entrypoint.d/11-queuerules.sql
ADD schema/configurations.sql /docker-entrypoint.d/11-configurations.sql
RUN chown -R $POSTGRES_LINUX_USER:$POSTGRES_LINUX_USER /docker-entrypoint.d/
USER $POSTGRES_LINUX_USER

# Increate max_connections to 200
RUN sed -i -e '/max_connections =/ s/= .*/= 200/' /etc/postgresql/14/main/postgresql.conf
# Enable XA transactions
RUN echo "max_prepared_transactions=100" >> /etc/postgresql/14/main/postgresql.conf

LABEL POSTGRES_DB="Name of db (optional). Default is 'rawrepo'" \
      POSTGRES_USER="(optional). Default is 'rawrepo'" \
      POSTGRES_PASSWORD="(optional). Default is 'rawrepo'"
LABEL MAINTAINER="metascrum"
