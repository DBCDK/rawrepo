#!/usr/bin/env bash


psql --db db_database < /scripts/rawrepo.sql
psql --db db_database < /scripts/queuerules.sql
printf "$(python /scripts/populate_db.py)" | psql --db db_database
