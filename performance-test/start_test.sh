#!/usr/bin/env bash

RAWREPO_SQL="rawrepo.sql"
if [ ${RR_PERFORMANCE_TEST_BOOTSTRAP_VERSION} ]; then
    RAWREPO_SQL="rawrepo${BOOTSTRAP_VERSION}.sql"
fi


cp ../access/schema/${RAWREPO_SQL} rawrepo.sql
cp ../access/schema/queuerules.sql .

docker stop rr_speedtest || true
docker rm rr_speedtest || true
docker run -d --name rr_speedtest -v $(pwd):/scripts docker.dbc.dk/dbc-postgres:9.5
docker exec -u 0 rr_speedtest bash -c 'apt-get update && apt-get -qy install python'
echo "=> Sleeping 10"

sleep 10s
docker logs rr_speedtest
docker exec rr_speedtest /scripts/prepare_db.sh



echo "=> Do speedtests here."
sleep 30s
echo "=> Done speedtests."
rm queuerules.sql rawrepo.sql
docker stop rr_speedtest
docker rm rr_speedtest