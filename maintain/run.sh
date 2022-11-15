docker build -f Dockerfile --pull --no-cache . -t docker-metascrum.artifacts.dbccloud.dk/rawrepo-maintain:dev
docker run --name rawrepo-maintain --rm -e RAWREPO_DB_URL="$RAWREPO_DB_URL" -e VIPCORE_ENDPOINT="$VIPCORE_ENDPOINT" -e JAVA_MAX_HEAP_SIZE=2G -e LOG_FORMAT=text -p 8080:8080 docker-metascrum.artifacts.dbccloud.dk/rawrepo-maintain:dev
