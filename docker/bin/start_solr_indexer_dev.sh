#!/bin/bash
set -x
# If this script is run by intellij, the docker must be detached since the run window isn't a tty. Therefore the default is -d.
# Console output can be seen with docker logs -f <container_ID>.
# If no version is specified, a new image will be build tagged as ${USER}
USER=${USER:-WHAT}    # silencing annoying intellij syntax quibble
SOLR_HOST=${SOLR_HOST:-WHAT}
SOLR_PORT=${SOLR_PORT:-WHAT}

package=solr-indexer
cid_file=solr-indexer.cid
docker_image=docker-os.dbc.dk/rawrepo-solr-indexer-1.10-snapshot
version=${USER}
port=`id -u ${USER}`3
detached="-d"
while getopts "p:v:u" opt; do
    case "$opt" in
    "u" )
            detached=""
            ;;
    "p" )
            port=$OPTARG
            ;;
    "v" )
            version=$OPTARG
            ;;
    esac
done

if [ ! -d ${HOME}/.ocb-tools ]
then
    mkdir ${HOME}/.ocb-tools
fi

if [ "$version" = "${USER}" ]
then
	hop=`pwd`
    cd ../../${package}
    rm -rf target
	mvn verify install > /tmp/mvn.out.${USER}.${package}
	cd ${hop}
    cd ../${package}
    rm -rf target
	mvn verify install > /tmp/mvn.out.${USER}.docker-${package}
    cd target/docker
    docker build -t ${docker_image}:${version} .
    cc=$?
    if [ ${cc} -ne 0 ]
    then
        echo "Couldn't build image"
        exit 1
    fi
fi

if [ -f ${HOME}/.ocb-tools/${cid_file} ]
then
    docker stop `cat ${HOME}/.ocb-tools/${cid_file}`
fi

if [ -f ${HOME}/.ocb-tools/SOLR_HOST ]
then
    SOLR_HOST=`cat ${HOME}/.ocb-tools/SOLR_HOST`
fi
if [ -f ${HOME}/.ocb-tools/SOLR_PORT ]
then
    SOLR_PORT=`cat ${HOME}/.ocb-tools/SOLR_PORT`
fi

rr_conn=`egrep rawrepo.jdbc.conn.url ${HOME}/.ocb-tools/testrun.properties | tr -d " " | cut -d"/" -f3-`
rr_user=`egrep rawrepo.jdbc.conn.user ${HOME}/.ocb-tools/testrun.properties | tr -d " " | cut -d"=" -f2`
rr_pass=`egrep rawrepo.jdbc.conn.passwd ${HOME}/.ocb-tools/testrun.properties | tr -d " " | cut -d"=" -f2`

echo "Starting container"
container_id=`docker run -it ${detached} -p ${port}:8080 \
		-e RAWREPO_URL="${rr_user}:${rr_pass}@${rr_conn}" \
		-e MAX_CONCURRENT=1 \
		-e TIMEOUT=10 \
		-e SOLR_URL=http://${SOLR_HOST}:${SOLR_PORT}/solr/rawrepo\
		-e OPENAGENCY_URL=http://openagency.addi.dk/2.33/ \
		-e WORKER_NAME=socl-sync\
		${docker_image}:${version}`
cc=$?
if [ ${cc} -ne 0 ]
then
    echo "Couldn't start"
    exit 1
else
    echo ${container_id} > ${HOME}/.ocb-tools/${cid_file}
    echo "PORT: ${port}"
    echo "CID : ${container_id}"
    imageName=`docker inspect --format='{{(index .Name)}}' ${container_id} | cut -d"/" -f2`
    echo "NAME: ${imageName}"
fi

