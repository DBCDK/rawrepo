#!/bin/bash
#set -x
# If this script is run by intellij, the docker must be detached since the run window isn't a tty. Therefore the default is -d.
# Console output can be seen with docker logs -f <container_ID>.
# If no version is specified, a new image will be build tagged as ${USER}
USER=${USER:-WHAT}    # silencing annoying intellij syntax quibble

package=content-service
cid_file=content-service.cid
docker_image=docker-io.dbc.dk/rawrepo-content-service-1.15-snapshot
version=${USER}
port=`id -u ${USER}`2
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
    docker build -t ${docker_image}:${USER} content-service/.
fi

if [ -f ${HOME}/.ocb-tools/${cid_file} ]
then
    docker stop `cat ${HOME}/.ocb-tools/${cid_file}`
fi

RAWREPO_PORT=`egrep rawrepo.jdbc.conn.url ${HOME}/.ocb-tools/testrun.properties | tr -d " " | cut -d "=" -f2 | cut -d "/" -f3 | cut -d ":" -f2`
echo "Starting container"

container_id=`docker run -it ${detached} -p ${port}:8080 \
		-e RAWREPO_DB_URL="rawrepo:thePassword@localhost:${RAWREPO_PORT}/rawrepo" \
		-e VIPCORE_ENDPOINT="http://vipcore.iscrum-vip-extern-test.svc.cloud.dbc.dk" \
		-e FORSRIGHTS_DISABLED="true" \
		-e INSTANCE_NAME="dev" \
		-e JAVA_MAX_HEAP_SIZE="2G" \
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

