#!/bin/bash
#set -x
# If this script is run by intellij, the docker must be detached since the run window isn't a tty. Therefore the default is -d.
# Console output can be seen with docker logs -f <container_ID>.
# If no version is specified, a new image will be build tagged as ${USER}
USER=${USER:-WHAT}    # silencing annoying intellij syntax quibble

package=hydra
cid_file=hydra.cid
docker_image=docker-i.dbc.dk/rawrepo-hydra-1.10-snapshot
version=${USER}
port=`id -u ${USER}`9
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
    echo "Building ${package}"
	mvn clean package > /tmp/mvn.out.${USER}.${package}
	echo "Done building"
    cd src/main/docker/
    rm *.war
    cp ../../../target/*war .
    docker build -t ${docker_image}:${USER} .
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

rr_conn=`egrep rawrepo.jdbc.conn.url ${HOME}/.ocb-tools/testrun.properties | tr -d " " | cut -d"/" -f3-`
rr_user=`egrep rawrepo.jdbc.conn.user ${HOME}/.ocb-tools/testrun.properties | tr -d " " | cut -d"=" -f2`
rr_pass=`egrep rawrepo.jdbc.conn.passwd ${HOME}/.ocb-tools/testrun.properties | tr -d " " | cut -d"=" -f2`
hi_conn=`egrep holdings.jdbc.conn.url ${HOME}/.ocb-tools/testrun.properties | tr -d " " | cut -d"/" -f3-`
hi_user=`egrep holdings.jdbc.conn.user ${HOME}/.ocb-tools/testrun.properties | tr -d " " | cut -d"=" -f2`
hi_pass=`egrep holdings.jdbc.conn.passwd ${HOME}/.ocb-tools/testrun.properties | tr -d " " | cut -d"=" -f2`
echo "Starting container"
container_id=`docker run -it ${detached} -p ${port}:8080 \
        -e RAWREPO_URL="${rr_user} ${rr_user}:${rr_pass}@${rr_conn}" \
        -e HOLDINGS_ITEMS_URL="${hi_user} ${hi_user}:${hi_pass}@${hi_conn}" \
		-e OPENAGENCY_URL="http://openagency.addi.dk/test_2.34/" \
		-e INSTANCE_NAME="${USER}_dev_basismig" \
		-e ADD_JVM_ARGS="-Xms2g" \
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

