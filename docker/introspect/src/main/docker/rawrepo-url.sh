#!/bin/bash

if [ -z ${RAWREPO_URL0+x} ]; then
    echo "required env variable \$RAWREPO_URL0 not set"
    exit 1
fi

stringarray=($RAWREPO_URL0)
name=${stringarray[0]}
url=${stringarray[1]}

file="$PAYARA_CFG/post/450-rawrepo-url.jdbc"
echo "[jdbc/rr/pool]" >"$file"
echo "postgres" >>"$file"
echo "maxPoolSize = \${MAX_POOL_SIZE|4}" >>"$file"
echo "poolResizeQuantity = \${POOL_RESIZE_QUANTITY|1}" >>"$file"
echo "steadyPoolSize = \${STEADY_POOL_SIZE|1}" >>"$file"
echo "[properties]" >>"$file"
echo "$url" >>"$file"
echo "[jdbc/rr]" >>"$file"

