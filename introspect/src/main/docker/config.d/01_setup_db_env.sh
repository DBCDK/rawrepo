#!/usr/bin/env bash

# Function to get local local scope for name
add_jdbc_resource_from_url "jdbc/rr" ${RAWREPO_URL0##* }

export INSTANCE_NAME=${RAWREPO_URL0%% *}