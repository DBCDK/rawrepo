#!/bin/bash
set -e  

echo Environment
echo `env`

/usr/local/bin/confd -onetime -backend env -log-level debug
