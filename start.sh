#!/bin/bash

cd /srv/dse-netdata-agent/
frequency=1
if [ -n "$1"   ]; then
    frequency=$1
fi
java -jar target/jmxfetch-0.10.0-jar-with-dependencies.jar -c mvp.yaml -p $(($frequency*1000)) -r console -D . -l log -L DEBUG collect
