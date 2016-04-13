#!/bin/sh
cd /srv/dse-netdata-agent/
MYSELF=`which "$0" 2>/dev/null`
[ $? -gt 0 -a -f "$0" ] && MYSELF="./$0"
java=java
if test -n "$JAVA_HOME"; then
    java="$JAVA_HOME/bin/java"
fi
frequency=1
if [ -n "$@"  ]; then
    frequency=$@
fi
pwd
ls mvp.yaml
#exec "$java" -XX:+UseG1GC -Xmx8G -Xms8G -XX:+UseTLAB -XX:+ResizeTLAB -XX:+AlwaysPreTouch $java_args -jar $MYSELF "$@"
exec "$java" -jar $MYSELF -c mvp.yaml -p $(($frequency*1000)) -r console -D . -l /srv/dse-netdata-agent/log -L DEBUG collect
exit 1 
