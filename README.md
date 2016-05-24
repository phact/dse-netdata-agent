# dse-netdata-agent

based off of https://github.com/DataDog/jmxfetch this program generates dse
metrics in a netdata compatible format

###pull repo:

    mkdir /srv/
    cd /srv/
    git clone https://github.com/phact/dse-netdata-agent


###to build:

    mvn clean compile assembly:single

###to run:

    ./start.sh <seconds>


## Installing and getting this running with netdata

###Install netdata:
dependencies

    sudo apt-get install zlib1g-dev gcc make git autoconf autogen automake pkg-config uuid-dev

download it - the directory 'netdata.git' will be created 

    git clone https://github.com/firehol/netdata.git --depth=1 
    cd netdata 

build it 

    sudo ./netdata-installer.sh

Run it

    killall netdata
    /usr/sbin/netdata

check the log at 

    tail -f /var/log/netdata/error.log

Add the dse plugin

    cp /srv/dse-netdata-agent/start-dse.sh /usr/libexec/netdata/plugins.d/dse.plugin
    chmod +x /usr/libexec/netdata/plugins.d/dse.plugin

Restart netdata to pick up the new plugin
```
sudo pkill -f dse.plugin
sudo pkill -f cassandra.plugin
sudo pkill -f jmxfetch

sudo killall -w netdata

/usr/sbin/netdata
```

Host the sample dashboards

    cd /srv/dse-netdata-plugin/www
    python -m SimpleHTTPServer

Hit the dashbaords at http://localhost:8000/
