java -jar target/jmxfetch-0.10.0-jar-with-dependencies.jar -c mvp.yaml -p $(($1*1000)) -r console -D . -l log -L DEBUG collect
