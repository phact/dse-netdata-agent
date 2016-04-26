package org.datadog.jmxfetch.reporter;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.datadog.jmxfetch.App;
import org.datadog.jmxfetch.Instance;
import org.datadog.jmxfetch.JMXAttribute;

import java.util.*;


public abstract class Reporter {

    private final static Logger LOGGER = Logger.getLogger(App.class.getName());
    public static final String VALUE = "value";

    private HashMap<String, Integer> serviceCheckCount;
    private HashMap<String, HashMap<String, HashMap<String, Object>>> ratesAggregator = new HashMap<String, HashMap<String, HashMap<String, Object>>>();

    public Reporter() {
        this.serviceCheckCount = new HashMap<String, Integer>();
    }

    String generateId(HashMap<String, Object> metric) {
        String key = (String) metric.get("alias");
        for (String tag : (String[]) metric.get("tags")) {
            key += tag;
        }
        return key;
    }

    public void clearRatesAggregator(String instanceName) {
        ratesAggregator.put(instanceName, new HashMap<String, HashMap<String, Object>>());
    }

    public void sendMetrics(LinkedList<HashMap<String, Object>> metrics, String instanceName) {
        HashMap<String, HashMap<String, Object>> instanceRatesAggregator;
        if (ratesAggregator.containsKey(instanceName)) {
            instanceRatesAggregator = ratesAggregator.get(instanceName);
        } else {
            instanceRatesAggregator = new HashMap<String, HashMap<String, Object>>();
        }

        int loopCounter = App.getLoopCounter();

        String sendingMessage = "Instance " + instanceName + " is sending " + metrics.size()
                + " metrics to the metrics reporter during collection #" + loopCounter;
        if (loopCounter <= 5 || loopCounter % 10 == 0) {
            LOGGER.info(sendingMessage);
            if (loopCounter == 5) {
                LOGGER.info("Next collections will be logged only every 10 collections.");
            }
        } else {
            LOGGER.debug(sendingMessage);
        }

        for (HashMap<String, Object> m : metrics) {
            // We need to edit metrics for legacy reasons (rename metrics, etc)
            HashMap<String, Object> metric = new HashMap<String, Object>(m);

            Double currentValue = (Double) metric.get(VALUE);
            if (currentValue.isNaN() || currentValue.isInfinite()) {
                continue;
            }

            String metricComplexity = (String) metric.get("complexity");
            String metricName = (String) metric.get("alias");
            String metricType = (String) metric.get("metric_type");
            String[] tags = Arrays.asList((String[]) metric.get("tags")).toArray(new String[0]);

            //System.out.println(Arrays.toString((String[]) metric.get("tags")));

            String[] nameArray = Arrays.stream(tags)
                    .filter(item -> item.startsWith("name:"))
                    .toArray(index -> new String[index]);

            String[] typeArray = Arrays.stream(tags)
                    .filter(item -> item.startsWith("type:"))
                    .toArray(index -> new String[index]);

            String[] jmxDomainArray= Arrays.stream(tags)
                    .filter(item -> item.startsWith("jmx_domain:"))
                    .toArray(index -> new String[index]);

            /*
            String jmxDomain = "";
            if (jmxDomainArray.length >0){
                jmxDomain= jmxDomainArray[0].split(":")[1];
                String[] domainArray = jmxDomain.split("\\.") ;
                jmxDomain= String.join(".",Arrays.copyOfRange(domainArray,2,domainArray.length));
            }
            */

            String name = "";
            if (nameArray.length >0){
                name = nameArray[0].split(":")[1];
            }

            String type = "";
            if (typeArray.length >0){
                type = typeArray[0].split(":")[1];
            }

            final String finalType= type;

            String[] typeTypeArray = Arrays.stream(tags)
                    .filter(item -> item.startsWith(finalType))
                    .toArray(index -> new String[index]);

            String typeType = "";
            if (type!="" && typeTypeArray.length > 0){
                typeType = typeTypeArray[0].split(":")[1];
            }

            if (name != "") {
                name = "." + name.toLowerCase();
            }
            if (type != "") {
                type = "." + type.toLowerCase();
            }
            if (typeType != "") {
                typeType = "." + typeType.toLowerCase();
            }

            String prettyMetricName= metricName.replace("_","");



            if (metricComplexity == "complex"){
                int lastIndex= prettyMetricName.lastIndexOf(".");
                prettyMetricName = prettyMetricName.substring(0,lastIndex);
            }
            /*
            if (prettyMetricName.split("\\.").length ==2){
                prettyMetricName = prettyMetricName + ".value";
            }

            String[] domainArray = jmxDomain.split("\\.") ;
            String[] nameArray = prettyMetricName.split("\\.") ;
            int j=0;
            for (int i = 0 ; i < nameArray.length ; i++){
               if (domainArray.length > i && domainArray[i].equals(nameArray[i])){
                   j++;
               }
            }

            prettyMetricName = jmxDomain + "." +String.join(".",Arrays.copyOfRange(nameArray, j, nameArray.length));
            */
            String jmxDomain = String.join(".",jmxDomainArray).split(":")[1];

            prettyMetricName = prettyMetricName.replace(jmxDomain+".", "");

            //System.out.println("Pretty metric name: "+  prettyMetricName);

            System.out.println("BEGIN " + jmxDomain + type + typeType +  "-" + jmxDomain + name + "." + prettyMetricName );

            // StatsD doesn't support rate metrics so we need to have our own aggregator to compute rates
            if (!"gauge".equals(metricType)) {
                String key = generateId(metric);
                if (!instanceRatesAggregator.containsKey(key)) {
                    HashMap<String, Object> rateInfo = new HashMap<String, Object>();
                    rateInfo.put("ts", System.currentTimeMillis());
                    rateInfo.put(VALUE, currentValue);
                    instanceRatesAggregator.put(key, rateInfo);
                    continue;
                }

                long oldTs = (Long) instanceRatesAggregator.get(key).get("ts");
                double oldValue = (Double) instanceRatesAggregator.get(key).get(VALUE);

                long now = System.currentTimeMillis();
                double rate = 1000 * (currentValue - oldValue) / (now - oldTs);

                if (!Double.isNaN(rate) && !Double.isInfinite(rate)) {
                    sendMetricPoint(metricName, rate, tags);
                }

                instanceRatesAggregator.get(key).put("ts", now);
                instanceRatesAggregator.get(key).put(VALUE, currentValue);
            } else { // The metric is a gauge
                sendMetricPoint(metricName, currentValue, tags);
            }

            System.out.println("END");
        }

        ratesAggregator.put(instanceName, instanceRatesAggregator);
    }

    public void sendServiceCheck(String checkName, String status, String message, String[] tags){
        this.incrementServiceCheckCount(checkName);
        String dataName = Reporter.formatServiceCheckPrefix(checkName);

        this.doSendServiceCheck(dataName, status, message, tags);
    }

    public void incrementServiceCheckCount(String checkName){
        int scCount = this.getServiceCheckCount(checkName);
        this.getServiceCheckCountMap().put(checkName, new Integer(scCount+1));
    }

    public int getServiceCheckCount(String checkName){
        Integer scCount = this.serviceCheckCount.get(checkName);
        return (scCount == null) ? 0 : scCount.intValue();
    }

    public void resetServiceCheckCount(String checkName){
        this.serviceCheckCount.put(checkName, new Integer(0));
    }

    protected HashMap<String, Integer> getServiceCheckCountMap(){
        return this.serviceCheckCount;
    }

    public static String formatServiceCheckPrefix(String fullname){
        String[] chunks = fullname.split("\\.");
        chunks[0] = chunks[0].replaceAll("[A-Z0-9:_\\-]", "");
        return StringUtils.join(chunks, ".");
    }

    protected abstract void sendMetricPoint(String metricName, double value, String[] tags);

    protected abstract void doSendServiceCheck(String checkName, String status, String message, String[] tags);

    public abstract void displayMetricReached();

    public abstract void displayNonMatchingAttributeName(JMXAttribute jmxAttribute);

    public abstract void displayInstanceName(Instance instance);

    public abstract void displayMatchingAttributeName(JMXAttribute jmxAttribute, int rank, int limit);

}
