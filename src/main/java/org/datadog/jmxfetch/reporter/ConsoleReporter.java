package org.datadog.jmxfetch.reporter;

import com.google.common.base.Joiner;
import org.datadog.jmxfetch.Instance;
import org.datadog.jmxfetch.JMXAttribute;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;

public class ConsoleReporter extends Reporter {

    private LinkedList<HashMap<String, Object>> metrics = new LinkedList<HashMap<String, Object>>();
    private LinkedList<HashMap<String, Object>> serviceChecks = new LinkedList<HashMap<String, Object>>();

    @Override
    protected void sendMetricPoint(String metricName, double value, String[] tags) {
        String tagString = "[" + Joiner.on(",").join(tags) + "]";
        //System.out.println(tagString);

        String prettyMetricName= metricName.replace("_","");

        String[] nameArray = Arrays.stream(tags)
                .filter(item -> item.startsWith("name:"))
                .toArray(index -> new String[index]);
        String[] indexArray= Arrays.stream(tags)
                .filter(item -> item.startsWith("index:"))
                .toArray(index -> new String[index]);

        String name = "";
        if (nameArray.length >0){
            name = nameArray[0].split(":")[1];
        }
        String myIndex= "";
        if (indexArray.length >0){
            myIndex = indexArray[0].split(":")[1];
        }

        if (name != "") {
            name = "." + name.toLowerCase();
        }
        if (myIndex!= "") {
            myIndex = "." + myIndex.toLowerCase();
        }


        /*
        if (prettyMetricName.split("\\.").length ==2){
            prettyMetricName = prettyMetricName + ".value";
        }
        */

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

        prettyMetricName = jmxDomain + "." +String.join(".", Arrays.copyOfRange(nameArray, j, nameArray.length));
        */

        String jmxDomain = String.join(".",jmxDomainArray).split(":")[1];

        prettyMetricName = prettyMetricName.replace(jmxDomain+".", "");


        //System.out.println(metricName + tagString + " - " + System.currentTimeMillis() / 1000 + " = " + value);
        //System.out.println("SET " + prettyMetricName +  " = " + String.format("%10f", (float)value));
        //System.out.println("Pretty metric name: "+prettyMetricName);

        System.out.println("SET " + jmxDomain  + myIndex + name +  "." + prettyMetricName +  " = " + String.format("%10f", (float)value));

        HashMap<String, Object> m = new HashMap<String, Object>();
        m.put("name", metricName);
        m.put("value", value);
        m.put("tags", tags);
        metrics.add(m);
    }

    public LinkedList<HashMap<String, Object>> getMetrics() {
        LinkedList<HashMap<String, Object>> returnedMetrics = new LinkedList<HashMap<String, Object>>();
        for (HashMap<String, Object> map : metrics) {
            returnedMetrics.add(new HashMap<String, Object>(map));
        }
        metrics.clear();
        return returnedMetrics;
    }

    public void doSendServiceCheck(String checkName, String status, String message, String[] tags) {
        String tagString = "";
        if (tags != null && tags.length > 0) {
            tagString = "[" + Joiner.on(",").join(tags) + "]";
        }
        //System.out.println(checkName + tagString + " - " + System.currentTimeMillis() / 1000 + " = " + status);

        HashMap<String, Object> sc = new HashMap<String, Object>();
        sc.put("name", checkName);
        sc.put("status", status);
        sc.put("message", message);
        sc.put("tags", tags);
        serviceChecks.add(sc);
    }

    public LinkedList<HashMap<String, Object>> getServiceChecks() {
        LinkedList<HashMap<String, Object>> returnedServiceChecks = new LinkedList<HashMap<String, Object>>();
        for (HashMap<String, Object> map : serviceChecks) {
            returnedServiceChecks.add(new HashMap<String, Object>(map));
        }
        serviceChecks.clear();
        return returnedServiceChecks;
    }

    @Override
    public void displayMetricReached() {
        System.out.println("\n\n\n       ------- METRIC LIMIT REACHED: ATTRIBUTES BELOW WON'T BE COLLECTED -------\n\n\n");
    }

    @Override
    public void displayMatchingAttributeName(JMXAttribute jmxAttribute, int rank, int limit) {
        System.out.println("       Matching: " + rank + "/" + limit + ". " + jmxAttribute);
    }

    @Override
    public void displayNonMatchingAttributeName(JMXAttribute jmxAttribute) {
        System.out.println("       Not Matching: " + jmxAttribute);
    }

    @Override
    public void displayInstanceName(Instance instance) {
        System.out.println("\n#####################################");
        System.out.println("Instance: " + instance);
        System.out.println("#####################################\n");
    }

}
