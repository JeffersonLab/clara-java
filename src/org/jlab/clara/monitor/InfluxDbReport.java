package org.jlab.clara.monitor;

import org.jlab.coda.xmsg.excp.xMsgException;
import org.jlab.coda.xmsg.net.xMsgProxyAddress;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Created by gurjyan on 4/25/17.
 */
public class InfluxDbReport extends DpeListenerAndReporter {

    int i;

    InfluxDbReport(String name, String proxyHost, int proxyPort){
        super(name,new xMsgProxyAddress(proxyHost, proxyPort));
    }

    @Override
    public void report(String jsonString) {
        JSONObject base = new JSONObject(jsonString);

        // registration information
        JSONObject registration = base.getJSONObject("DPERegistration");
        JSONArray reg_containers = registration.getJSONArray("containers");
        JSONObject reg_container = reg_containers.getJSONObject(0);
        JSONArray cont_services = reg_container.getJSONArray("services");
        for(int i = 0; i<cont_services.length();i++){
            JSONObject service = cont_services.getJSONObject(i);
            System.out.println("start_time = "+service.getString("start_time"));
            System.out.println("author = "+service.getString("author"));
            System.out.println("pool_size = "+service.getInt("pool_size"));
            System.out.println("description = "+service.getString("description"));
            System.out.println("engine_name = "+service.getString("engine_name"));
            System.out.println("language = "+service.getString("language"));
            System.out.println("class_name = "+service.getString("class_name"));
            System.out.println("version = "+service.getString("version"));
        }
        
        // runtime information
        JSONObject runtime = base.getJSONObject("DPERuntime");
        JSONArray rt_containers = runtime.getJSONArray("containers");
        JSONObject rt_container = rt_containers.getJSONObject(0);
        JSONArray rt_services = rt_container.getJSONArray("services");
        for(int i = 0; i<rt_services.length();i++){
            JSONObject service = rt_services.getJSONObject(i);
            System.out.println("snapshot_time = "+service.getString("snapshot_time"));
            System.out.println("n_requests = "+service.getInt("n_requests"));
            System.out.println("n_failures = "+service.getInt("n_failures"));
            System.out.println("shm_reads = "+service.getInt("shm_reads"));
            System.out.println("name = "+service.getString("name"));
            System.out.println("shm_writes = "+service.getInt("shm_writes"));
            System.out.println("exec_time = "+service.getLong("exec_time"));
            System.out.println("bytes_recv = "+service.getInt("bytes_recv"));
            System.out.println("bytes_sent = "+service.getInt("bytes_sent"));
        }

//        System.out.println(base.toString(4));
    }

    public static void main(String[] args) {
        System.out.println(args[0]);
        System.out.println(args[1]);
        try (InfluxDbReport rep = new InfluxDbReport(args[0], args[1], Integer.parseInt(args[2]))) {
            rep.start();
        } catch (xMsgException e) {
            e.printStackTrace();
        }
    }
}
