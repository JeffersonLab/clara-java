package org.jlab.clara.util.report;

import org.jlab.coda.jinflux.JinFlux;
import org.jlab.coda.jinflux.JinFluxException;
import org.jlab.coda.jinflux.JinTime;

import java.util.HashMap;
import java.util.Map;

/**
 * Class description here....
 * <p>
 *
 * @author gurjyan
 *         Date 11/18/16
 * @version 4.x
 */
public class JinfluxReport extends JinFlux implements ExternalReport {

        private String dbName;
        private boolean jinFxConnected = true;

        public JinfluxReport(String dbNode, String dbName, String user, String password) throws JinFluxException {
            super(dbNode, user, password);
            this.dbName = dbName;
            try {
                if (!existsDB(dbName)) {
                    createDB(dbName, 1, JinTime.HOURE);
                }
            } catch (Exception e) {
                jinFxConnected = false;
            }

        }

        public JinfluxReport(String dbNode, String dbName, String expid) throws JinFluxException {
            super(dbNode);
            this.dbName = dbName;

            try {
                if (!existsDB(dbName)) {
                    createDB(dbName, 1, JinTime.HOURE);
                }

            } catch (Exception e) {
                jinFxConnected = false;
            }
        }


        public void push(DpeReport dpeData) {
            try {

                if (jinFxConnected) {
//                    if (!component.getExpid().equals(AConstants.udf) &&
//                            !component.getName().equals(AConstants.udf)) {
//
//                        Map<String, String> tags = new HashMap<>();
//                        tags.put(AConstants.RUNTYPE, component.getRunType());
//                        tags.put(AConstants.SESSION, component.getSession());
//
//                        String c_name = component.getName();
//                        if(component.getName().contains("sms")) c_name = "sms";
//                        tags.put(AConstants.CODANAME, c_name);
//
//                        Point.Builder p = openTB(component.getExpid(), tags);
//                        addDP(p, AConstants.TYPE, component.getType());
//                        int state = 0;
//                        switch (component.getState()) {
//                            case AConstants.disconnected:
//                                state = 1;
//                                break;
//                            case AConstants.connected:
//                                state = 2;
//                                break;
//                            case AConstants.booted:
//                                state = 3;
//                                break;
//                            case AConstants.configured:
//                                state = 4;
//                                break;
//                            case AConstants.downloaded:
//                                state = 5;
//                                break;
//                            case AConstants.prestarted:
//                                state = 6;
//                                break;
//                            case AConstants.active:
//                                state = 7;
//                                break;
//                            case AConstants.ended:
//                                state = 8;
//                                break;
//                        }
//                        addDP(p, AConstants.STATE, state);
//                        addDP(p, AConstants.EVENTRATE, component.getEventRate());
//                        addDP(p, AConstants.EVENTRATEAVERAGE, component.getEventRateAverage());
//                        addDP(p, AConstants.DATARATE, component.getDataRate());
//                        addDP(p, AConstants.DATARATEAVERAGE, component.getDataRateAverage());
//                        addDP(p, AConstants.NUMBEROFLONGS, component.getNumberOfLongs());
//                        addDP(p, AConstants.EVENTNUMBER, component.getEventNumber());
//                        addDP(p, AConstants.LIVETIME, component.getLiveTime());
//                        addDP(p, AConstants.RUNNUMBER, component.getRunNumber());
//                        // add input buffers
//                        Map<String, Integer> ib = component.getInBuffers();
//                        for (String s : ib.keySet()) {
//                            addDP(p, "I." + s, ib.get(s));
//                        }
//                        // add output buffers
//                        Map<String, Integer> ob = component.getOutBuffers();
//                        for (String s : ob.keySet()) {
//                            addDP(p, "O." + s, ob.get(s));
//                        }
//                        write(dbName, p);
//                        AfecsTool.sleep(1);
//                    }
                }
            } catch (Exception e) {
                System.out.println("DDD: Error writing into influxDB");
            }
        }


        public boolean isConnected() {

            return jinFxConnected;
        }

        public boolean isServerUp(int timeout) throws Exception {
            jinFxConnected = ping(timeout);
            return jinFxConnected;
        }

        public void checkCreate() {
            try {
                if (isServerUp(1)) {
                    // connect to the database
                    // create database if it does not exists
                    if (!existsDB(dbName)) {
                        createDB(dbName, 1, JinTime.HOURE);
                    }
                }
            } catch (Exception e) {
                jinFxConnected = false;
            }
        }




    @Override
    public String generateReport(DpeReport dpeData) {
        return null;
    }

}
