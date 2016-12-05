/*
 *   Copyright (c) 2016.  Jefferson Lab (JLab). All rights reserved. Permission
 *   to use, copy, modify, and distribute  this software and its documentation for
 *   educational, research, and not-for-profit purposes, without fee and without a
 *   signed licensing agreement.
 *
 *   IN NO EVENT SHALL JLAB BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL
 *   INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING
 *   OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF JLAB HAS
 *   BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *   JLAB SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *   THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *   PURPOSE. THE CLARA SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY,
 *   PROVIDED HEREUNDER IS PROVIDED "AS IS". JLAB HAS NO OBLIGATION TO PROVIDE
 *   MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *   This software was developed under the United States Government license.
 *   For more information contact author at gurjyan@jlab.org
 *   Department of Experimental Nuclear Physics, Jefferson Lab.
 */

package org.jlab.clara.util.report;

import org.influxdb.dto.Point;
import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.base.core.ClaraConstants;
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
public class JinFluxReportBuilder extends JinFlux implements ExternalReport {

    private String dbName, session;
    private boolean jinFxConnected = true;
    private long totalExecTime;

    public JinFluxReportBuilder(String dbNode, String dbName, String session, String user, String password) throws JinFluxException {
        super(dbNode, user, password);
        this.dbName = dbName;
        this.session = session;
        try {
            if (!existsDB(dbName)) {
                createDB(dbName, 1, JinTime.HOURE);
            }
        } catch (Exception e) {
            jinFxConnected = false;
        }

    }

    public JinFluxReportBuilder(String dbNode, String dbName, String session) throws JinFluxException {
        super(dbNode);
        this.dbName = dbName;
        this.session = session;

        try {
            if (!existsDB(dbName)) {
                createDB(dbName, 1, JinTime.HOURE);
            }

        } catch (Exception e) {
            jinFxConnected = false;
            e.printStackTrace();
        }
    }


    public void push(DpeReport dpeData) {
        try {

            if (jinFxConnected) {

                Map<String, String> tags;
                Point.Builder p;

                long totalExecTime = 0;
                for (ContainerReport cr : dpeData.getContainers()) {

                    for (ServiceReport sr : cr.getServices()) {
                        tags = new HashMap<>();
                        tags.put(ClaraConstants.SESSION, session+"-"+dpeData.getHost());
                        tags.put("service_name", sr.getEngineName());
                        p = openTB("clas12", tags);

                        addDP(p,ClaraConstants.DPE, dpeData.getHost());
//                        addDP(p, "core_count", dpeData.getCoreCount());
                        addDP(p, "core_count", 7);
                        addDP(p, "pool_size", dpeData.getPoolSize());
                        addDP(p, "cpu_usage", dpeData.getCpuUsage());
                        addDP(p, "memory_usage", dpeData.getMemoryUsage());
                        addDP(p, "load", dpeData.getLoad());

//                        addDP(p, "class_name", sr.getClassName());
//                        addDP(p, "engine_name", sr.getEngineName());
//                        addDP(p, "author", sr.getAuthor());
//                        addDP(p, "version", sr.getVersion());
//                        addDP(p, "description", sr.getDescription());
//                        addDP(p, "language", sr.getLang());
//                        addDP(p, "start_time", sr.getStartTime());

                        long serviceRequests = sr.getRequestCount();

                        addDP(p,"n_requests", serviceRequests);
                        addDP(p,"n_failures", sr.getFailureCount());
                        addDP(p,"shm_reads", sr.getShrmReads());
                        addDP(p,"shm_writes", sr.getShrmWrites());
                        addDP(p,"bytes_recv", sr.getBytesReceived());
                        addDP(p,"bytes_sent", sr.getBytesSent());
                        if (sr.getShrmReads()>0) {
                            long execTime = sr.getExecutionTime()/sr.getShrmReads();
                            addDP(p,"exec_time", execTime);
                            totalExecTime = totalExecTime+execTime;
                        }
                        write(dbName, p);
                        ClaraUtil.sleep(100);
                    }
                }
                tags = new HashMap<>();
                tags.put(ClaraConstants.SESSION, session+"-"+dpeData.getHost());
                p = openTB("clas12", tags);
                addDP(p,ClaraConstants.DPE, dpeData.getHost());
                addDP(p,"total_exec_time", totalExecTime);
                addDP(p,"average_exec_time", totalExecTime/dpeData.getPoolSize());
                write(dbName, p);

                System.out.println("JinFlux report ...");
            }
        } catch (Exception e) {
            System.out.println("DDD:Error =============: Error writing into influxDB");
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
        push(dpeData);
        return null;
    }

}
