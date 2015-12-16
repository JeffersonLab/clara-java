/*
 * Copyright (C) 2015. Jefferson Lab, CLARA framework (JLAB). All Rights Reserved.
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for educational, research, and not-for-profit purposes,
 * without fee and without a signed licensing agreement.
 *
 * Author Vardan Gyurjyan
 * Department of Experimental Nuclear Physics, Jefferson Lab.
 *
 * IN NO EVENT SHALL JLAB BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
 * INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
 * THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF JLAB HAS BEEN ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * JLAB SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE. THE CLARA SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED
 * HEREUNDER IS PROVIDED "AS IS". JLAB HAS NO OBLIGATION TO PROVIDE MAINTENANCE,
 * SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */
package org.jlab.clara.util.report;

/**
 * @author gurjyan
 * @version 4.x
 */
public class JsonReportBuilder implements ExternalReport {
    @Override
    public String generateReport(DpeReport dpeData) {
        StringBuilder sb = new StringBuilder();


        sb.append(" {");
        sb.append(" \"DPERuntime\": {");
        sb.append(" \"hostname\": \"" + dpeData.getHost() + "\",");
        sb.append(" \"snapshot_time\": \"" + dpeData.getSnapshotTime() + "\",");
        sb.append(" \"cpu_usage\": " + dpeData.getCpuUsage() + ",");
        sb.append(" \"memory_usage\": " + dpeData.getMemoryUsage() + ",");
        sb.append(" \"load\": " + dpeData.getLoad() + ",");
        sb.append(" \"containers\": [");

        for (ContainerReport cr : dpeData.getContainers().values()) {
            sb.append(" {");
            sb.append(" \"ContainerRuntime\": {");
            sb.append(" \"name\": \"" + cr.getName() + "\",");
            sb.append(" \"snapshot_time\": \"" + cr.getSnapshotTime() + "\",");
            sb.append(" \"n_requests\": " + cr.getRequestCount() + ",");
            sb.append(" \"services\": [");
            for (ServiceReport sr : cr.getServices().values()) {
                sb.append(" {");
                sb.append(" \"ServiceRuntime\": {");
                sb.append(" \"name\": \"" + sr.getName() + "\",");
                sb.append(" \"snapshot_time\": \"" + sr.getSnapshotTime() + "\",");
                sb.append(" \"n_requests\": " + sr.getRequestCount() + ",");
                sb.append(" \"n_failures\": " + sr.getFailureCount() + ",");
                sb.append(" \"shm_reads\": " + sr.getShrmReads() + ",");
                sb.append(" \"shm_writes\": " + sr.getShrmWrites() + ",");
                sb.append(" \"bytes_recv\": " + sr.getBytesReceived() + ",");
                sb.append(" \"bytes_sent\": " + sr.getBytesSent() + ",");
                sb.append(" \"exec_time\": " + sr.getExecutionTime() + "");
                sb.append(" }");
                sb.append(" },");
            }
            sb.append("  ],");
        }
        sb.append(" ]");
        sb.append(" },");

        sb.append(" \"DPERegistration\": {");
        sb.append(" \"language\": \"" + dpeData.getLang() + "\",");
        sb.append(" \"start_time\": \"" + dpeData.getStartTime() + "\",");
        sb.append(" \"n_cores\": " + dpeData.getCoreCount() + ",");
        sb.append(" \"hostname\": \"" + dpeData.getHost() + "\",");
        sb.append(" \"memory_size\": " + dpeData.getMemorySize() + ",");
        sb.append(" \"containers\": [");

        for (ContainerReport cr : dpeData.getContainers().values()) {
            sb.append(" {");
            sb.append(" \"ContainerRegistration\": {");
            sb.append(" \"name\": \"" + cr.getName() + "\",");
            sb.append(" \"language\": \"" + cr.getLang() + "\",");
            sb.append(" \"author\": \"" + cr.getAuthor() + "\",");
            sb.append(" \"start_time\": \"" + cr.getStartTime() + "\",");
            sb.append(" \"services\": [");
            for (ServiceReport sr : cr.getServices().values()) {
                sb.append(" {");
                sb.append(" \"ServiceRegistration\": {");
                sb.append(" \"class_name\": \"" + sr.getClassName() + "\",");
                sb.append(" \"engine_name\": \"" + sr.getEngineName() + "\",");
                sb.append(" \"author\": \"" + sr.getAuthor() + "\",");
                sb.append(" \"version\": \"" + sr.getVersion() + "\",");
                sb.append(" \"description\": \"" + sr.getDescription() + "\",");
                sb.append(" \"language\": \"" + sr.getLang() + "\",");
                sb.append(" \"start_time\": \"" + sr.getStartTime() + "\"");
                sb.append(" },");
            }
            sb.append(" },");
        }
        sb.append(" ]");
        sb.append(" }");
        sb.append(" }");

        return sb.toString();
    }
}