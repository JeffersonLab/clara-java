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

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
/**
 * @author gurjyan
 * @version 4.x
 */
public class JsonReportBuilder implements ExternalReport {

	@SuppressWarnings("unchecked")
	@Override
	public String generateReport(DpeReport dpeData) {
		JSONObject dpeRuntime = new JSONObject();
		dpeRuntime.put("hostname", dpeData.getHost());
		dpeRuntime.put("snapshot_time", dpeData.getSnapshotTime());
		dpeRuntime.put("cpu_usage", dpeData.getCpuUsage());
		dpeRuntime.put("memory_usage", dpeData.getMemoryUsage());
		dpeRuntime.put("load", dpeData.getLoad());

		JSONArray containersRuntimeArray = new JSONArray();
		for (ContainerReport cr : dpeData.getContainers().values()) {
			JSONObject containerRuntime = new JSONObject();
			containerRuntime.put("name", cr.getName());
			containerRuntime.put("snapshot_time", cr.getSnapshotTime());
			containerRuntime.put("n_requests", cr.getRequestCount());

			JSONArray servicesRuntimeArray = new JSONArray();
			for (ServiceReport sr : cr.getServices().values()) {
				JSONObject serviceRuntime = new JSONObject();
				serviceRuntime.put("name", sr.getName());
				serviceRuntime.put("snapshot_time", sr.getSnapshotTime());
				serviceRuntime.put("n_requests", sr.getRequestCount());
				serviceRuntime.put("n_failures", sr.getFailureCount());
				serviceRuntime.put("shm_reads", sr.getShrmReads());
				serviceRuntime.put("shm_writes", sr.getShrmWrites());
				serviceRuntime.put("bytes_recv", sr.getBytesReceived());
				serviceRuntime.put("bytes_sent", sr.getBytesSent());
				serviceRuntime.put("exec_time", sr.getExecutionTime());

				servicesRuntimeArray.add(serviceRuntime);
			}

			containerRuntime.put("services", servicesRuntimeArray);
			containersRuntimeArray.add(containerRuntime);

        }

		dpeRuntime.put("containers", containersRuntimeArray);

		JSONObject dpeRegistration = new JSONObject();
		dpeRegistration.put("language", dpeData.getLang());
		dpeRegistration.put("start_time", dpeData.getStartTime());
		dpeRegistration.put("n_cores", dpeData.getCoreCount());
		dpeRegistration.put("hostname", dpeData.getHost());
		dpeRegistration.put("memory_size", dpeData.getMemorySize());

		JSONArray containersRegistrationArray = new JSONArray();
		for (ContainerReport cr : dpeData.getContainers().values()) {
			JSONObject containerRegistration = new JSONObject();
			containerRegistration.put("name", cr.getName());
			containerRegistration.put("language", cr.getLang());
			containerRegistration.put("author", cr.getAuthor());
			containerRegistration.put("start_time", cr.getStartTime());

			JSONArray servicesRegistrationArray = new JSONArray();
			for (ServiceReport sr : cr.getServices().values()) {
				JSONObject serviceRegistration = new JSONObject();
				serviceRegistration.put("class_name", sr.getClassName());
				serviceRegistration.put("engine_name", sr.getEngineName());
				serviceRegistration.put("author", sr.getAuthor());
				serviceRegistration.put("version", sr.getVersion());
				serviceRegistration.put("description", sr.getDescription());
				serviceRegistration.put("language", sr.getLang());
				serviceRegistration.put("start_time", sr.getStartTime());

				servicesRegistrationArray.add(serviceRegistration);
			}

			containerRegistration.put("services", servicesRegistrationArray);
			containersRegistrationArray.add(containerRegistration);

		}

		dpeRegistration.put("containers", containersRegistrationArray);

		JSONObject dpeJsonData = new JSONObject();
		dpeJsonData.put("DPERuntime", dpeRuntime);
		dpeJsonData.put("DPERegistration", dpeRegistration);

		return dpeJsonData.toJSONString();
	}
}
