package org.jlab.clara.base;

import org.json.JSONObject;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.util.report.JsonUtils;

public class ServiceRuntimeDataTest {

    private final JSONObject json;
    private final ServiceRuntimeData data;

    public ServiceRuntimeDataTest() {
        json = JsonUtils.readJson("/resources/runtime-data.json")
                        .getJSONObject(ClaraConstants.RUNTIME_KEY);
        data = new ServiceRuntimeData(JsonUtils.getService(json, 1, 0));
    }

    @Test
    public void name() throws Exception {
        assertThat(data.name().canonicalName(), is("10.1.1.10_java:franklin:Engine2"));
    }

    @Test
    public void snapshotTime() throws Exception {
        assertThat(data.snapshotTime(), notNullValue());
    }

    @Test
    public void numRequests() throws Exception {
        assertThat(data.numRequests(), is(2000L));
    }

    @Test
    public void numFailures() throws Exception {
        assertThat(data.numFailures(), is(200L));
    }

    @Test
    public void sharedMemoryReads() throws Exception {
        assertThat(data.sharedMemoryReads(), is(1800L));
    }

    @Test
    public void sharedMemoryWrites() throws Exception {
        assertThat(data.sharedMemoryWrites(), is(1800L));
    }

    @Test
    public void bytesReceived() throws Exception {
        assertThat(data.bytesReceived(), is(100L));
    }

    @Test
    public void bytesSent() throws Exception {
        assertThat(data.bytesSent(), is(330L));
    }

    @Test
    public void executionTime() throws Exception {
        assertThat(data.executionTime(), is(243235243543L));
    }
}
