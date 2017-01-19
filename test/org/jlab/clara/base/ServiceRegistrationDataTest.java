package org.jlab.clara.base;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.util.report.JsonUtils;

public class ServiceRegistrationDataTest {

    private final JSONObject json;

    private ServiceRegistrationData data;

    public ServiceRegistrationDataTest() {
        json = JsonUtils.readJson("/resources/registration-data.json")
                        .getJSONObject(ClaraConstants.REGISTRATION_KEY);
    }

    @Before
    public void setup() {
        data = new ServiceRegistrationData(JsonUtils.getService(json, 1, 0));
    }

    @Test
    public void name() throws Exception {
        assertThat(data.name().canonicalName(), is("10.1.1.10_java:franklin:Engine2"));
    }

    @Test
    public void className() throws Exception {
        assertThat(data.className(), is("org.jlab.clara.examples.Engine2"));
    }

    @Test
    public void startTime() throws Exception {
        assertThat(data.startTime(), notNullValue());
    }

    @Test
    public void poolSize() throws Exception {
        assertThat(data.poolSize(), is(2));
    }

    @Test
    public void author() throws Exception {
        assertThat(data.author(), is("Trevor"));
    }

    @Test
    public void version() throws Exception {
        assertThat(data.version(), is("1.0"));
    }

    @Test
    public void description() throws Exception {
        assertThat(data.description(), containsString("Some description"));
    }
}
