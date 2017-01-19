package org.jlab.clara.base;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.util.report.JsonUtils;

public class ContainerRegistrationDataTest {

    private final JSONObject json;

    private ContainerRegistrationData data;

    public ContainerRegistrationDataTest() {
        json = JsonUtils.readJson("/resources/registration-data.json")
                        .getJSONObject(ClaraConstants.REGISTRATION_KEY);
    }

    @Before
    public void setup() {
        data = new ContainerRegistrationData(JsonUtils.getContainer(json, 1));
    }

    @Test
    public void name() throws Exception {
        assertThat(data.name().canonicalName(), is("10.1.1.10_java:franklin"));
    }

    @Test
    public void startTime() throws Exception {
        assertThat(data.startTime(), notNullValue());
    }

    @Test
    public void withServices() throws Exception {
        Set<String> names = data.services()
                                .stream()
                                .map(ServiceRegistrationData::name)
                                .map(ServiceName::toString)
                                .collect(Collectors.toSet());

        assertThat(names, containsInAnyOrder("10.1.1.10_java:franklin:Engine2",
                                             "10.1.1.10_java:franklin:Engine3"));
    }

    @Test
    public void emptyServices() throws Exception {
        data = new ContainerRegistrationData(JsonUtils.getContainer(json, 2));

        assertThat(data.services(), empty());
    }
}
