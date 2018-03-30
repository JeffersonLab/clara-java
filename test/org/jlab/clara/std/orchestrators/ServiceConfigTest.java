/*
 *   Copyright (c) 2017.  Jefferson Lab (JLab). All rights reserved. Permission
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

package org.jlab.clara.std.orchestrators;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.jlab.clara.base.ServiceName;
import org.json.JSONObject;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ServiceConfigTest {

    private static final JSONObject GLOBAL = new JSONObject();

    private static final JSONObject S1 = new JSONObject();
    private static final JSONObject S2 = new JSONObject();
    private static final JSONObject W = new JSONObject();

    private static final ServiceName N1 = new ServiceName("10.1.1.1_java:master:S1");
    private static final ServiceName N2 = new ServiceName("10.1.1.1_java:master:S2");

    static {
        fillWriter(W);
        fillGlobal(GLOBAL);
        fillS1(S1);
        fillS2(S2);
    }

    private static void fillGlobal(JSONObject json) {
        JSONObject geom = new JSONObject();
        geom.put("run", 10);
        geom.put("variation", "custom");

        json.put("limit", 9000);
        json.put("geometry", geom);
        json.put("log", false);
    }

    private static void fillWriter(JSONObject json) {
        json.put("compression", 2);
    }

    private static void fillS1(JSONObject json) {
        json.put("layers", Arrays.asList("inner", "outer"));
        json.put("filter", "skip");
        json.put("log", true);
    }

    private static void fillS2(JSONObject json) {
        json.put("hits", 10);
        json.put("filter", "greedy");
    }

    @Test
    public void withoutIOConfigurationReaderReturnsEmpty() throws Exception {
        ServiceConfig conf = new ServiceConfig(new JSONObject());

        assertThat(conf.reader().keySet(), is(empty()));
    }

    @Test
    public void withIOConfigurationReturnsService() throws Exception {
        JSONObject io = new JSONObject();
        io.put("writer", W);

        JSONObject data = new JSONObject();
        data.put("io-services", io);

        ServiceConfig conf = new ServiceConfig(data);

        assertThat(conf.writer().similar(W), is(true));
    }

    @Test
    public void withoutUserConfigurationReturnsEmpty() throws Exception {
        ServiceConfig conf = new ServiceConfig(new JSONObject());

        assertThat(conf.get(N1).keySet(), is(empty()));
    }

    @Test
    public void withOnlyGlobalConfigurationReturnsGlobal() throws Exception {
        JSONObject data = new JSONObject();
        data.put("global", GLOBAL);

        ServiceConfig conf = new ServiceConfig(data);

        assertThat(conf.get(N1).similar(GLOBAL), is(true));
    }

    @Test
    public void withOnlyOtherServicesConfigurationReturnsEmpty() throws Exception {
        JSONObject services = new JSONObject();
        services.put("S1", S1);

        JSONObject data = new JSONObject();
        data.put("services", services);

        ServiceConfig conf = new ServiceConfig(data);

        assertThat(conf.get(N2).keySet(), is(empty()));
    }

    @Test
    public void withOnlyServiceConfigurationReturnsService() throws Exception {
        JSONObject services = new JSONObject();
        services.put("S1", S1);
        services.put("S2", S2);

        JSONObject data = new JSONObject();
        data.put("services", services);

        ServiceConfig conf = new ServiceConfig(data);

        assertThat(conf.get(N2).similar(S2), is(true));
    }

    @Test
    public void withGlobalAndServiceConfigurationReturnsMerged() throws Exception {
        JSONObject services = new JSONObject();
        services.put("S1", S1);
        services.put("S2", S2);

        JSONObject data = new JSONObject();
        data.put("global", GLOBAL);
        data.put("services", services);

        JSONObject expected = new JSONObject();
        fillGlobal(expected);
        fillS1(expected);

        ServiceConfig conf = new ServiceConfig(data);

        assertThat(conf.get(N1).similar(expected), is(true));
    }

    @Test
    public void withGlobalAndServiceConfigurationOverridesGlobal() throws Exception {
        JSONObject services = new JSONObject();
        services.put("S1", S1);
        services.put("S2", S2);

        JSONObject data = new JSONObject();
        data.put("global", GLOBAL);
        data.put("services", services);

        ServiceConfig conf = new ServiceConfig(data);

        assertThat(conf.get(N1).getBoolean("log"), is(true));
    }

    @Test
    public void withTemplateAsValue() throws Exception {
        JSONObject test = new JSONObject();
        test.put("variation", "custom");
        test.put("run", 10);
        test.put("config_file", "${input_file?replace(\".dat\", \".conf\")}");
        test.put("user", "${\"john doe\"?capitalize}");

        JSONObject services = new JSONObject();
        services.put("Test", test);

        JSONObject data = new JSONObject();
        data.put("services", services);

        Map<String, Object> model = new HashMap<>();
        model.put("input_file", "run10_20180101.dat");

        ServiceConfig conf = new ServiceConfig(data, model);
        JSONObject serviceData = conf.get(new ServiceName("10.1.1.1_java:master:Test"));

        assertThat(serviceData.getString("config_file"), is("run10_20180101.conf"));
        assertThat(serviceData.getString("user"), is("John Doe"));
    }
}
