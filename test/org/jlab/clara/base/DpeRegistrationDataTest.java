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

package org.jlab.clara.base;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

import org.jlab.clara.IntegrationTest;
import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.util.report.JsonUtils;

@Category(IntegrationTest.class)
public class DpeRegistrationDataTest {

    private final JSONObject json;
    private final DpeRegistrationData data;

    public DpeRegistrationDataTest() {
        json = JsonUtils.readJson("/registration-data.json")
                        .getJSONObject(ClaraConstants.REGISTRATION_KEY);
        data = new DpeRegistrationData(json);
    }

    @Test
    public void name() throws Exception {
        assertThat(data.name().canonicalName(), is("10.1.1.10_java"));
    }

    @Test
    public void startTime() throws Exception {
        assertThat(data.startTime(), notNullValue());
    }

    @Test
    public void claraHome() throws Exception {
        assertThat(data.claraHome(), is("/home/user/clara"));
    }

    @Test
    public void session() throws Exception {
        assertThat(data.session(), is("los_santos"));
    }

    @Test
    public void numCores() throws Exception {
        assertThat(data.numCores(), is(8));
    }

    @Test
    public void memorySize() throws Exception {
        assertThat(data.memorySize(), greaterThan(0L));
    }

    @Test
    public void withContainers() throws Exception {
        Set<String> names = data.containers()
                                .stream()
                                .map(ContainerRegistrationData::name)
                                .map(ContainerName::toString)
                                .collect(Collectors.toSet());

        assertThat(names, containsInAnyOrder("10.1.1.10_java:trevor",
                                             "10.1.1.10_java:franklin",
                                             "10.1.1.10_java:michael"));
    }

    @Test
    public void emptyContainers() throws Exception {
        json.put("containers", new JSONArray());
        DpeRegistrationData dpe = new DpeRegistrationData(json);

        assertThat(dpe.containers(), empty());
    }
}
