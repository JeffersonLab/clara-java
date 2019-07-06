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
public class DpeRuntimeDataTest {

    private final JSONObject json;
    private final DpeRuntimeData data;

    public DpeRuntimeDataTest() {
        json = JsonUtils.readJson("/runtime-data.json")
                        .getJSONObject(ClaraConstants.RUNTIME_KEY);
        data = new DpeRuntimeData(json);
    }

    @Test
    public void name() throws Exception {
        assertThat(data.name().canonicalName(), is("10.1.1.10_java"));
    }

    @Test
    public void snapshotTime() throws Exception {
        assertThat(data.snapshotTime(), notNullValue());
    }

    @Test
    public void cpuUsage() throws Exception {
        assertThat(data.cpuUsage(), is(greaterThan(0.0)));
    }

    @Test
    public void testMemoryUsage() throws Exception {
        assertThat(data.memoryUsage(), is(greaterThan(0L)));
    }

    @Test
    public void testLoad() throws Exception {
        assertThat(data.systemLoad(), is(greaterThan(0.0)));
    }

    @Test
    public void withContainers() throws Exception {
        Set<String> names = data.containers()
                                .stream()
                                .map(ContainerRuntimeData::name)
                                .map(ContainerName::toString)
                                .collect(Collectors.toSet());

        assertThat(names, containsInAnyOrder("10.1.1.10_java:trevor",
                                             "10.1.1.10_java:franklin",
                                             "10.1.1.10_java:michael"));
    }

    @Test
    public void emptyContainers() throws Exception {
        json.put("containers", new JSONArray());
        DpeRuntimeData dpe = new DpeRuntimeData(json);

        assertThat(dpe.containers(), empty());
    }
}
