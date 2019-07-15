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

import org.json.JSONObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.util.report.JsonUtils;

@Tag("integration")
public class ServiceRuntimeDataTest {

    private final JSONObject json;
    private final ServiceRuntimeData data;

    public ServiceRuntimeDataTest() {
        json = JsonUtils.readJson("/runtime-data.json")
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
