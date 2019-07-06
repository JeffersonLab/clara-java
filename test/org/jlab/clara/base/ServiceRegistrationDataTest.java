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
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

import org.jlab.clara.IntegrationTest;
import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.util.report.JsonUtils;

@Category(IntegrationTest.class)
public class ServiceRegistrationDataTest {

    private final JSONObject json;
    private final ServiceRegistrationData data;

    public ServiceRegistrationDataTest() {
        json = JsonUtils.readJson("/registration-data.json")
                        .getJSONObject(ClaraConstants.REGISTRATION_KEY);
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
