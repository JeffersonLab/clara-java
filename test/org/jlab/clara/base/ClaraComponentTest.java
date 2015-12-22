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

package org.jlab.clara.base;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.jlab.coda.xmsg.core.xMsgConstants;


public class ClaraComponentTest {

    private final int defaultPort = xMsgConstants.DEFAULT_PORT;

    @Test
    public void testDpeComponent() throws Exception {
        ClaraComponent c = ClaraComponent.dpe("10.2.9.1_java");

        assertThat(c.getCanonicalName(), is("10.2.9.1_java"));
        assertThat(c.getTopic().toString(), is("dpe:10.2.9.1_java"));

        assertThat(c.getDpeCanonicalName(), is("10.2.9.1_java"));
        assertThat(c.getDpeHost(), is("10.2.9.1"));
        assertThat(c.getDpeLang(), is("java"));
        assertThat(c.getDpePort(), is(defaultPort));
    }

    @Test
    public void testContainerComponent() throws Exception {
        ClaraComponent c = ClaraComponent.container("10.2.9.1_java:master");

        assertThat(c.getCanonicalName(), is("10.2.9.1_java:master"));
        assertThat(c.getTopic().toString(), is("container:10.2.9.1_java:master"));

        assertThat(c.getDpeCanonicalName(), is("10.2.9.1_java"));
        assertThat(c.getDpeHost(), is("10.2.9.1"));
        assertThat(c.getDpeLang(), is("java"));
        assertThat(c.getDpePort(), is(defaultPort));

        assertThat(c.getContainerName(), is("master"));
    }

    @Test
    public void testServiceComponent() throws Exception {
        ClaraComponent c = ClaraComponent.service("10.2.9.1_java:master:E1");

        assertThat(c.getCanonicalName(), is("10.2.9.1_java:master:E1"));
        assertThat(c.getTopic().toString(), is("10.2.9.1_java:master:E1"));

        assertThat(c.getDpeCanonicalName(), is("10.2.9.1_java"));
        assertThat(c.getDpeHost(), is("10.2.9.1"));
        assertThat(c.getDpeLang(), is("java"));
        assertThat(c.getDpePort(), is(defaultPort));

        assertThat(c.getContainerName(), is("master"));
        assertThat(c.getEngineName(), is("E1"));
    }
}
