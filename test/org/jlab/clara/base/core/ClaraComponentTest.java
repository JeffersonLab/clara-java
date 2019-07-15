/*
 *   Copyright (c) 2016.  Jefferson Lab (JLab). All rights reserved. Permission
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

package org.jlab.clara.base.core;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;


public class ClaraComponentTest {

    @Test
    public void testJavaDpeComponent() throws Exception {
        ClaraComponent c = ClaraComponent.dpe("10.2.9.1_java");

        assertThat(c.getCanonicalName(), is("10.2.9.1_java"));
        assertThat(c.getTopic().toString(), is("dpe:10.2.9.1_java"));

        assertThat(c.getDpeCanonicalName(), is("10.2.9.1_java"));
        assertThat(c.getDpeHost(), is("10.2.9.1"));
        assertThat(c.getDpeLang(), is(ClaraConstants.JAVA_LANG));
        assertThat(c.getDpePort(), is(ClaraConstants.JAVA_PORT));
    }

    @Test
    public void testJavaContainerComponent() throws Exception {
        ClaraComponent c = ClaraComponent.container("10.2.9.1_java:master");

        assertThat(c.getCanonicalName(), is("10.2.9.1_java:master"));
        assertThat(c.getTopic().toString(), is("container:10.2.9.1_java:master"));

        assertThat(c.getDpeCanonicalName(), is("10.2.9.1_java"));
        assertThat(c.getDpeHost(), is("10.2.9.1"));
        assertThat(c.getDpeLang(), is(ClaraConstants.JAVA_LANG));
        assertThat(c.getDpePort(), is(ClaraConstants.JAVA_PORT));

        assertThat(c.getContainerName(), is("master"));
    }

    @Test
    public void testJavaServiceComponent() throws Exception {
        ClaraComponent c = ClaraComponent.service("10.2.9.1_java:master:E1");

        assertThat(c.getCanonicalName(), is("10.2.9.1_java:master:E1"));
        assertThat(c.getTopic().toString(), is("10.2.9.1_java:master:E1"));

        assertThat(c.getDpeCanonicalName(), is("10.2.9.1_java"));
        assertThat(c.getDpeHost(), is("10.2.9.1"));
        assertThat(c.getDpeLang(), is(ClaraConstants.JAVA_LANG));
        assertThat(c.getDpePort(), is(ClaraConstants.JAVA_PORT));

        assertThat(c.getContainerName(), is("master"));
        assertThat(c.getEngineName(), is("E1"));
    }

    @Test
    public void testCppDpeComponent() throws Exception {
        ClaraComponent c = ClaraComponent.dpe("10.2.9.1_cpp");

        assertThat(c.getCanonicalName(), is("10.2.9.1_cpp"));
        assertThat(c.getTopic().toString(), is("dpe:10.2.9.1_cpp"));

        assertThat(c.getDpeCanonicalName(), is("10.2.9.1_cpp"));
        assertThat(c.getDpeHost(), is("10.2.9.1"));
        assertThat(c.getDpeLang(), is(ClaraConstants.CPP_LANG));
        assertThat(c.getDpePort(), is(ClaraConstants.CPP_PORT));
    }

    @Test
    public void testCppContainerComponent() throws Exception {
        ClaraComponent c = ClaraComponent.container("10.2.9.1_cpp:master");

        assertThat(c.getCanonicalName(), is("10.2.9.1_cpp:master"));
        assertThat(c.getTopic().toString(), is("container:10.2.9.1_cpp:master"));

        assertThat(c.getDpeCanonicalName(), is("10.2.9.1_cpp"));
        assertThat(c.getDpeHost(), is("10.2.9.1"));
        assertThat(c.getDpeLang(), is(ClaraConstants.CPP_LANG));
        assertThat(c.getDpePort(), is(ClaraConstants.CPP_PORT));

        assertThat(c.getContainerName(), is("master"));
    }

    @Test
    public void testCppServiceComponent() throws Exception {
        ClaraComponent c = ClaraComponent.service("10.2.9.1_cpp:master:E1");

        assertThat(c.getCanonicalName(), is("10.2.9.1_cpp:master:E1"));
        assertThat(c.getTopic().toString(), is("10.2.9.1_cpp:master:E1"));

        assertThat(c.getDpeCanonicalName(), is("10.2.9.1_cpp"));
        assertThat(c.getDpeHost(), is("10.2.9.1"));
        assertThat(c.getDpeLang(), is(ClaraConstants.CPP_LANG));
        assertThat(c.getDpePort(), is(ClaraConstants.CPP_PORT));

        assertThat(c.getContainerName(), is("master"));
        assertThat(c.getEngineName(), is("E1"));
    }

    @Test
    public void testPythonDpeComponent() throws Exception {
        ClaraComponent c = ClaraComponent.dpe("10.2.9.1_python");

        assertThat(c.getCanonicalName(), is("10.2.9.1_python"));
        assertThat(c.getTopic().toString(), is("dpe:10.2.9.1_python"));

        assertThat(c.getDpeCanonicalName(), is("10.2.9.1_python"));
        assertThat(c.getDpeHost(), is("10.2.9.1"));
        assertThat(c.getDpeLang(), is(ClaraConstants.PYTHON_LANG));
        assertThat(c.getDpePort(), is(ClaraConstants.PYTHON_PORT));
    }

    @Test
    public void testPythonContainerComponent() throws Exception {
        ClaraComponent c = ClaraComponent.container("10.2.9.1_python:master");

        assertThat(c.getCanonicalName(), is("10.2.9.1_python:master"));
        assertThat(c.getTopic().toString(), is("container:10.2.9.1_python:master"));

        assertThat(c.getDpeCanonicalName(), is("10.2.9.1_python"));
        assertThat(c.getDpeHost(), is("10.2.9.1"));
        assertThat(c.getDpeLang(), is(ClaraConstants.PYTHON_LANG));
        assertThat(c.getDpePort(), is(ClaraConstants.PYTHON_PORT));

        assertThat(c.getContainerName(), is("master"));
    }

    @Test
    public void testPythonServiceComponent() throws Exception {
        ClaraComponent c = ClaraComponent.service("10.2.9.1_python:master:E1");

        assertThat(c.getCanonicalName(), is("10.2.9.1_python:master:E1"));
        assertThat(c.getTopic().toString(), is("10.2.9.1_python:master:E1"));

        assertThat(c.getDpeCanonicalName(), is("10.2.9.1_python"));
        assertThat(c.getDpeHost(), is("10.2.9.1"));
        assertThat(c.getDpeLang(), is(ClaraConstants.PYTHON_LANG));
        assertThat(c.getDpePort(), is(ClaraConstants.PYTHON_PORT));

        assertThat(c.getContainerName(), is("master"));
        assertThat(c.getEngineName(), is("E1"));
    }

    @Test
    public void testComponentWithCustomPort() throws Exception {
        ClaraComponent c = ClaraComponent.dpe("10.2.9.1%9999_java");

        assertThat(c.getCanonicalName(), is("10.2.9.1%9999_java"));
        assertThat(c.getTopic().toString(), is("dpe:10.2.9.1%9999_java"));

        assertThat(c.getDpeCanonicalName(), is("10.2.9.1%9999_java"));
        assertThat(c.getDpeHost(), is("10.2.9.1"));
        assertThat(c.getDpeLang(), is("java"));
        assertThat(c.getDpePort(), is(9999));
    }
}
