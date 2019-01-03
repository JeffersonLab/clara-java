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

package org.jlab.clara.base;

import org.junit.Test;

import java.lang.reflect.Method;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.jlab.clara.base.core.ClaraConstants;

public class ClaraUtilTest {

    private static String[] goodDpeNames = new String[] {
        "192.168.1.102_java",
        "192.168.1.102_cpp",
        "192.168.1.102_python",
        "192.168.1.102%20000_java",
        "192.168.1.102%16000_cpp",
        "192.168.1.102%9999_python",
    };
    private static String[] goodContainerNames = new String[] {
        "10.2.58.17_java:master",
        "10.2.58.17_cpp:container1",
        "10.2.58.17_python:User",
        "10.2.58.17%20000_python:User",
        "10.2.58.17_java:best_container",
    };
    private static String[] goodServiceNames = new String[] {
        "129.57.28.27_java:master:SimpleEngine",
        "129.57.28.27_cpp:container1:IntegrationEngine",
        "129.57.28.27_python:User:StatEngine",
        "129.57.28.27%20000_python:User:StatEngine",
    };

    private static String[] badDpeNames = new String[] {
        "192.168.1.102",
        "192.168.1.102%",
        "192_168_1_102_java",
        "192.168.1.102_erlang",
        "192.168.1.103:python",
        "192.168.1.103%aaa_python",
        "192 168 1 102 java",
        " 192.168.1.102_java",
    };
    private static String[] badContainerNames = new String[] {
        "10.2.9.9_java:",
        "10.2.9.9_cpp:container:",
        "10.2.9.9_python:long,user",
        "10.2.58.17_python: User",
    };
    private static String[] badServiceNames = new String[] {
        "129.57.28.27_java:master:Simple:Engine",
        "129.57.28.27_cpp:container1:Integration...",
        "129.57.28.27_python:User:Stat,Engine",
        " 129.57.28.27_java:master:SimpleEngine",
        "129.57.28.27_java:master: SimpleEngine",
    };


    @Test
    public void validDpeName() throws Exception {
        assertValidNames("isDpeName", goodDpeNames);
    }


    @Test
    public void invalidDpeName() throws Exception {
        assertInvalidNames("isDpeName", badDpeNames);
        assertInvalidNames("isDpeName", goodContainerNames);
        assertInvalidNames("isDpeName", badContainerNames);
        assertInvalidNames("isDpeName", goodServiceNames);
        assertInvalidNames("isDpeName", badServiceNames);
    }


    @Test
    public void validContainerName() throws Exception {
        assertValidNames("isContainerName", goodContainerNames);
    }


    @Test
    public void invalidContainerName() throws Exception {
        assertInvalidNames("isContainerName", goodDpeNames);
        assertInvalidNames("isContainerName", badDpeNames);
        assertInvalidNames("isContainerName", badContainerNames);
        assertInvalidNames("isContainerName", goodServiceNames);
        assertInvalidNames("isContainerName", badServiceNames);
    }


    @Test
    public void validServiceName() throws Exception {
        assertValidNames("isServiceName", goodServiceNames);
    }


    @Test
    public void invalidServiceName() throws Exception {
        assertInvalidNames("isServiceName", goodDpeNames);
        assertInvalidNames("isServiceName", badDpeNames);
        assertInvalidNames("isServiceName", goodContainerNames);
        assertInvalidNames("isServiceName", badContainerNames);
        assertInvalidNames("isServiceName", badServiceNames);
    }


    @Test
    public void validCanonicalName() throws Exception {
        assertValidNames("isCanonicalName", goodDpeNames);
        assertValidNames("isCanonicalName", goodContainerNames);
        assertValidNames("isCanonicalName", goodServiceNames);
    }


    @Test
    public void invalidCanonicalName() throws Exception {
        assertInvalidNames("isCanonicalName", badDpeNames);
        assertInvalidNames("isCanonicalName", badContainerNames);
        assertInvalidNames("isCanonicalName", badServiceNames);
    }


    private void assertValidNames(String method, String[] names) throws Exception {
        Method m = ClaraUtil.class.getMethod(method, String.class);
        for (String name : names) {
            assertThat(m.invoke(null, name), is(true));
        }
    }


    private void assertInvalidNames(String method, String[] names) throws Exception {
        Method m = ClaraUtil.class.getMethod(method, String.class);
        for (String name : names) {
            assertThat(m.invoke(null, name), is(false));
        }
    }


    @Test
    public void getDpeHostReturnsTheHost() throws Exception {
        assertThat(ClaraUtil.getDpeHost(goodDpeNames[0]),       is("192.168.1.102"));
        assertThat(ClaraUtil.getDpeHost(goodContainerNames[0]), is("10.2.58.17"));
        assertThat(ClaraUtil.getDpeHost(goodServiceNames[0]),   is("129.57.28.27"));

        assertThat(ClaraUtil.getDpeHost(goodDpeNames[3]),       is("192.168.1.102"));
        assertThat(ClaraUtil.getDpeHost(goodContainerNames[3]), is("10.2.58.17"));
        assertThat(ClaraUtil.getDpeHost(goodServiceNames[3]),   is("129.57.28.27"));
    }

    @Test
    public void getDpePortReturnsThePort() throws Exception {
        assertThat(ClaraUtil.getDpePort(goodDpeNames[0]),       is(ClaraConstants.JAVA_PORT));
        assertThat(ClaraUtil.getDpePort(goodContainerNames[0]), is(ClaraConstants.JAVA_PORT));
        assertThat(ClaraUtil.getDpePort(goodServiceNames[0]),   is(ClaraConstants.JAVA_PORT));

        assertThat(ClaraUtil.getDpePort(goodDpeNames[1]),       is(ClaraConstants.CPP_PORT));
        assertThat(ClaraUtil.getDpePort(goodContainerNames[1]), is(ClaraConstants.CPP_PORT));
        assertThat(ClaraUtil.getDpePort(goodServiceNames[1]),   is(ClaraConstants.CPP_PORT));

        assertThat(ClaraUtil.getDpePort(goodDpeNames[2]),       is(ClaraConstants.PYTHON_PORT));
        assertThat(ClaraUtil.getDpePort(goodContainerNames[2]), is(ClaraConstants.PYTHON_PORT));
        assertThat(ClaraUtil.getDpePort(goodServiceNames[2]),   is(ClaraConstants.PYTHON_PORT));

        assertThat(ClaraUtil.getDpePort(goodDpeNames[3]),       is(20000));
        assertThat(ClaraUtil.getDpePort(goodContainerNames[3]), is(20000));
        assertThat(ClaraUtil.getDpePort(goodServiceNames[3]),   is(20000));
    }

    @Test
    public void getDpeLangReturnsTheLang() throws Exception {
        assertThat(ClaraUtil.getDpeLang(goodDpeNames[0]),       is("java"));
        assertThat(ClaraUtil.getDpeLang(goodContainerNames[0]), is("java"));
        assertThat(ClaraUtil.getDpeLang(goodServiceNames[0]),   is("java"));

        assertThat(ClaraUtil.getDpeLang(goodDpeNames[3]),       is("java"));
        assertThat(ClaraUtil.getDpeLang(goodContainerNames[3]), is("python"));
        assertThat(ClaraUtil.getDpeLang(goodServiceNames[3]),   is("python"));
    }

    @Test
    public void getDpeNameReturnsTheName() throws Exception {
        assertThat(ClaraUtil.getDpeName(goodDpeNames[0]),       is("192.168.1.102_java"));
        assertThat(ClaraUtil.getDpeName(goodContainerNames[0]), is("10.2.58.17_java"));
        assertThat(ClaraUtil.getDpeName(goodServiceNames[0]),   is("129.57.28.27_java"));

        assertThat(ClaraUtil.getDpeName(goodDpeNames[3]),       is("192.168.1.102%20000_java"));
        assertThat(ClaraUtil.getDpeName(goodContainerNames[3]), is("10.2.58.17%20000_python"));
        assertThat(ClaraUtil.getDpeName(goodServiceNames[3]),   is("129.57.28.27%20000_python"));
    }


    @Test
    public void getContainerCanonicalNameReturnsTheName() throws Exception {
        assertThat(ClaraUtil.getContainerCanonicalName(goodContainerNames[0]),
                   is("10.2.58.17_java:master"));

        assertThat(ClaraUtil.getContainerCanonicalName(goodServiceNames[0]),
                   is("129.57.28.27_java:master"));
    }


    @Test
    public void getContainerNameReturnsTheName() throws Exception {
        assertThat(ClaraUtil.getContainerName(goodContainerNames[0]), is("master"));
        assertThat(ClaraUtil.getContainerName(goodServiceNames[0]),   is("master"));
    }


    @Test
    public void getEngineNameReturnsTheName() throws Exception {
        assertThat(ClaraUtil.getEngineName(goodServiceNames[0]), is("SimpleEngine"));
    }


    @Test
    public void formDpeNameReturnsTheCanonicalName() throws Exception {
        assertThat(new DpeName("10.2.58.17", ClaraLang.JAVA).canonicalName(),
                   is("10.2.58.17_java"));
    }


    @Test
    public void formContainerNameReturnsTheCanonicalName() throws Exception {
        DpeName dpe = new DpeName("10.2.58.17", ClaraLang.JAVA);
        ContainerName container = new ContainerName(dpe, "master");
        assertThat(container.canonicalName(), is("10.2.58.17_java:master"));

        assertThat(new ContainerName("10.2.58.17", ClaraLang.JAVA, "master").canonicalName(),
                   is("10.2.58.17_java:master"));
    }


    @Test
    public void formServiceNameReturnsTheCanonicalName() throws Exception {
        ContainerName container = new ContainerName("10.2.58.17", ClaraLang.JAVA, "cont");
        assertThat(new ServiceName(container, "Engine").canonicalName(),
                   is("10.2.58.17_java:cont:Engine"));

        assertThat(new ServiceName("10.2.58.17", ClaraLang.JAVA, "cont", "Engine").canonicalName(),
                   is("10.2.58.17_java:cont:Engine"));
    }


    @Test
    public void splitIntoLinesSingleLine() throws Exception {
        String text = "Call me Ishmael.";
        int length = text.length();

        assertThat(ClaraUtil.splitIntoLines("Call me Ishmael.", "", length + 10),
                   is("Call me Ishmael."));

        assertThat(ClaraUtil.splitIntoLines("Call me Ishmael.", "", length),
                   is("Call me Ishmael."));

        assertThat(ClaraUtil.splitIntoLines("Call me Ishmael.", "    ", length + 10),
                   is("    Call me Ishmael."));

        assertThat(ClaraUtil.splitIntoLines("Call me Ishmael.", "    ", length),
                   is("    Call me Ishmael."));
    }


    @Test
    public void splitIntoLinesMultipleLines() throws Exception {
        String text = "Moby Dick seeks thee not. It is thou, thou, that madly seekest him!";

        assertThat(ClaraUtil.splitIntoLines(text, "", 25),
                   is("Moby Dick seeks thee not.\n"
                    + "It is thou, thou, that\n"
                    + "madly seekest him!"));

        assertThat(ClaraUtil.splitIntoLines(text, ">>>", 25),
                   is(">>>Moby Dick seeks thee not.\n"
                    + ">>>It is thou, thou, that\n"
                    + ">>>madly seekest him!"));
    }
}
