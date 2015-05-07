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

import java.lang.reflect.Method;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ClaraUtilTest {

    private String[] goodDpeNames = new String[] {
            "192.168.1.102_java",
            "192.168.1.102_cpp",
            "192.168.1.102_python",
    };
    private String[] goodContainerNames = new String[] {
            "10.2.58.17_java:master",
            "10.2.58.17_java:best_container",
            "10.2.58.17_cpp:container1",
            "10.2.58.17_python:User",
    };
    private String[] goodServiceNames = new String[] {
            "129.57.28.27_java:master:SimpleEngine",
            "129.57.28.27_cpp:container1:IntegrationEngine",
            "129.57.28.27_python:User:StatEngine",
    };

    private String[] badDpeNames = new String[] {
            "192.168.1.102",
            "192_168_1_102_java",
            "192.168.1.102_erlang",
            "192.168.1.103:python",
            "192 168 1 102 java",
            " 192.168.1.102_java",
    };
    private String[] badContainerNames = new String[] {
            "10.2.9.9_java:",
            "10.2.9.9_cpp:container:",
            "10.2.9.9_python:long,user",
            "10.2.58.17_python: User",
    };
    private String[] badServiceNames = new String[] {
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
            assertThat((boolean) m.invoke(null, name), is(true));
        }
    }


    private void assertInvalidNames(String method, String[] names) throws Exception {
        Method m = ClaraUtil.class.getMethod(method, String.class);
        for (String name : names) {
            assertThat((boolean) m.invoke(null, name), is(false));
        }
    }


    @Test
    public void getHostNameReturnsTheHost() throws Exception {
        assertThat(ClaraUtil.getHostName(goodDpeNames[0]),       is("192.168.1.102"));
        assertThat(ClaraUtil.getHostName(goodContainerNames[0]), is("10.2.58.17"));
        assertThat(ClaraUtil.getHostName(goodServiceNames[0]),   is("129.57.28.27"));
    }


    @Test
    public void getDpeNameReturnsTheName() throws Exception {
        assertThat(ClaraUtil.getDpeName(goodDpeNames[0]),       is("192.168.1.102_java"));
        assertThat(ClaraUtil.getDpeName(goodContainerNames[0]), is("10.2.58.17_java"));
        assertThat(ClaraUtil.getDpeName(goodServiceNames[0]),   is("129.57.28.27_java"));
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
}
