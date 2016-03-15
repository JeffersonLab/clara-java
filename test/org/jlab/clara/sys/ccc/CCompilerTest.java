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

package org.jlab.clara.sys.ccc;

import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.base.error.ClaraException;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class CCompilerTest {

    private String composition = "10.10.10.1_java:C:S1+" +
                                 "10.10.10.1_java:C:S2+" +
                                 "10.10.10.1_java:C:S3+" +
                                 "10.10.10.1_java:C:S4;";

    @Test(expected = ClaraException.class)
    public void testInvalidServiceName() throws Exception {
        String composition = "10.10.10.1_java:C:S1+" +
                             "10.10.10.1:C:S2+" +
                             "10.10.10.1:C:S4;";
        CCompiler cc = new CCompiler("10.10.10.1_java:C:S1");
        cc.compile(composition);
    }

    @Test(expected = ClaraException.class)
    public void testMissingCurrentService() throws Exception {
        CCompiler cc = new CCompiler("10.10.10.1_java:C:S6");
        cc.compile(composition);
    }

    @Test
    public void testServiceAtTheBeginning() throws Exception {
        CCompiler cc = new CCompiler("10.10.10.1_java:C:S1");
        cc.compile(composition);

        Set<String> expected = new HashSet<>(Arrays.asList("10.10.10.1_java:C:S2"));

        assertThat(cc.getUnconditionalLinks(), is(expected));
    }


    @Test
    public void testServiceAtTheMiddle() throws Exception {
        CCompiler cc = new CCompiler("10.10.10.1_java:C:S2");
        cc.compile(composition);

        Set<String> expected = new HashSet<>(Arrays.asList("10.10.10.1_java:C:S3"));
        assertThat(cc.getUnconditionalLinks(), is(expected));
    }

    @Test
    public void testServiceAtTheEnd() throws Exception {
        CCompiler cc = new CCompiler("10.10.10.1_java:C:S4");
        cc.compile(composition);

        Set<String> expected = new HashSet<>();
        assertThat(cc.getUnconditionalLinks(), is(expected));
    }

    @Test
    public void testLastServiceOnALoop() throws Exception {
        CCompiler cc = new CCompiler("10.10.10.1_java:C:S3");
        String composition = "10.10.10.1_java:C:S1+"
                           + "10.10.10.1_java:C:S3+"
                           + "10.10.10.1_java:C:S1;";
        cc.compile(composition);


        Set<String> expected = new HashSet<>(Arrays.asList("10.10.10.1_java:C:S1"));
        assertThat(cc.getUnconditionalLinks(), is(expected));
    }

    @Test
    public void testMultipleCalls() throws Exception {
        CCompiler cc = new CCompiler("10.10.10.1_java:C:S3");
        cc.compile(composition);

        String composition2 = "10.10.10.1_java:C:S1+" +
                              "10.10.10.1_java:C:S3+" +
                              "10.10.10.1_java:C:S5;";
        cc.compile(composition2);

        Set<String> expected = new HashSet<>(Arrays.asList("10.10.10.1_java:C:S5"));
        assertThat(cc.getUnconditionalLinks(), is(expected));
    }

    @Test
    public void testConditional() throws Exception {
        CCompiler cc = new CCompiler("10.10.10.1_java:C:S1");
        cc.compile(composition);

        String composition2 = "10.10.10.1_java:C:S1;"
                            + "if (10.10.10.1_java:C:S1 == \"FOO\") { "
                            + "  10.10.10.1_java:C:S1+10.10.10.1_java:C:S2;"
                            + "}";
        cc.compile(composition2);

        ServiceState owner = new ServiceState("10.10.10.1_java:C:S1", "FOO");
        ServiceState input = new ServiceState("WHATEVER", "DON'T CARE");

        Set<String> expected = new HashSet<>(Arrays.asList("10.10.10.1_java:C:S2"));
        assertThat(cc.getLinks(owner, input), is(expected));
    }

    @Test
    public void testElifConditional() throws Exception {
        CCompiler cc = new CCompiler("10.10.10.1_java:C:S1");
        cc.compile(composition);

        String composition2 = "10.10.10.1_java:C:S1;"
                            + "if (10.10.10.1_java:C:S1 == \"FOO\") { "
                            + "  10.10.10.1_java:C:S1+10.10.10.1_java:C:S2;"
                            + "} elseif (10.10.10.1_java:C:S1 == \"BAR\") { "
                            + "  10.10.10.1_java:C:S1+10.10.10.1_java:C:S3;"
                            + "} elseif (10.10.10.1_java:C:S1 == \"FROZ\") { "
                            + "  10.10.10.1_java:C:S1+10.10.10.1_java:C:S4;"
                            + "}";
        cc.compile(composition2);

        ServiceState owner = new ServiceState("10.10.10.1_java:C:S1", "FROZ");
        ServiceState input = new ServiceState("WHATEVER", "DON'T CARE");

        Set<String> expected = new HashSet<>(Arrays.asList("10.10.10.1_java:C:S4"));
        assertThat(cc.getLinks(owner, input), is(expected));
    }

    @Test
    public void testElseConditional() throws Exception {
        CCompiler cc = new CCompiler("10.10.10.1_java:C:S1");
        cc.compile(composition);

        String composition2 = "10.10.10.1_java:C:S1;"
                            + "if (10.10.10.1_java:C:S1 == \"FOO\") { "
                            + "  10.10.10.1_java:C:S1+10.10.10.1_java:C:S2;"
                            + "} elseif (10.10.10.1_java:C:S1 == \"BAR\") { "
                            + "  10.10.10.1_java:C:S1+10.10.10.1_java:C:S3;"
                            + "} elseif (10.10.10.1_java:C:S1 == \"FROZ\") { "
                            + "  10.10.10.1_java:C:S1+10.10.10.1_java:C:S4;"
                            + "} else {"
                            + "  10.10.10.1_java:C:S1+10.10.10.1_java:C:S5;"
                            + "}";
        cc.compile(composition2);

        ServiceState owner = new ServiceState("10.10.10.1_java:C:S1", "FRAPP");
        ServiceState input = new ServiceState("WHATEVER", "DON'T CARE");

        Set<String> expected = new HashSet<>(Arrays.asList("10.10.10.1_java:C:S5"));
        assertThat(cc.getLinks(owner, input), is(expected));
    }

    @Test
    public void testConditionalMultipleStatement() throws Exception {
        CCompiler cc = new CCompiler("10.10.10.1_java:C:S1");
        cc.compile(composition);

        String composition2 = "10.10.10.1_java:C:S1;"
                            + "if (10.10.10.1_java:C:S1 == \"FOO\") { "
                            + "  10.10.10.1_java:C:S1+10.10.10.1_java:C:S2;"
                            + "  10.10.10.1_java:C:S1+10.10.10.1_java:C:S7;"
                            + "}";
        cc.compile(composition2);

        ServiceState owner = new ServiceState("10.10.10.1_java:C:S1", "FOO");
        ServiceState input = new ServiceState("WHATEVER", "DON'T CARE");

        Set<String> expected = new HashSet<>(Arrays.asList("10.10.10.1_java:C:S2",
                                                                 "10.10.10.1_java:C:S7"));
        assertThat(cc.getLinks(owner, input), is(expected));
    }

    @Test
    public void testConditionalLastServiceOnALoop() throws Exception {
        CCompiler cc = new CCompiler("10.10.10.1_java:C:S3");
        String composition = "10.10.10.1_java:C:S1+"
                           + "10.10.10.1_java:C:S3+"
                           + "10.10.10.1_java:C:S1;";
        cc.compile(composition);


        // service-states for conditional routing
        ServiceState ownerSS = new ServiceState("10.10.10.1_java:C:S3", ClaraConstants.UNDEFINED);
        ServiceState inputSS = new ServiceState("10.10.10.1_java:C:S3", ClaraConstants.UNDEFINED);

        Set<String> expected = new HashSet<>(Arrays.asList("10.10.10.1_java:C:S1"));
        assertThat(cc.getLinks(ownerSS, inputSS), is(expected));
    }
}
