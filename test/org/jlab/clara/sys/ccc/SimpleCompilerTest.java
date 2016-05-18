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

import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class SimpleCompilerTest {

    private String composition = "10.10.10.1_java:C:S1+" +
                                 "10.10.10.1_java:C:S2+" +
                                 "10.10.10.1_java:C:S3+" +
                                 "10.10.10.1_java:C:S4";

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidServiceName() throws Exception {
        String composition = "10.10.10.1_java:C:S1+" +
                             "10.10.10.1:C:S2+" +
                             "10.10.10.1:C:S4";
        SimpleCompiler cc = new SimpleCompiler("10.10.10.1_java:C:S1");
        cc.compile(composition);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingCurrentService() throws Exception {
        SimpleCompiler cc = new SimpleCompiler("10.10.10.1_java:C:S6");
        cc.compile(composition);
    }

    @Test
    public void testServiceAtTheBeginning() throws Exception {
        SimpleCompiler cc = new SimpleCompiler("10.10.10.1_java:C:S1");
        cc.compile(composition);

        Set<String> expected = new HashSet<>(Arrays.asList("10.10.10.1_java:C:S2"));
        assertThat(cc.getOutputs(), is(expected));
    }

    @Test
    public void testServiceAtTheMiddle() throws Exception {
        SimpleCompiler cc = new SimpleCompiler("10.10.10.1_java:C:S2");
        cc.compile(composition);

        Set<String> expected = new HashSet<>(Arrays.asList("10.10.10.1_java:C:S3"));
        assertThat(cc.getOutputs(), is(expected));
    }

    @Test
    public void testServiceAtTheEnd() throws Exception {
        SimpleCompiler cc = new SimpleCompiler("10.10.10.1_java:C:S4");
        cc.compile(composition);

        Set<String> expected = new HashSet<>();
        assertThat(cc.getOutputs(), is(expected));
    }

    @Test
    public void testMultipleCalls() throws Exception {
        SimpleCompiler cc = new SimpleCompiler("10.10.10.1_java:C:S3");
        cc.compile(composition);

        String composition2 = "10.10.10.1_java:C:S1+" +
                              "10.10.10.1_java:C:S3+" +
                              "10.10.10.1_java:C:S5";
        cc.compile(composition2);

        Set<String> expected = new HashSet<>(Arrays.asList("10.10.10.1_java:C:S5"));
        assertThat(cc.getOutputs(), is(expected));
    }
}
