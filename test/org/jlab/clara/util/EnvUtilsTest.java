/*
 *   Copyright (c) 2018.  Jefferson Lab (JLab). All rights reserved. Permission
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

package org.jlab.clara.util;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class EnvUtilsTest {

    private final Map<String, String> env = new HashMap<>();

    public EnvUtilsTest() {
        env.put("VAR", "value");
        env.put("TEST", "this test");
        env.put("DIR", "/mnt/files");
    }

    @Test
    public void expandVariableWithValue() throws Exception {
        // checkstyle.off: LineLength
        assertThat(EnvUtils.expandEnvironment("$VAR", env), is("value"));
        assertThat(EnvUtils.expandEnvironment("${VAR}", env), is("value"));
        assertThat(EnvUtils.expandEnvironment("${VAR:-default}", env), is("value"));
        assertThat(EnvUtils.expandEnvironment("${VAR} and ${VAR}", env), is("value and value"));
        assertThat(EnvUtils.expandEnvironment("the ${VAR} is ${DIR}", env), is("the value is /mnt/files"));
        assertThat(EnvUtils.expandEnvironment("$DIR/exp1", env), is("/mnt/files/exp1"));
        assertThat(EnvUtils.expandEnvironment("test ${TEST}", env), is("test this test"));
        // checkstyle.on: LineLength
    }

    @Test
    public void expandMissingVariableWithDefault() throws Exception {
        assertThat(EnvUtils.expandEnvironment("${FOO:-bar}", env), is("bar"));
        assertThat(EnvUtils.expandEnvironment("a ${MISSING:-default value1} variable", env),
                   is("a default value1 variable"));
    }

    @Test
    public void expandMissingVariableWithNoDefault() throws Exception {
        assertThat(EnvUtils.expandEnvironment("a ${MISSING} variable", env), is("a  variable"));
        assertThat(EnvUtils.expandEnvironment("$MISSING", env), is(""));
    }

    @Test
    public void expandVariableWithUnbracedSeparator() throws Exception {
        assertThat(EnvUtils.expandEnvironment("a $VAR:-foo", env), is("a value:-foo"));
        assertThat(EnvUtils.expandEnvironment("a $MISSING:-bar", env), is("a :-bar"));
    }

    @Test
    public void expandEscapedVariable() throws Exception {
        assertThat(EnvUtils.expandEnvironment("this is \\$VAR", env), is("this is $VAR"));
        assertThat(EnvUtils.expandEnvironment("\\${VAR}", env), is("${VAR}"));
    }

    @Test
    public void expandWithoutVariables() throws Exception {
        assertThat(EnvUtils.expandEnvironment("foo bar", env), is("foo bar"));
        assertThat(EnvUtils.expandEnvironment("", env), is(""));
        assertThat(EnvUtils.expandEnvironment("{}", env), is("{}"));
        assertThat(EnvUtils.expandEnvironment("\\$", env), is("$"));
    }

    @Test
    public void testName() throws Exception {
        String[] invalidVariables = {
            "${",
            "$}",
            "${}",
            "${ }",
            "${ foo}",
            "${foo }"
        };

        for (String invalid : invalidVariables) {
            try {
                EnvUtils.expandEnvironment(invalid, env);
                fail("Expected an exception to be thrown for " + invalid);
            } catch (IllegalArgumentException e) { }
        }
    }
}
