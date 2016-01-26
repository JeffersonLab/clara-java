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

package org.jlab.clara.sys.ccc;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import org.jlab.clara.base.CException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class CCompilerTest {

    private String composition = "10.10.10.1_java:C:S1+" +
                                 "10.10.10.1_java:C:S2+" +
                                 "10.10.10.1_java:C:S3+" +
                                 "10.10.10.1_java:C:S4;";

    @Test(expected = CException.class)
    public void testInvalidServiceName() throws Exception {
        String composition = "10.10.10.1_java:C:S1+" +
                             "10.10.10.1:C:S2+" +
                             "10.10.10.1:C:S4;";
        CCompiler cc = new CCompiler("10.10.10.1_java:C:S1");
        cc.compile(composition);
    }

    @Test(expected = CException.class)
    public void testMissingCurrentService() throws Exception {
        CCompiler cc = new CCompiler("10.10.10.1_java:C:S6");
        cc.compile(composition);
    }

    @Test
    public void testServiceAtTheBeginning() throws Exception {
        CCompiler cc = new CCompiler("10.10.10.1_java:C:S1");
        cc.compile(composition);

        Set<String> expected = new HashSet<String>(Arrays.asList("10.10.10.1_java:C:S2"));
        
        assertThat(getUnconditionalLinks(cc), is(expected));
    }

    private Set<String> getUnconditionalLinks(CCompiler cc) {
        Set<String> uncond = new HashSet<String>();
        for (Instruction inst : cc.getInstructions()) {
            for (Statement stmt : inst.getUnCondStatements()) {
                uncond.addAll(stmt.getOutputLinks());
            }
        }
        return uncond;
    }
    
    @Test
    public void testServiceAtTheMiddle() throws Exception {
        CCompiler cc = new CCompiler("10.10.10.1_java:C:S2");
        cc.compile(composition);

        Set<String> expected = new HashSet<String>(Arrays.asList("10.10.10.1_java:C:S3"));
        assertThat(getUnconditionalLinks(cc), is(expected));
    }

    @Test
    public void testServiceAtTheEnd() throws Exception {
        CCompiler cc = new CCompiler("10.10.10.1_java:C:S4");
        cc.compile(composition);

        Set<String> expected = new HashSet<String>();
        assertThat(getUnconditionalLinks(cc), is(expected));
    }

    @Test
    public void testLastServiceOnALoop() throws Exception {
        CCompiler cc = new CCompiler("10.10.10.1_java:C:S3");
        String composition = "10.10.10.1_java:C:S1+"
                           + "10.10.10.1_java:C:S3+"
                           + "10.10.10.1_java:C:S1;";
        cc.compile(composition);

        Set<String> expected = new HashSet<String>(Arrays.asList("10.10.10.1_java:C:S1"));
        assertThat(getUnconditionalLinks(cc), is(expected));
    }

    @Test
    public void testMultipleCalls() throws Exception {
        CCompiler cc = new CCompiler("10.10.10.1_java:C:S3");
        cc.compile(composition);

        String composition2 = "10.10.10.1_java:C:S1+" +
                              "10.10.10.1_java:C:S3+" +
                              "10.10.10.1_java:C:S5;";
        cc.compile(composition2);

        Set<String> expected = new HashSet<String>(Arrays.asList("10.10.10.1_java:C:S5"));
        assertThat(getUnconditionalLinks(cc), is(expected));
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
        
        ServiceState owner = new ServiceState("10.10.10.1_java:C:S1","FOO");
        ServiceState input = new ServiceState("WHATEVER","DON'T CARE");
        
        Set<String> expected = new HashSet<String>(Arrays.asList("10.10.10.1_java:C:S2"));
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
        
        ServiceState owner = new ServiceState("10.10.10.1_java:C:S1","FROZ");
        ServiceState input = new ServiceState("WHATEVER","DON'T CARE");
        
        Set<String> expected = new HashSet<String>(Arrays.asList("10.10.10.1_java:C:S4"));
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
        
        ServiceState owner = new ServiceState("10.10.10.1_java:C:S1","FRAPP");
        ServiceState input = new ServiceState("WHATEVER","DON'T CARE");
        
        Set<String> expected = new HashSet<String>(Arrays.asList("10.10.10.1_java:C:S5"));
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
        
        ServiceState owner = new ServiceState("10.10.10.1_java:C:S1","FOO");
        ServiceState input = new ServiceState("WHATEVER","DON'T CARE");
        
        Set<String> expected = new HashSet<String>(Arrays.asList("10.10.10.1_java:C:S2","10.10.10.1_java:C:S7"));
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
        ServiceState ownerSS = new ServiceState("10.10.10.1_java:C:S3", "udf");
        ServiceState inputSS = new ServiceState("10.10.10.1_java:C:S3", "udf");

        Set<String> expected = new HashSet<String>(Arrays.asList("10.10.10.1_java:C:S1"));
        assertThat(cc.getLinks(ownerSS, inputSS), is(expected));
    }
}
