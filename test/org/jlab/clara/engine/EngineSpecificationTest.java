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

package org.jlab.clara.engine;

import static org.junit.Assert.assertEquals;

import org.jlab.clara.IntegrationTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

@Category(IntegrationTest.class)
public class EngineSpecificationTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();


    @Test
    public void constructorThrowsIfSpecificationFileNotFound() {
        expectedEx.expect(EngineSpecification.ParseException.class);
        expectedEx.expectMessage("Service specification file not found");
        new EngineSpecification("std.services.convertors.EvioToNothing");
    }


    @Test
    public void constructorThrowsIfYamlIsMalformed() {
        expectedEx.expect(EngineSpecification.ParseException.class);
        expectedEx.expectMessage("Unexpected YAML content");
        new EngineSpecification("resources/service-spec-bad-1");
    }


    @Test
    public void parseServiceSpecification() {
        EngineSpecification sd = new EngineSpecification("resources/service-spec-simple");
        assertEquals(sd.name(), "SomeService");
        assertEquals(sd.engine(), "std.services.SomeService");
        assertEquals(sd.type(), "java");
    }


    @Test
    public void parseAuthorSpecification() {
        EngineSpecification sd = new EngineSpecification("resources/service-spec-simple");
        assertEquals(sd.author(), "Sebastian Mancilla");
        assertEquals(sd.email(), "smancill@jlab.org");
    }


    @Test
    public void parseVersionWhenYamlReturnsDouble() {
        EngineSpecification sd = new EngineSpecification("resources/service-spec-1");
        assertEquals(sd.version(), "0.8");
    }


    @Test
    public void parseVersionWhenYamlReturnsInteger() {
        EngineSpecification sd = new EngineSpecification("resources/service-spec-simple");
        assertEquals(sd.version(), "2");
    }


    @Test
    public void parseStringThrowsIfMissingKey() {
        expectedEx.expect(EngineSpecification.ParseException.class);
        expectedEx.expectMessage("Missing key:");
        new EngineSpecification("resources/service-spec-bad-2");
    }


    @Test
    public void parseStringThrowsIfBadKeyType() {
        expectedEx.expect(EngineSpecification.ParseException.class);
        expectedEx.expectMessage("Bad type for:");
        new EngineSpecification("resources/service-spec-bad-3");
    }
}
