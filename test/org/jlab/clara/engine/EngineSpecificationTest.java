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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.jlab.clara.engine.EngineSpecification.ParseException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
public class EngineSpecificationTest {

    @Test
    public void constructorThrowsIfSpecificationFileNotFound() {
        Exception ex = assertThrows(ParseException.class, () ->
                new EngineSpecification("std.services.convertors.EvioToNothing"));
        assertThat(ex.getMessage(), containsString("Service specification file not found"));
    }


    @Test
    public void constructorThrowsIfYamlIsMalformed() {
        Exception ex = assertThrows(ParseException.class, () ->
                new EngineSpecification("resources/service-spec-bad-1"));
        assertThat(ex.getMessage(), is("Unexpected YAML content"));
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
        Exception ex = assertThrows(ParseException.class, () ->
                new EngineSpecification("resources/service-spec-bad-2"));
        assertThat(ex.getMessage(), containsString("Missing key:"));
    }


    @Test
    public void parseStringThrowsIfBadKeyType() {
        Exception ex = assertThrows(ParseException.class, () ->
                new EngineSpecification("resources/service-spec-bad-3"));
        assertThat(ex.getMessage(), containsString("Bad type for:"));
    }
}
