package org.jlab.clara.sys;

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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
