package org.jlab.clara.std.cli;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.junit.Test;

import java.util.function.Function;

public class ConfigTest {

    @Test
    public void parseValues() throws Exception {
        Config c = new Config();

        addVariable(c, "vstring", "value");
        addVariable(c, "vint1", 10);
        addVariable(c, "vint2", "5");
        addVariable(c, "vlong1", 200L);
        addVariable(c, "vlong2", "500");
        addVariable(c, "vdouble1", 30.0);
        addVariable(c, "vdouble2", 50);
        addVariable(c, "vdouble3", "24.5");
        addVariable(c, "vbool1", true);
        addVariable(c, "vbool2", "true");
        addVariable(c, "vbool3", "yes");
        addVariable(c, "vobj", new Object());

        assertThat(c.getString("vstring"), is("value"));
        assertThat(c.getInt("vint1"), is(10));
        assertThat(c.getInt("vint2"), is(5));
        assertThat(c.getLong("vlong1"), is(200L));
        assertThat(c.getLong("vlong2"), is(500L));
        assertThat(c.getDouble("vdouble1"), is(30.0));
        assertThat(c.getDouble("vdouble2"), is(50.0));
        assertThat(c.getDouble("vdouble3"), is(24.5));
        assertThat(c.getBoolean("vbool1"), is(true));
        assertThat(c.getBoolean("vbool2"), is(true));
        assertThat(c.getBoolean("vbool3"), is(false));

        assertInvalidType("vint1", c::getString);
        assertInvalidType("vstring", c::getInt);
        assertInvalidType("vstring", c::getLong);
        assertInvalidType("vstring", c::getDouble);
        assertInvalidType("vobj", c::getInt);
        assertInvalidType("vobj", c::getLong);
        assertInvalidType("vobj", c::getDouble);
        assertInvalidType("vint1", c::getBoolean);
    }

    private void addVariable(Config c, String name, Object value) {
        c.addVariable(ConfigVariable.newBuilder(name, "").withInitialValue(value).build());
    }

    private void assertInvalidType(String variable, Function<String, Object> action) {
        try {
            action.apply(variable);
            fail();
        } catch (IllegalArgumentException e) {
            String expectedMessage = String.format("variable \"%s\" is not", variable);
            assertThat(e.getMessage(), startsWith(expectedMessage));
        }
    }
}
