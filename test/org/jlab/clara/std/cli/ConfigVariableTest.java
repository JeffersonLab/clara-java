package org.jlab.clara.std.cli;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import org.jline.reader.Candidate;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.StringsCompleter;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class ConfigVariableTest {

    @Test
    public void buildWithDefaultParameters() throws Exception {
        ConfigVariable v = ConfigVariable.newBuilder("var", "test variable").build();

        assertThat(v.getName(), is("var"));
        assertThat(v.getDescription(), is("test variable"));
        assertThat(v.hasValue(), is(false));
    }

    @Test
    public void buildWithInitialValue() throws Exception {
        Stream.of("value", 12, 12.8, false).forEach(o -> {
            ConfigVariable v = ConfigVariable.newBuilder("var", "test variable")
                    .withInitialValue(o)
                    .build();

            assertThat(v.hasValue(), is(true));
            assertThat(v.getValue(), is(o));
        });
    }

    @Test
    public void buildWithDefaultParser() throws Exception {
        ConfigVariable v = ConfigVariable.newBuilder("var", "test variable").build();

        v.parseValue("value");

        assertThat(v.getValue(), is("value"));
    }

    @Test
    public void buildWithCustomParser() throws Exception {
        ConfigVariable v = ConfigVariable.newBuilder("var", "test variable")
                .withParser(ConfigParsers::toPositiveInteger)
                .build();

        v.parseValue("187");

        assertThat(v.getValue(), is(187));
    }

    @Test
    public void buildWithExpectedStringValues() throws Exception {
        ConfigVariable v = ConfigVariable.newBuilder("var", "a test variable")
                .withExpectedValues("hello", "hola", "bonjour")
                .build();

        assertCandidates(v, "hello", "hola", "bonjour");
    }

    @Test
    public void buildWithExpectedObjectValues() throws Exception {
        ConfigVariable v = ConfigVariable.newBuilder("var", "test variable")
                .withExpectedValues(4, 8, 15)
                .build();

        assertCandidates(v, "4", "8", "15");
    }

    @Test
    public void buildWithDefaultCompleter() throws Exception {
        ConfigVariable v = ConfigVariable.newBuilder("var", "test variable").build();

        assertCandidates(v);
    }

    @Test
    public void buildWithCustomCompleter() throws Exception {
        ConfigVariable v = ConfigVariable.newBuilder("var", "test variable")
                .withCompleter(new StringsCompleter("one", "two"))
                .build();

        assertCandidates(v, "one", "two");
    }

    private static void assertCandidates(ConfigVariable variable, String... expected) {
        List<Candidate> candidates = new ArrayList<>();

        variable.getCompleter()
                .complete(mock(LineReader.class), mock(ParsedLine.class), candidates);

        assertThat(candidates.stream().map(Candidate::value).toArray(String[]::new),
                   is(expected));
    }
}
