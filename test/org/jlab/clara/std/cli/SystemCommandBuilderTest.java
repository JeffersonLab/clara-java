package org.jlab.clara.std.cli;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Before;
import org.junit.Test;

public class SystemCommandBuilderTest {

    private SystemCommandBuilder b;

    @Before
    public void setUp() {
        b = new SystemCommandBuilder("${CLARA_HOME}/bin/clara-orchestrator");
        b.addOption("-t", 10);
        b.addOption("-i", "$CLAS12DIR/exp/input");
        b.addArgument("custom services.yml");
        b.addArgument("data/files.txt");
    }

    @Test
    public void outputArrayDoesNotNeedQuotes() throws Exception {
        assertThat(b.toArray(), is(new String[]{
                "${CLARA_HOME}/bin/clara-orchestrator",
                "-t", "10",
                "-i", "$CLAS12DIR/exp/input",
                "custom services.yml",
                "data/files.txt"
                }));
    }

    @Test
    public void outputStringNeedsQuotes() throws Exception {
        assertThat(b.toString(), is(
                "\"${CLARA_HOME}/bin/clara-orchestrator\""
                + " -t 10"
                + " -i \"$CLAS12DIR/exp/input\""
                + " \"custom services.yml\""
                + " data/files.txt"));
    }

    @Test
    public void outputStringCanQuoteEverything() throws Exception {
        b.quoteAll(true);

        assertThat(b.toString(), is(
                "\"${CLARA_HOME}/bin/clara-orchestrator\""
                + " \"-t\" \"10\""
                + " \"-i\" \"$CLAS12DIR/exp/input\""
                + " \"custom services.yml\""
                + " \"data/files.txt\""));
    }
}
