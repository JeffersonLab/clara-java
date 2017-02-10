package org.jlab.clara.cli;

import org.junit.Before;
import org.junit.Test;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.jline.terminal.TerminalBuilder;

public class SetCommandTest {

    private RunConfig config;
    private SetCommand command;

    @Before
    public void setup() throws Exception {
        config = new RunConfig();
        command = new SetCommand(TerminalBuilder.builder().build(), config);
    }

    @Test
    public void testDefaultSession() throws Exception {

        assertThat(config.getSession(), is(""));
    }

    @Test
    public void testSetSession() throws Exception {

        command.execute(new String[]{"set", "session", "trevor"});

        assertThat(config.getSession(), is("trevor"));
    }

    @Test
    public void testSetYaml() throws Exception {
        command.execute(new String[]{"set", "yaml", "/home/user/services.yaml"});

        assertThat(config.getConfigFile(), is("/home/user/services.yaml"));
    }

    @Test
    public void testSetFileList() throws Exception {
        command.execute(new String[]{"set", "fileList", "/home/user/files.list"});

        assertThat(config.getFilesList(), is("/home/user/files.list"));
    }

    @Test
    public void testSetMaxThreads() throws Exception {
        command.execute(new String[]{"set", "threads", "5"});

        assertThat(config.getMaxThreads(), is(5));
    }

    @Test
    public void testSetInputDir() throws Exception {
        command.execute(new String[]{"set", "inputDir", "/home/user/inputDir"});

        assertThat(config.getInputDir(), is("/home/user/inputDir"));
    }

    @Test
    public void testSetOutputDir() throws Exception {
        command.execute(new String[]{"set", "outputDir", "/home/user/outputDir"});

        assertThat(config.getOutputDir(), is("/home/user/outputDir"));
    }
}
