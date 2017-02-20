package org.jlab.clara.cli;

import org.junit.Before;
import org.junit.Test;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

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
        String userFile = createTempFile("yaml");

        command.execute(new String[]{"set", "yaml", userFile});

        assertThat(config.getConfigFile(), is(userFile));
    }

    @Test
    public void testSetFileList() throws Exception {
        String userFile = createTempFile("fileList");

        command.execute(new String[]{"set", "fileList", userFile});

        assertThat(config.getFilesList(), is(userFile));
    }

    @Test
    public void testSetMaxThreads() throws Exception {
        command.execute(new String[]{"set", "threads", "5"});

        assertThat(config.getMaxThreads(), is(5));
    }

    @Test
    public void testSetInputDir() throws Exception {
        String userDir = createTempDir("input");

        command.execute(new String[]{"set", "inputDir", userDir});

        assertThat(config.getInputDir(), is(userDir));
    }

    @Test
    public void testSetOutputDir() throws Exception {
        String userDir = createTempDir("output");

        command.execute(new String[]{"set", "outputDir", userDir});

        assertThat(config.getOutputDir(), is(userDir));
    }

    private static String createTempDir(String prefix) throws IOException {
        Path tmpDir = Files.createTempDirectory(prefix);
        tmpDir.toFile().deleteOnExit();
        return tmpDir.toString();
    }

    private static String createTempFile(String prefix) throws IOException {
        File tmpFile = File.createTempFile(prefix, "");
        tmpFile.deleteOnExit();
        return tmpFile.toString();
    }
}
