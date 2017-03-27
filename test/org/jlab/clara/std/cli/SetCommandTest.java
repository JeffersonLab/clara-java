package org.jlab.clara.std.cli;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import org.jline.terminal.Terminal;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class SetCommandTest {

    private static final Terminal TERMINAL = mock(Terminal.class);

    private RunConfig config;
    private SetCommand command;

    @Before
    public void setup() throws Exception {
        config = new RunConfig();
        command = new SetCommand(TERMINAL, config);
    }

    @Test
    public void testDefaultSession() throws Exception {

        assertThat(config.getSession(), is(""));
    }

    @Test
    public void testSetSession() throws Exception {

        command.execute(new String[]{"session", "trevor"});

        assertThat(config.getSession(), is("trevor"));
    }

    @Test
    public void testSetServicesFile() throws Exception {
        String userFile = createTempFile("yaml");

        command.execute(new String[]{"servicesFile", userFile});

        assertThat(config.getConfigFile(), is(userFile));
    }

    @Test
    public void testSetFileList() throws Exception {
        String userFile = createTempFile("fileList");

        command.execute(new String[]{"fileList", userFile});

        assertThat(config.getFilesList(), is(userFile));
    }

    @Test
    public void testSetMaxThreads() throws Exception {
        command.execute(new String[]{"threads", "5"});

        assertThat(config.getMaxThreads(), is(5));
    }

    @Test
    public void testSetInputDir() throws Exception {
        String userDir = createTempDir("input");

        command.execute(new String[]{"inputDir", userDir});

        assertThat(config.getInputDir(), is(userDir));
    }

    @Test
    public void testSetOutputDir() throws Exception {
        String userDir = createTempDir("output");

        command.execute(new String[]{"outputDir", userDir});

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
