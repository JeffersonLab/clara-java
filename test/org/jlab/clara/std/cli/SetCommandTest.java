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

    private Config config;
    private SetCommand command;

    @Before
    public void setup() throws Exception {
        config = new Config();
        command = new SetCommand(TERMINAL, config);
    }

    @Test
    public void testDefaultSession() throws Exception {

        assertThat(config.getValue(Config.SESSION), is(""));
    }

    @Test
    public void testSetSession() throws Exception {

        command.execute(new String[]{"session", "trevor"});

        assertThat(config.getValue(Config.SESSION), is("trevor"));
    }

    @Test
    public void testSetServicesFile() throws Exception {
        String userFile = createTempFile("yaml");

        command.execute(new String[]{"servicesFile", userFile});

        assertThat(config.getValue(Config.SERVICES_FILE), is(userFile));
    }

    @Test
    public void testSetFileList() throws Exception {
        String userFile = createTempFile("fileList");

        command.execute(new String[]{"fileList", userFile});

        assertThat(config.getValue(Config.FILES_LIST), is(userFile));
    }

    @Test
    public void testSetMaxThreads() throws Exception {
        command.execute(new String[]{"threads", "5"});

        assertThat((Integer) config.getValue(Config.MAX_THREADS), is(5));
    }

    @Test
    public void testSetInputDir() throws Exception {
        String userDir = createTempDir("input");

        command.execute(new String[]{"inputDir", userDir});

        assertThat(config.getValue(Config.INPUT_DIR), is(userDir));
    }

    @Test
    public void testSetOutputDir() throws Exception {
        String userDir = createTempDir("output");

        command.execute(new String[]{"outputDir", userDir});

        assertThat(config.getValue(Config.OUTPUT_DIR), is(userDir));
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
