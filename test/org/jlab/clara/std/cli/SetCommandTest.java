/*
 *   Copyright (c) 2017.  Jefferson Lab (JLab). All rights reserved. Permission
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
        command = new SetCommand(new Context(TERMINAL, config));
    }

    @Test
    public void testDefaultSession() throws Exception {
        assertThat(config.getString(Config.SESSION), is(Config.user()));
    }

    @Test
    public void testSetSession() throws Exception {
        command.execute(new String[]{"session", "trevor"});

        assertThat(config.getString(Config.SESSION), is("trevor"));
    }

    @Test
    public void testSetServicesFile() throws Exception {
        String userFile = createTempFile("yaml");

        command.execute(new String[]{"servicesFile", userFile});

        assertThat(config.getString(Config.SERVICES_FILE), is(userFile));
    }

    @Test
    public void testSetFileList() throws Exception {
        String userFile = createTempFile("fileList");

        command.execute(new String[]{"fileList", userFile});

        assertThat(config.getString(Config.FILES_LIST), is(userFile));
    }

    @Test
    public void testSetMaxThreads() throws Exception {
        command.execute(new String[]{"threads", "5"});

        assertThat(config.getInt(Config.MAX_THREADS), is(5));
    }

    @Test
    public void testSetInputDir() throws Exception {
        String userDir = createTempDir("input");

        command.execute(new String[]{"inputDir", userDir});

        assertThat(config.getString(Config.INPUT_DIR), is(userDir));
    }

    @Test
    public void testSetOutputDir() throws Exception {
        String userDir = createTempDir("output");

        command.execute(new String[]{"outputDir", userDir});

        assertThat(config.getString(Config.OUTPUT_DIR), is(userDir));
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
