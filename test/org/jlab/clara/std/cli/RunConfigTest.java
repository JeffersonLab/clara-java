package org.jlab.clara.std.cli;

import java.io.File;

import org.junit.Test;

public class RunConfigTest {

    @Test(expected = IllegalArgumentException.class)
    public void setConfigFileFailsIfEmptyArg() throws Exception {
        RunConfig config = new RunConfig();

        config.setConfigFile("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setConfigFileFailsIfPathNotExist() throws Exception {
        RunConfig config = new RunConfig();

        config.setConfigFile("/tmp/notafile.txt");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setConfigFileFailsIfPathIsNotRegularFile() throws Exception {
        RunConfig config = new RunConfig();

        config.setConfigFile("/tmp/");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setFileListFailsIfEmptyPath() throws Exception {
        RunConfig config = new RunConfig();

        config.setFilesList("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setFileListFailsIfPathNotExist() throws Exception {
        RunConfig config = new RunConfig();

        config.setFilesList("/tmp/notafile.txt");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setFileListFailsIfPathIsNotRegularFile() throws Exception {
        RunConfig config = new RunConfig();

        config.setFilesList("/tmp/");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setInputDirFailsIfEmptyArg() throws Exception {
        RunConfig config = new RunConfig();

        config.setInputDir("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setInputDirFailsIfPathNotExists() throws Exception {
        RunConfig config = new RunConfig();

        config.setInputDir("/badpath/");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setInputDirFailsIfPathIsNotDirectory() throws Exception {
        RunConfig config = new RunConfig();
        File tempFile = File.createTempFile("test", "");
        tempFile.deleteOnExit();

        config.setInputDir(tempFile.getAbsolutePath());
    }

    @Test(expected = IllegalArgumentException.class)
    public void setOutputDirFailsIfEmptyArg() throws Exception {
        RunConfig config = new RunConfig();

        config.setOutputDir("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setOutputDirFailsIfPathNotExists() throws Exception {
        RunConfig config = new RunConfig();

        config.setOutputDir("/badpath/");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setOutputDirFailsIfPathIsNotDirectory() throws Exception {
        RunConfig config = new RunConfig();
        File tempFile = File.createTempFile("test", "");
        tempFile.deleteOnExit();

        config.setOutputDir(tempFile.getAbsolutePath());
    }

    @Test(expected = IllegalArgumentException.class)
    public void setMaxThreadsFailsIfArgIsNotPositive() throws Exception {
        RunConfig config = new RunConfig();

        config.setMaxThreads(-2);
    }
}
