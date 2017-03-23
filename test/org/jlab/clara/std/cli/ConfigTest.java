package org.jlab.clara.std.cli;

import java.io.File;

import org.junit.Test;

public class ConfigTest {

    @Test(expected = IllegalArgumentException.class)
    public void setConfigFileFailsIfEmptyArg() throws Exception {
        Config config = new Config();

        config.setConfigFile("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setConfigFileFailsIfPathNotExist() throws Exception {
        Config config = new Config();

        config.setConfigFile("/tmp/notafile.txt");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setConfigFileFailsIfPathIsNotRegularFile() throws Exception {
        Config config = new Config();

        config.setConfigFile("/tmp/");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setFileListFailsIfEmptyPath() throws Exception {
        Config config = new Config();

        config.setFilesList("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setFileListFailsIfPathNotExist() throws Exception {
        Config config = new Config();

        config.setFilesList("/tmp/notafile.txt");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setFileListFailsIfPathIsNotRegularFile() throws Exception {
        Config config = new Config();

        config.setFilesList("/tmp/");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setInputDirFailsIfEmptyArg() throws Exception {
        Config config = new Config();

        config.setInputDir("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setInputDirFailsIfPathNotExists() throws Exception {
        Config config = new Config();

        config.setInputDir("/badpath/");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setInputDirFailsIfPathIsNotDirectory() throws Exception {
        Config config = new Config();
        File tempFile = File.createTempFile("test", "");
        tempFile.deleteOnExit();

        config.setInputDir(tempFile.getAbsolutePath());
    }

    @Test(expected = IllegalArgumentException.class)
    public void setOutputDirFailsIfEmptyArg() throws Exception {
        Config config = new Config();

        config.setOutputDir("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setOutputDirFailsIfPathNotExists() throws Exception {
        Config config = new Config();

        config.setOutputDir("/badpath/");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setOutputDirFailsIfPathIsNotDirectory() throws Exception {
        Config config = new Config();
        File tempFile = File.createTempFile("test", "");
        tempFile.deleteOnExit();

        config.setOutputDir(tempFile.getAbsolutePath());
    }

    @Test(expected = IllegalArgumentException.class)
    public void setMaxThreadsFailsIfArgIsNotPositive() throws Exception {
        Config config = new Config();

        config.setMaxThreads(-2);
    }
}
