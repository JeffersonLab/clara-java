package org.jlab.clara.cli;

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

    @Test(expected = IllegalArgumentException.class)
    public void setFarmFlavorFailsIfEmptyArgs() throws Exception {
        RunConfig config = new RunConfig();

        config.setFarmFlavor("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setFarmFlavorFailsIfNotPbsOrJlab() throws Exception {
        RunConfig config = new RunConfig();

        config.setFarmFlavor("hi");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setFarmLoadingZoneFailsIfEmptyArgs() throws Exception {
        RunConfig config = new RunConfig();

        config.setFarmLoadingZone("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setFarmLoadingZoneFailsIfPathNotExists() throws Exception {
        RunConfig config = new RunConfig();

        config.setFarmLoadingZone("/badpath/");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setFarmLoadingZoneFailsIfPathIsNotDirectory() throws Exception {
        RunConfig config = new RunConfig();
        File tempFile = File.createTempFile("test", "");
        tempFile.deleteOnExit();

        config.setFarmLoadingZone(tempFile.getAbsolutePath());
    }

    @Test(expected = IllegalArgumentException.class)
    public void setFarmMemoryFailsIfArgIsNotPositive() throws Exception {
        RunConfig config = new RunConfig();

        config.setFarmMemory(-2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setFarmTrackFailsIfEmptyArgs() throws Exception {
        RunConfig config = new RunConfig();

        config.setFarmTrack("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setFarmOSFailsIfEmptyArgs() throws Exception {
        RunConfig config = new RunConfig();

        config.setFarmOS("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void setFarmCPUFailsIfArgIsNotPositive() throws Exception {
        RunConfig config = new RunConfig();

        config.setFarmCPU(-2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setFarmDiskFailsIfArgIsNotPositive() throws Exception {
        RunConfig config = new RunConfig();

        config.setFarmDisk(-2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setFarmTimeFailsIfArgIsNotPositive() throws Exception {
        RunConfig config = new RunConfig();

        config.setFarmTime(-2);
    }

}
