package org.jlab.clara.std.cli;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class ConfigParsersTest {

    @Test
    public void parseStringSucceedsWithArg() throws Exception {
        assertThat(ConfigParsers.toStringOrEmpty("test_string"), is("test_string"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseStringFailsIfEmptyArg() throws Exception {
        ConfigParsers.toString("");
    }

    @Test
    public void parseStringOrEmptySucceedsWithArg() throws Exception {
        assertThat(ConfigParsers.toStringOrEmpty("test_string"), is("test_string"));
    }

    @Test
    public void parseStringOrEmptySucceedsIfNoArgs() throws Exception {
        assertThat(ConfigParsers.toStringOrEmpty(""), is(""));
    }

    @Test
    public void parseStringOrEmptySucceedsIfEmptyArg() throws Exception {
        assertThat(ConfigParsers.toStringOrEmpty(""), is(""));
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseFileFailsIfEmptyArg() throws Exception {
        ConfigParsers.toFile("");
    }

    public void parseFileSucceedsIfPathNotExist() throws Exception {
        assertThat(ConfigParsers.toFile("/tmp/notafile.txt"), is("/tmp/notafile.txt"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseFileFailsIfPathIsNotRegularFile() throws Exception {
        ConfigParsers.toExistingFile("/tmp");
    }


    @Test(expected = IllegalArgumentException.class)
    public void parseExistingFileFailsIfEmptyArg() throws Exception {
        ConfigParsers.toExistingFile("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseExistingFileFailsIfPathNotExist() throws Exception {
        ConfigParsers.toExistingFile("/tmp/notafile.txt");
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseExistingFileFailsIfPathIsNotRegularFile() throws Exception {
        ConfigParsers.toExistingFile("/tmp");
    }


    @Test(expected = IllegalArgumentException.class)
    public void parseDirectoryFailsIfEmptyArg() throws Exception {
        ConfigParsers.toDirectory("");
    }

    public void parseDirectorySucceedsIfPathNotExist() throws Exception {
        assertThat(ConfigParsers.toDirectory("/tmp/missingdir/"), is("/tmp/missingdir/"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseDirectoryFailsIfPathIsNotDirectory() throws Exception {
        ConfigParsers.toExistingDirectory("/tmp/notadir");
    }


    @Test(expected = IllegalArgumentException.class)
    public void parseExistingDirectoryFailsIfEmptyArg() throws Exception {
        ConfigParsers.toExistingDirectory("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseExistingDirectoryFailsIfPathNotExist() throws Exception {
        ConfigParsers.toExistingDirectory("/tmp/notafile.txt");
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseExistingDirectoryFailsIfPathIsNotDirectory() throws Exception {
        ConfigParsers.toExistingDirectory("/tmp/notadir");
    }
}
