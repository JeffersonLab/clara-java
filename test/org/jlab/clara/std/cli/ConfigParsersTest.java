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
        assertThat(ConfigParsers.toStringOrEmpty(), is(""));
    }

    @Test
    public void parseStringOrEmptySucceedsIfEmptyArg() throws Exception {
        assertThat(ConfigParsers.toStringOrEmpty(""), is(""));
    }


    @Test
    public void parseAlphaNumWordSucceedsWithArg() throws Exception {
        assertThat(ConfigParsers.toAlphaNumWord("test_string"), is("test_string"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseAlphaNumWordFailsIfArgContainsSpaces() throws Exception {
        ConfigParsers.toAlphaNumWord("test string");
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseAlphaNumWordFailsIfEmptyArg() throws Exception {
        ConfigParsers.toAlphaNumWord("");
    }

    @Test
    public void parseAlphaNumWordOrEmptySucceedsWithArg() throws Exception {
        assertThat(ConfigParsers.toAlphaNumWordOrEmpty("test_string"), is("test_string"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseAlphaNumWordOrEmptyFailsIfArgContainsSpaces() throws Exception {
        ConfigParsers.toAlphaNumWordOrEmpty("test string");
    }

    @Test
    public void parseAlphaNumWordOrEmptySucceedsIfNoArgs() throws Exception {
        assertThat(ConfigParsers.toAlphaNumWordOrEmpty(), is(""));
    }

    @Test
    public void parseAlphaNumWordOrEmptySucceedsIfEmptyArg() throws Exception {
        assertThat(ConfigParsers.toAlphaNumWordOrEmpty(""), is(""));
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
