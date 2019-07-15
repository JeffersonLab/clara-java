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
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class ConfigParsersTest {

    @Test
    public void parseStringSucceedsWithArg() throws Exception {
        assertThat(ConfigParsers.toStringOrEmpty("test_string"), is("test_string"));
    }

    @Test
    public void parseStringFailsIfEmptyArg() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> ConfigParsers.toString(""));
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
    public void parseAlphaNumSucceedsWithArg() throws Exception {
        assertThat(ConfigParsers.toAlphaNum("string01"), is("string01"));
    }

    @Test
    public void parseAlphaNumFailsIfArgContainsSpaces() throws Exception {
        assertThrows(IllegalArgumentException.class, () ->
                ConfigParsers.toAlphaNum("test string"));
    }

    @Test
    public void parseAlphaNumFailsIfEmptyArg() throws Exception {
        assertThrows(IllegalArgumentException.class, () ->
                ConfigParsers.toAlphaNum(""));
    }

    @Test
    public void parseAlphaNumOrEmptySucceedsWithArg() throws Exception {
        assertThat(ConfigParsers.toAlphaNumOrEmpty("string01"), is("string01"));
    }

    @Test
    public void parseAlphaNumOrEmptyFailsIfArgContainsSpaces() throws Exception {
        assertThrows(IllegalArgumentException.class, () ->
                ConfigParsers.toAlphaNumOrEmpty("test string"));
    }

    @Test
    public void parseAlphaNumOrEmptySucceedsIfNoArgs() throws Exception {
        assertThat(ConfigParsers.toAlphaNumOrEmpty(), is(""));
    }

    @Test
    public void parseAlphaNumOrEmptySucceedsIfEmptyArg() throws Exception {
        assertThat(ConfigParsers.toAlphaNumOrEmpty(""), is(""));
    }


    @Test
    public void parseNonBlankSucceedsWithArg() throws Exception {
        assertThat(ConfigParsers.toNonWhitespace("string-0.1"), is("string-0.1"));
    }

    @Test
    public void parseNonBlankFailsIfArgContainsSpaces() throws Exception {
        assertThrows(IllegalArgumentException.class, () ->
                ConfigParsers.toNonWhitespace("test string"));
    }

    @Test
    public void parseNonBlankFailsIfEmptyArg() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> ConfigParsers.toNonWhitespace(""));
    }


    @Test
    public void parseFileFailsIfEmptyArg() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> ConfigParsers.toFile(""));
    }

    public void parseFileSucceedsIfPathNotExist() throws Exception {
        assertThat(ConfigParsers.toFile("/tmp/notafile.txt"), is("/tmp/notafile.txt"));
    }

    @Test
    public void parseFileFailsIfPathIsNotRegularFile() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> ConfigParsers.toExistingFile("/tmp"));
    }


    @Test
    public void parseExistingFileFailsIfEmptyArg() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> ConfigParsers.toExistingFile(""));
    }

    @Test
    public void parseExistingFileFailsIfPathNotExist() throws Exception {
        assertThrows(IllegalArgumentException.class, () ->
                ConfigParsers.toExistingFile("/tmp/notafile.txt"));
    }

    @Test
    public void parseExistingFileFailsIfPathIsNotRegularFile() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> ConfigParsers.toExistingFile("/tmp"));
    }


    @Test
    public void parseDirectoryFailsIfEmptyArg() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> ConfigParsers.toDirectory(""));
    }

    public void parseDirectorySucceedsIfPathNotExist() throws Exception {
        assertThat(ConfigParsers.toDirectory("/tmp/missingdir/"), is("/tmp/missingdir/"));
    }

    @Test
    public void parseDirectoryFailsIfPathIsNotDirectory() throws Exception {
        assertThrows(IllegalArgumentException.class, () ->
                ConfigParsers.toExistingDirectory("/tmp/notadir"));
    }


    @Test
    public void parseExistingDirectoryFailsIfEmptyArg() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> ConfigParsers.toExistingDirectory(""));
    }

    @Test
    public void parseExistingDirectoryFailsIfPathNotExist() throws Exception {
        assertThrows(IllegalArgumentException.class, () ->
                ConfigParsers.toExistingDirectory("/tmp/notafile.txt"));
    }

    @Test
    public void parseExistingDirectoryFailsIfPathIsNotDirectory() throws Exception {
        assertThrows(IllegalArgumentException.class, () ->
                ConfigParsers.toExistingDirectory("/tmp/notadir"));
    }
}
