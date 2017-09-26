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

package org.jlab.clara.util;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public final class FileUtils {

    private FileUtils() { }

    public static String expandHome(String path) {
        String home = System.getProperty("user.home");
        if (path.startsWith("~")) {
            return path.replace("~", home);
        } else if (path.startsWith("$HOME")) {
            return path.replace("$HOME", home);
        }
        return path;
    }

    public static Path getFileName(Path path) {
        Path fileName = path.getFileName();
        if (fileName == null) {
            throw new IllegalArgumentException("empty path");
        }
        return fileName;
    }

    public static Path getParent(Path path) {
        Path parent = path.getParent();
        if (parent == null) {
            return Paths.get(".");
        }
        return parent;
    }

    public static void createDirectories(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            Files.createDirectories(dir);
        }
    }

    public static void deleteFileTree(Path dir) throws IOException {
        try {
            Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                            throws IOException {
                        Files.deleteIfExists(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException ex)
                            throws IOException {
                        if (ex == null) {
                            Files.deleteIfExists(dir);
                            return FileVisitResult.CONTINUE;
                        } else if (ex instanceof NoSuchFileException) {
                            return FileVisitResult.CONTINUE;
                        }
                        throw ex;
                    }
            });
        } catch (NoSuchFileException e) {
            // ignore
        }
    }

    public static PrintWriter openOutputTextFile(Path path, boolean append) throws IOException {
        return new PrintWriter(new BufferedWriter(new OutputStreamWriter(
              new FileOutputStream(path.toFile(), append), "utf-8")));
    }
}
