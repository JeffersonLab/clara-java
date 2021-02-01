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

package org.jlab.clara.std.orchestrators;

import org.jlab.clara.util.FileUtils;

import java.io.File;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OrchestratorPaths {

    static final public String INPUT_DIR = FileUtils.claraPath("data", "input").toString();
    static final public String OUTPUT_DIR = FileUtils.claraPath("data", "output").toString();
    static final public String STAGE_DIR = File.separator + "scratch";
    static final public String OUTPUT_FILE_PREFIX = "out_";

    final public List<WorkerFile> allFiles;

    final public Path inputDir;
    final public Path outputDir;
    final public Path stageDir;
    final public String prefix;

    public static class Builder {

        private List<WorkerFile> allFiles;
        private Stream<String> afStream = null;


        private Path inputDir = Paths.get(INPUT_DIR);
        private Path outputDir = Paths.get(OUTPUT_DIR);
        private Path stageDir = Paths.get(STAGE_DIR);
        private String prefix = OUTPUT_FILE_PREFIX;


        public Builder(String inputFile, String outputFile) {
            Path inputPath = Paths.get(inputFile);
            Path outputPath = Paths.get(outputFile);

            String inputName = FileUtils.getFileName(inputPath).toString();
            String outputName = FileUtils.getFileName(outputPath).toString();

            this.allFiles = Arrays.asList(new WorkerFile(inputName, outputName));
            this.inputDir = FileUtils.getParent(inputPath).toAbsolutePath().normalize();
            this.outputDir = FileUtils.getParent(outputPath).toAbsolutePath().normalize();
        }

        public Builder(List<String> inputFiles) {
            afStream = inputFiles.stream();
            this.allFiles = inputFiles.stream()
                    .peek(f -> checkValidFileName(f))
                    .map(f -> new WorkerFile(f, prefix + f))
                    .collect(Collectors.toList());
        }

        public Builder withInputDir(String inputDir) {
            this.inputDir = Paths.get(inputDir).toAbsolutePath().normalize();
            return this;
        }

        public Builder withOutputDir(String outputDir) {
            this.outputDir = Paths.get(outputDir).toAbsolutePath().normalize();
            return this;
        }

        public Builder withOutputFilePrefix(String prefix) {
            this.prefix = prefix;
            this.allFiles = afStream
                    .peek(f -> checkValidFileName(f))
                    .map(f -> new WorkerFile(f, prefix + f))
                    .collect(Collectors.toList());
            return this;
        }

        public Builder withStageDir(String stageDir) {
            this.stageDir = Paths.get(stageDir).toAbsolutePath().normalize();
            return this;
        }

        public OrchestratorPaths build() {
            return new OrchestratorPaths(this);
        }

        private static void checkValidFileName(String file) {
            try {
                if (Paths.get(file).getParent() != null) {
                    throw new OrchestratorConfigException("Input file cannot be a path: " + file);
                }
            } catch (InvalidPathException e) {
                throw new OrchestratorConfigException(e);
            }
        }
    }


    protected OrchestratorPaths(Builder builder) {
        this.allFiles = builder.allFiles;
        this.inputDir = builder.inputDir;
        this.outputDir = builder.outputDir;
        this.stageDir = builder.stageDir;
        this.prefix = builder.prefix;
    }

    Path inputFilePath(WorkerFile recFile) {
        return inputDir.resolve(recFile.inputName);
    }

    Path outputFilePath(WorkerFile recFile) {
        return outputDir.resolve(recFile.outputName);
    }

    Path stageInputFilePath(WorkerFile recFile) {
        return stageDir.resolve(recFile.inputName);
    }

    Path stageOutputFilePath(WorkerFile recFile) {
        return stageDir.resolve(recFile.outputName);
    }

    int numFiles() {
        return allFiles.size();
    }
}
