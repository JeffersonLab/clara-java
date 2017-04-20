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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class OrchestratorPaths {

    static final String DATA_DIR = System.getenv("CLARA_HOME") + File.separator + "data";
    static final String INPUT_DIR = DATA_DIR + File.separator + "in";
    static final String OUTPUT_DIR = DATA_DIR + File.separator + "out";
    static final String STAGE_DIR = File.separator + "scratch";

    final Path inputDir;
    final Path outputDir;
    final Path stageDir;

    final List<WorkerFile> allFiles;

    OrchestratorPaths(String inputFile, String outputFile) {
        Path inputPath = Paths.get(inputFile);
        Path outputPath = Paths.get(outputFile);

        String inputName = FileUtils.getFileName(inputPath).toString();
        String outputName = FileUtils.getFileName(outputPath).toString();

        this.inputDir = FileUtils.getParent(inputPath).toAbsolutePath();
        this.outputDir = FileUtils.getParent(outputPath).toAbsolutePath();
        this.stageDir = Paths.get(STAGE_DIR).toAbsolutePath();

        this.allFiles = Arrays.asList(new WorkerFile(inputName, outputName));
    }

    OrchestratorPaths(List<String> inputFiles,
                      String inputDir, String outputDir, String stageDir) {
        this.inputDir = Paths.get(inputDir).toAbsolutePath();
        this.outputDir = Paths.get(outputDir).toAbsolutePath();
        this.stageDir = Paths.get(stageDir).toAbsolutePath();
        this.allFiles = inputFiles.stream()
                                  .map(f -> new WorkerFile(f, "out_" + f))
                                  .collect(Collectors.toList());
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
