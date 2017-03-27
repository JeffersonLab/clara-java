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

    String inputFilePath(WorkerFile recFile) {
        return inputDir + File.separator + recFile.inputName;
    }

    String outputFilePath(WorkerFile recFile) {
        return outputDir + File.separator + recFile.outputName;
    }

    String stageInputFilePath(WorkerFile recFile) {
        return stageDir + File.separator + recFile.inputName;
    }

    String stageOutputFilePath(WorkerFile recFile) {
        return stageDir + File.separator + recFile.outputName;
    }

    int numFiles() {
        return allFiles.size();
    }
}
