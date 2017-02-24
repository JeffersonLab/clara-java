package org.jlab.clara.std.orchestrators;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class OrchestratorPaths {

    static final String DATA_DIR = System.getenv("CLARA_HOME") + File.separator + "data";
    static final String CACHE_DIR = "/mss/hallb/exp/raw";
    static final String INPUT_DIR = DATA_DIR + File.separator + "in";
    static final String OUTPUT_DIR = DATA_DIR + File.separator + "out";
    static final String STAGE_DIR = File.separator + "scratch";

    final String inputDir;
    final String outputDir;
    final String stageDir;

    final List<WorkerFile> allFiles;

    OrchestratorPaths(String inputFile, String outputFile) {
        Path inputPath = Paths.get(inputFile);
        Path outputPath = Paths.get(outputFile);

        String inputName = inputPath.getFileName().toString();
        String outputName = outputPath.getFileName().toString();

        this.inputDir = getParent(inputPath);
        this.outputDir = getParent(outputPath);
        this.stageDir = STAGE_DIR;

        this.allFiles = Arrays.asList(new WorkerFile(inputName, outputName));
    }

    OrchestratorPaths(List<String> inputFiles,
                      String inputDir, String outputDir, String stageDir) {
        this.inputDir = inputDir;
        this.outputDir = outputDir;
        this.stageDir = stageDir;
        this.allFiles = inputFiles.stream()
                                  .map(f -> new WorkerFile(f, "out_" + f))
                                  .collect(Collectors.toList());
    }

    private String getParent(Path file) {
        Path parent = file.getParent();
        if (parent == null) {
            return Paths.get("").toAbsolutePath().toString();
        } else {
            return parent.toString();
        }
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
