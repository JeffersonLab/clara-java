package org.jlab.clara.std.orchestrators;

class WorkerFile {

    final String inputName;
    final String outputName;

    WorkerFile(String inFile, String outFile) {
        inputName = inFile;
        outputName = outFile;
    }
}
