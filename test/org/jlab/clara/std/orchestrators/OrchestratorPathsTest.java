package org.jlab.clara.std.orchestrators;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.jlab.clara.util.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class OrchestratorPathsTest {

    private static final List<String> SIMPLE_LIST = Arrays.asList("in.ev");

    @Before
    public void setUp() throws Exception {
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void singleFileIsUsedAsInputFile() throws Exception {
        OrchestratorPaths paths = new OrchestratorPaths.Builder("input.ev", "out.ev").build();

        assertThat(inputFiles(paths), is(Arrays.asList("input.ev")));
        assertThat(paths.inputDir, is(Paths.get("").toAbsolutePath()));
        assertThat(paths.numFiles(), is(1));
    }

    @Test
    public void singleFileIsUsedAsOutputFile() throws Exception {
        OrchestratorPaths paths = new OrchestratorPaths.Builder("input.ev", "out.ev").build();

        assertThat(outputFiles(paths), is(Arrays.asList("out.ev")));
        assertThat(paths.outputDir, is(Paths.get("").toAbsolutePath()));
    }

    @Test
    public void singleFileCanContainPath() throws Exception {
        OrchestratorPaths paths = new OrchestratorPaths.Builder("/a/b/in.ev", "/t/out.ev").build();

        assertThat(inputFiles(paths), is(Arrays.asList("in.ev")));
        assertThat(paths.inputDir.toString(), is("/a/b"));
        assertThat(paths.outputDir.toString(), is("/t"));
    }

    @Test
    public void fileListIsUsedAsInputFiles() throws Exception {
        List<String> files = Arrays.asList("f1.ev", "f2.ev", "f3.ev");

        OrchestratorPaths paths = new OrchestratorPaths.Builder(files).build();

        assertThat(inputFiles(paths), is(files));
        assertThat(paths.numFiles(), is(3));
    }

    @Test
    public void fileListIsUsedAsOutputFiles() throws Exception {
        List<String> files = Arrays.asList("c1.evio", "c2.evio");

        OrchestratorPaths paths = new OrchestratorPaths.Builder(files).build();

        assertThat(outputFiles(paths), is(Arrays.asList("out_c1.evio", "out_c2.evio")));
    }

    @Test
    public void fileListShallNotContainFullPaths() throws Exception {
        List<String> files = Arrays.asList("/a/b/c1.evio", "/a/b/c2.evio");

        thrown.expect(OrchestratorConfigException.class);
        thrown.expectMessage("Input file cannot be a path: /a/b/c1.evio");
        new OrchestratorPaths.Builder(files);
    }

    @Test
    public void fileListShallNotContainRelavitePaths() throws Exception {
        List<String> files = Arrays.asList("c1.evio", "b/c2.evio");

        thrown.expect(OrchestratorConfigException.class);
        thrown.expectMessage("Input file cannot be a path: b/c2.evio");
        new OrchestratorPaths.Builder(files);
    }

    @Test
    public void defaultInputDirIsInClaraHome() throws Exception {
        OrchestratorPaths paths = new OrchestratorPaths.Builder(SIMPLE_LIST).build();

        assertThat(paths.inputDir, is(FileUtils.claraPath("data", "input")));
    }

    @Test
    public void defaultOutputDirIsInClaraHome() throws Exception {
        OrchestratorPaths paths = new OrchestratorPaths.Builder(SIMPLE_LIST).build();

        assertThat(paths.outputDir, is(FileUtils.claraPath("data", "output")));
    }

    @Test
    public void defaultStageDirIsInScratch() throws Exception {
        OrchestratorPaths paths = new OrchestratorPaths.Builder(SIMPLE_LIST).build();

        assertThat(paths.stageDir, is(Paths.get("/scratch")));
    }

    @Test
    public void inputDirCanBeUserDefined() throws Exception {
        OrchestratorPaths paths = new OrchestratorPaths.Builder(SIMPLE_LIST)
                .withInputDir("/data/files/input")
                .build();

        assertThat(paths.inputDir, is(Paths.get("/data/files/input")));
    }

    @Test
    public void outputDirCanBeUserDefined() throws Exception {
        OrchestratorPaths paths = new OrchestratorPaths.Builder(SIMPLE_LIST)
                .withOutputDir("/data/files/output")
                .build();

        assertThat(paths.outputDir, is(Paths.get("/data/files/output")));
    }

    @Test
    public void stageDirCanBeUserDefined() throws Exception {
        OrchestratorPaths paths = new OrchestratorPaths.Builder(SIMPLE_LIST)
                .withStageDir("/tmp/clara/stage")
                .build();

        assertThat(paths.stageDir, is(Paths.get("/tmp/clara/stage")));
    }

    private static List<String> inputFiles(OrchestratorPaths paths) {
        return paths.allFiles.stream().map(f -> f.inputName).collect(Collectors.toList());
    }

    private static List<String> outputFiles(OrchestratorPaths paths) {
        return paths.allFiles.stream().map(f -> f.outputName).collect(Collectors.toList());
    }
}
