package org.jlab.clara.std.orchestrators;

import org.jlab.clara.base.DpeName;
import org.jlab.clara.std.orchestrators.GenericOrchestrator.DpeReportCB;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GenericOrchestratorTest {

    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(3);
    private static final DpeInfo FRONT_END = AppData.dpe("10.1.1.254_java");

    private CoreOrchestrator orchestrator;


    @Before
    public void setup() {
        orchestrator = mock(CoreOrchestrator.class);
        when(orchestrator.getFrontEnd()).thenReturn(FRONT_END.name);
    }


    @Test
    public void singleLangUseSingleNode() throws Exception {
        NodeData data = singleLangData();
        DpeReportCBTest cb = data.callback(false, 10);

        cb.callback("10.1.1.1_java");
        cb.callback("10.1.1.1_java");
        cb.callback("10.1.1.1_java");

        assertThat(cb.nodes(), contains(data.nodes("10.1.1.1")));
    }


    @Test
    public void singleLangUseMultipleNodes() throws Exception {
        NodeData data = singleLangData();
        DpeReportCBTest cb = data.callback(false, 10);

        cb.callback("10.1.1.1_java");
        cb.callback("10.1.1.3_java");
        cb.callback("10.1.1.2_java");

        cb.callback("10.1.1.3_java");
        cb.callback("10.1.1.2_java");
        cb.callback("10.1.1.1_java");

        WorkerNode[] expected = data.nodes("10.1.1.1", "10.1.1.2", "10.1.1.3");

        assertThat(cb.nodes(), containsInAnyOrder(expected));
    }


    @Test
    public void singleLangSingleNodeUsingFrontEnd() throws Exception {
        NodeData data = singleLangData();
        DpeReportCBTest cb = data.callback(true, 10);

        cb.callback("10.1.1.254_java");

        assertThat(cb.nodes(), contains(data.nodes("10.1.1.254")));
    }


    @Test
    public void singleLangMultipleNodesUsingFrontEnd() throws Exception {
        NodeData data = singleLangData();
        DpeReportCBTest cb = data.callback(true, 10);

        cb.callback("10.1.1.254_java");
        cb.callback("10.1.1.1_java");
        cb.callback("10.1.1.2_java");

        WorkerNode[] expected = data.nodes("10.1.1.254", "10.1.1.1", "10.1.1.2");

        assertThat(cb.nodes(), containsInAnyOrder(expected));
    }


    @Test
    public void singleLangSingleNodeIgnoringFrontEnd() throws Exception {
        NodeData data = singleLangData();
        DpeReportCBTest cb = data.callback(false, 10);

        cb.callback("10.1.1.254_java");

        assertThat(cb.nodes(), is(empty()));
    }


    @Test
    public void singleLangMultipleNodesIgnoringFrontEnd() throws Exception {
        NodeData data = singleLangData();
        DpeReportCBTest cb = data.callback(false, 10);

        cb.callback("10.1.1.1_java");
        cb.callback("10.1.1.2_java");
        cb.callback("10.1.1.254_java");

        WorkerNode[] expected = data.nodes("10.1.1.1", "10.1.1.2");

        assertThat(cb.nodes(), containsInAnyOrder(expected));
    }


    @Test
    public void singleLangLimitNodesUsingFrontEnd() throws Exception {
        NodeData data = singleLangData();
        DpeReportCBTest cb = data.callback(true, 3);

        cb.callback("10.1.1.254_java");
        cb.callback("10.1.1.3_java");

        cb.waitCallbacks();

        cb.callback("10.1.1.2_java");
        cb.callback("10.1.1.1_java");

        cb.callback("10.1.1.3_java");
        cb.callback("10.1.1.4_java");

        Set<WorkerNode> actual = new HashSet<>(cb.nodes());

        assertThat(actual, iterableWithSize(3));
        assertThat(actual, hasItem(data.node("10.1.1.254")));
    }


    @Test
    public void singleLangLimitNodesIgnoringFrontEnd() throws Exception {
        NodeData data = singleLangData();
        DpeReportCBTest cb = data.callback(false, 3);

        cb.callback("10.1.1.254_java");
        cb.callback("10.1.1.2_java");
        cb.callback("10.1.1.3_java");

        cb.callback("10.1.1.5_java");
        cb.callback("10.1.1.2_java");
        cb.callback("10.1.1.254_java");

        cb.callback("10.1.1.3_java");
        cb.callback("10.1.1.1_java");
        cb.callback("10.1.1.4_java");

        Set<WorkerNode> actual = new HashSet<>(cb.nodes());

        assertThat(actual, iterableWithSize(3));
        assertThat(actual, not(hasItem(data.node("10.1.1.254"))));
    }


    @Test
    public void multiLangUseSingleNode() throws Exception {
        NodeData data = multiLangData();
        DpeReportCBTest cb = data.callback(false, 10);

        cb.callback("10.1.1.1_java");
        cb.callback("10.1.1.1_cpp");
        cb.callback("10.1.1.1_cpp");
        cb.callback("10.1.1.1_java");

        WorkerNode expected = data.node("10.1.1.1");

        assertThat(cb.nodes(), contains(expected));
    }


    @Test
    public void multiLangUseAllLanguages() throws Exception {
        NodeData data = new NodeData(AppData.J1, AppData.C1, AppData.P1);
        DpeReportCBTest cb = data.callback(false, 10);

        cb.callback("10.1.1.1_java");
        cb.callback("10.1.1.1_cpp");
        cb.callback("10.1.1.1_python");

        DpeName[] expectedDpes = new DpeName[] {
                new DpeName("10.1.1.1_java"),
                new DpeName("10.1.1.1_cpp"),
                new DpeName("10.1.1.1_python")
        };

        assertThat(cb.nodes().get(0).dpes(), contains(expectedDpes));
    }


    @Test
    public void multiLangUseMultipleNodes() throws Exception {
        NodeData data = multiLangData();
        DpeReportCBTest cb = data.callback(false, 10);

        cb.callback("10.1.1.1_java");
        cb.callback("10.1.1.3_cpp");
        cb.callback("10.1.1.2_java");

        cb.callback("10.1.1.3_java");
        cb.callback("10.1.1.2_cpp");
        cb.callback("10.1.1.1_cpp");

        WorkerNode[] expected = data.nodes("10.1.1.1", "10.1.1.2", "10.1.1.3");

        assertThat(cb.nodes(), containsInAnyOrder(expected));
    }


    @Test
    public void multiLangIgnoreUncompletedNodes() throws Exception {
        NodeData data = multiLangData();
        DpeReportCBTest cb = data.callback(false, 10);

        cb.callback("10.1.1.1_java");
        cb.callback("10.1.1.3_cpp");
        cb.callback("10.1.1.2_java");

        cb.callback("10.1.1.3_java");
        cb.callback("10.1.1.4_cpp");
        cb.callback("10.1.1.1_cpp");

        WorkerNode[] expected = data.nodes("10.1.1.1", "10.1.1.3");

        assertThat(cb.nodes(), containsInAnyOrder(expected));
    }


    @Test
    public void multiLangSingleNodeUsingFrontEnd() throws Exception {
        NodeData data = multiLangData();
        DpeReportCBTest cb = data.callback(true, 10);

        cb.callback("10.1.1.254_java");
        cb.callback("10.1.1.254_cpp");

        assertThat(cb.nodes(), contains(data.nodes("10.1.1.254")));
    }


    @Test
    public void multiLangMultipleNodesUsingFrontEnd() throws Exception {
        NodeData data = multiLangData();
        DpeReportCBTest cb = data.callback(true, 10);

        cb.callback("10.1.1.254_java");
        cb.callback("10.1.1.254_cpp");
        cb.callback("10.1.1.1_java");
        cb.callback("10.1.1.2_java");
        cb.callback("10.1.1.1_cpp");
        cb.callback("10.1.1.2_cpp");

        WorkerNode[] expected = data.nodes("10.1.1.254", "10.1.1.1", "10.1.1.2");

        assertThat(cb.nodes(), containsInAnyOrder(expected));
    }


    @Test
    public void multiLangSingleNodeIgnoringFrontEnd() throws Exception {
        NodeData data = multiLangData();
        DpeReportCBTest cb = data.callback(false, 10);

        cb.callback("10.1.1.254");

        assertThat(cb.nodes(), is(empty()));
    }


    @Test
    public void multiLangMultipleNodesIgnoringFrontEnd() throws Exception {
        NodeData data = multiLangData();
        DpeReportCBTest cb = data.callback(false, 10);

        cb.callback("10.1.1.1_java");
        cb.callback("10.1.1.2_java");
        cb.callback("10.1.1.254_java");
        cb.callback("10.1.1.254_cpp");
        cb.callback("10.1.1.1_cpp");
        cb.callback("10.1.1.2_cpp");

        WorkerNode[] expected = data.nodes("10.1.1.1", "10.1.1.2");

        assertThat(cb.nodes(), containsInAnyOrder(expected));
    }


    @Test
    public void multiLangLimitNodesUsingFrontEnd() throws Exception {
        NodeData data = multiLangData();
        DpeReportCBTest cb = data.callback(true, 3);

        cb.callback("10.1.1.254_java");
        cb.callback("10.1.1.254_cpp");
        cb.callback("10.1.1.3_java");
        cb.callback("10.1.1.3_cpp");

        cb.waitCallbacks();

        cb.callback("10.1.1.2_java");
        cb.callback("10.1.1.1_java");
        cb.callback("10.1.1.4_java");

        cb.callback("10.1.1.2_cpp");
        cb.callback("10.1.1.1_cpp");
        cb.callback("10.1.1.4_cpp");

        Set<WorkerNode> actual = new HashSet<>(cb.nodes());

        assertThat(actual, iterableWithSize(3));
        assertThat(actual, hasItem(data.node("10.1.1.254")));
    }


    @Test
    public void multiLangLimitNodesIgnoringFrontEnd() throws Exception {
        NodeData data = multiLangData();
        DpeReportCBTest cb = data.callback(false, 3);

        cb.callback("10.1.1.2_java");
        cb.callback("10.1.1.3_java");
        cb.callback("10.1.1.254_java");

        cb.callback("10.1.1.2_cpp");
        cb.callback("10.1.1.3_cpp");
        cb.callback("10.1.1.254_cpp");

        cb.callback("10.1.1.1_java");
        cb.callback("10.1.1.5_java");
        cb.callback("10.1.1.254_java");

        cb.callback("10.1.1.1_cpp");
        cb.callback("10.1.1.5_cpp");
        cb.callback("10.1.1.254_cpp");

        cb.callback("10.1.1.4_java");
        cb.callback("10.1.1.4_cpp");

        Set<WorkerNode> actual = new HashSet<>(cb.nodes());

        assertThat(actual, iterableWithSize(3));
        assertThat(actual, not(hasItem(data.node("10.1.1.254"))));
    }


    private NodeData singleLangData() {
        return new NodeData(AppData.J1);
    }


    private NodeData multiLangData() {
        return new NodeData(AppData.J1, AppData.C1);
    }


    private class NodeData {

        private final ApplicationInfo app;

        NodeData(ServiceInfo... services) {
            this.app = AppData.newAppInfo(services);
        }

        DpeReportCBTest callback(boolean useFrontEnd, int maxNodes) {
            return new DpeReportCBTest(app, useFrontEnd, maxNodes);
        }

        WorkerNode node(String host) {
            WorkerApplication app = AppData.builder().withDpes(dpes(host)).build();
            return new WorkerNode(orchestrator, app);
        }

        WorkerNode[] nodes(String... hosts) {
            return Stream.of(hosts).map(this::node).toArray(WorkerNode[]::new);
        }

        private DpeInfo[] dpes(String host) {
            return app.getLanguages().stream()
                      .map(lang -> new DpeName(host, lang))
                      .map(name -> new DpeInfo(name, AppData.CORES, ""))
                      .toArray(DpeInfo[]::new);
        }
    }


    private class DpeReportCBTest {

        private final List<Callable<Object>> tasks;
        private final List<WorkerNode> nodes;
        private final Consumer<WorkerNode> nodeConsumer;
        private final DpeReportCB callback;

        DpeReportCBTest(ApplicationInfo application, boolean useFrontEnd, int maxNodes) {
            tasks = Collections.synchronizedList(new ArrayList<>());
            nodes = Collections.synchronizedList(new ArrayList<>());
            nodeConsumer = nodes::add;
            callback = new DpeReportCB(orchestrator, options(useFrontEnd, maxNodes),
                                       application, nodeConsumer);
        }

        public void callback(String dpeName) {
            tasks.add(Executors.callable(() -> callback.callback(AppData.dpe(dpeName))));
        }

        public void waitCallbacks() throws Exception {
            EXECUTOR.invokeAll(tasks);
            tasks.clear();
        }

        public List<WorkerNode> nodes() throws Exception {
            waitCallbacks();
            return nodes;
        }
    }


    private static OrchestratorOptions options(boolean useFrontEnd, int maxNodes) {
        OrchestratorOptions.Builder builder = OrchestratorOptions.builder();
        if (useFrontEnd) {
            builder.useFrontEnd();
        }
        builder.withMaxNodes(maxNodes);
        return builder.build();
    }
}
