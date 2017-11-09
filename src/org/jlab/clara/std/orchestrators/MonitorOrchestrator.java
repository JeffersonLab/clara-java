package org.jlab.clara.std.orchestrators;

import org.jlab.clara.base.BaseOrchestrator;
import org.jlab.clara.base.ClaraLang;
import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.base.DataRingAddress;
import org.jlab.clara.base.DataRingTopic;
import org.jlab.clara.base.DpeName;
import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;

import java.util.Set;

/**
 * Listen to reports published to the CLARA data-ring.
 */
public class MonitorOrchestrator implements AutoCloseable {

    private final BaseOrchestrator orchestrator;

    public static void main(String[] args) throws Exception {
        try {
            MonitorOrchestrator monitor = new MonitorOrchestrator();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> monitor.close()));

            monitor.listenDpeReports((reg, run) -> {
                System.out.printf("%s: received DPE report from %s%n",
                        ClaraUtil.getCurrentTime(), reg.name());
            });

            monitor.listenEngineReports(new EngineReportHandler() {
                @Override
                public void handleEvent(EngineData event) {
                    System.out.printf("%s: received %s [%s] from %s%n",
                            ClaraUtil.getCurrentTime(),
                            event.getExecutionState(),
                            event.getMimeType(),
                            event.getEngineName());
                }

                @Override
                public Set<EngineDataType> dataTypes() {
                    return ClaraUtil.buildDataTypes(EngineDataType.STRING, EngineDataType.JSON);
                }
            });
        } catch (ClaraException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }


    /**
     * Create a new monitor orchestrator to a default CLARA data-ring in the localhost.
     */
    public MonitorOrchestrator() {
        this(getDataRing());
    }


    /**
     * Create a new monitor orchestrator to the given CLARA data-ring.
     *
     * @param address the address of the CLARA data-ring
     */
    public MonitorOrchestrator(DataRingAddress address) {
        orchestrator = new BaseOrchestrator(getRingAsDpe(address), getPoolSize());
    }


    private static DataRingAddress getDataRing() {
        String monName = System.getenv(ClaraConstants.ENV_MONITOR_FE);
        if (monName != null) {
            return new DataRingAddress(new DpeName(monName));
        }
        return new DataRingAddress(ClaraUtil.localhost());
    }


    private static int getPoolSize() {
        int cores = Runtime.getRuntime().availableProcessors();
        if (cores <= 8) {
            return 12;
        }
        if (cores <= 16) {
            return 24;
        }
        return 32;
    }


    private static DpeName getRingAsDpe(DataRingAddress address) {
        return new DpeName(address.host(), address.pubPort(), ClaraLang.JAVA);
    }


    /**
     * Listen DPE reports.
     *
     * @param handler DPE report handler
     * @throws ClaraException if the subscription could not be started
     */
    public void listenDpeReports(DpeReportHandler handler)
            throws ClaraException {
        orchestrator.listen()
                .dpeReport()
                .parseJson()
                .start(handler::handleReport);
    }


    /**
     * Listen DPE reports.
     *
     * @param session DPE session
     * @param handler DPE report handler
     * @throws ClaraException if the subscription could not be started
     */
    public void listenDpeReports(String session, DpeReportHandler handler)
            throws ClaraException {
        orchestrator.listen()
                .dpeReport(session)
                .parseJson()
                .start(handler::handleReport);
    }


    /**
     * Listen engine reports.
     *
     * @param handler data-ring event handler
     * @throws ClaraException if the subscription could not be started
     */
    public void listenEngineReports(EngineReportHandler handler)
            throws ClaraException {
        orchestrator.listen()
                .dataRing()
                .withDataTypes(handler.dataTypes())
                .start(handler::handleEvent);
    }


    /**
     * Listen engine reports.
     *
     * @param ringTopic data-ring topic
     * @param handler data-ring event handler
     * @throws ClaraException if the subscription could not be started
     */
    public void listenEngineReports(DataRingTopic ringTopic, EngineReportHandler handler)
            throws ClaraException {
        orchestrator.listen()
                .dataRing(ringTopic)
                .withDataTypes(handler.dataTypes())
                .start(handler::handleEvent);
    }


    @Override
    public void close() {
        orchestrator.close();
    }
}
