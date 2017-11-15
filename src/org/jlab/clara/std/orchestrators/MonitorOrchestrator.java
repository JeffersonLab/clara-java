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

import org.jlab.clara.base.BaseOrchestrator;
import org.jlab.clara.base.ClaraLang;
import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.base.DataRingAddress;
import org.jlab.clara.base.DataRingTopic;
import org.jlab.clara.base.DpeName;
import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.std.orchestrators.CallbackInfo.RingCallbackInfo;
import org.jlab.clara.std.orchestrators.CallbackInfo.RingListener;
import org.jlab.coda.xmsg.core.xMsgConstants;

import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Listen to reports published to the CLARA data-ring.
 */
public class MonitorOrchestrator implements AutoCloseable {

    private final BaseOrchestrator orchestrator;

    public static void main(String[] args) throws Exception {
        CommandLineBuilder cl = new CommandLineBuilder();
        try {
            cl.parse(args);
            if (cl.hasHelp()) {
                System.out.println(cl.usage());
                System.exit(0);
            }
            OrchestratorConfigParser parser = new OrchestratorConfigParser(cl.setupFile());
            List<RingCallbackInfo> ringCallbacks = parser.parseDataRingCallbacks();
            if (ringCallbacks.isEmpty()) {
                System.err.println("Error: no callbacks found in " + cl.setupFile());
                System.exit(1);
            }

            MonitorOrchestrator monitor = new MonitorOrchestrator();
            Queue<AutoCloseable> handlers = new ConcurrentLinkedQueue<>();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                monitor.close();
                closeHandlers(handlers);
            }));

            RingListener listener = asListener(monitor);
            for (RingCallbackInfo callback : ringCallbacks) {
                AutoCloseable handler = callback.loadCallback(listener);
                handlers.add(handler);
            }
            Logging.info("Waiting reports...");
        } catch (CommandLineException e) {
            System.err.println("error: " + e.getMessage());
            System.err.println(cl.usage());
            System.exit(1);
        } catch (OrchestratorConfigException e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        } catch (ClaraException e) {
            System.err.println("Error: " + e.getMessage());
            Logging.error("Exiting...");
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
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
        Logging.info("Subscribed to all DPE reports");
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
        Logging.info("Subscribed to DPE reports with session = \"%s\"", session);
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
        Logging.info("Subscribed to all service reports");
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
        Logging.info("Subscribed to service reports with %s", getTopicLog(ringTopic));
    }


    private String getTopicLog(DataRingTopic topic) {
        StringBuilder sb = new StringBuilder();
        sb.append("state = ").append('"').append(topic.state()).append('"');
        if (!topic.session().equals(xMsgConstants.ANY)) {
            sb.append("  session = ").append('"').append(topic.session()).append('"');
        }
        if (!topic.engine().equals(xMsgConstants.ANY)) {
            sb.append("  engine = ").append('"').append(topic.engine()).append('"');
        }
        return sb.toString();
    }


    @Override
    public void close() {
        orchestrator.close();
    }


    private static RingListener asListener(MonitorOrchestrator orchestrator) {
        return new RingListener() {

            @Override
            public void listen(DataRingTopic topic, EngineReportHandler handler)
                    throws ClaraException {
                if (topic == null) {
                    orchestrator.listenEngineReports(handler);
                } else {
                    orchestrator.listenEngineReports(topic, handler);
                }
            }

            @Override
            public void listen(String session, DpeReportHandler handler) throws ClaraException {
                if (session == null) {
                    orchestrator.listenDpeReports(handler);
                } else {
                    orchestrator.listenDpeReports(session, handler);
                }
            }
        };
    }


    private static void closeHandlers(Queue<AutoCloseable> handlers) {
        for (AutoCloseable handler : handlers) {
            try {
                handler.close();
            } catch (Exception e) {
                Logging.error("could not close handler: " + e.getMessage());
            }
        }
    }


    static class CommandLineException extends RuntimeException {

        CommandLineException(String message) {
            super(message);
        }

        CommandLineException(Throwable cause) {
            super(cause);
        }

        @Override
        public String getMessage() {
            Throwable cause = getCause();
            if (cause != null) {
                return cause.getMessage();
            }
            return super.getMessage();
        }
    }


    static class CommandLineBuilder {

        private final OptionSpec<String> arguments;

        private OptionParser parser;
        private OptionSet options;

        CommandLineBuilder() {
            parser = new OptionParser();
            arguments = parser.nonOptions();

            parser.acceptsAll(Arrays.asList("h", "help")).forHelp();
        }

        public void parse(String[] args) {
            try {
                options = parser.parse(args);
                if (hasHelp()) {
                    return;
                }
                int numArgs = options.nonOptionArguments().size();
                if (numArgs == 0) {
                    throw new CommandLineException("missing arguments");
                }
                if (numArgs > 1) {
                    throw new CommandLineException("invalid number of arguments");
                }
            } catch (OptionException e) {
                throw new CommandLineException(e);
            }
        }

        public boolean hasHelp() {
            return options.has("help");
        }

        public String setupFile() {
            List<String> args = arguments.values(options);
            return args.get(0);
        }

        public String usage() {
            String wrapper = "clara-monitor";
            return String.format("usage: %s [options] <setup.yml>", wrapper);
        }
    }
}
