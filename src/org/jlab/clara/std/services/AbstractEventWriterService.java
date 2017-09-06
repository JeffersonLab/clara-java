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

package org.jlab.clara.std.services;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.jlab.clara.util.FileUtils;
import org.json.JSONObject;

/**
 * An abstract writer service that writes all received events into the
 * configured output file.
 *
 * @param <Writer> the class for the user-defined writer of the given data-type
 */
public abstract class AbstractEventWriterService<Writer> extends AbstractService {

    private static final String CONF_ACTION = "action";
    private static final String CONF_FILENAME = "file";

    private static final String CONF_ACTION_OPEN = "open";
    private static final String CONF_ACTION_CLOSE = "close";
    private static final String CONF_ACTION_SKIP = "skip";

    private static final String OUTPUT_NEXT = "next-rec";
    private static final String EVENT_SKIP = "skip";

    private static final String NO_NAME = "";
    private static final String NO_FILE = "No open file";

    private String fileName = NO_NAME;
    private boolean skipEvents = false;

    private String openError = NO_FILE;
    private int eventCounter;

    /** The writer object. */
    protected Writer writer;
    private final Object writerLock = new Object();


    @Override
    public EngineData configure(EngineData input) {
        final long startTime = System.currentTimeMillis();
        if (input.getMimeType().equalsIgnoreCase(EngineDataType.JSON.mimeType())) {
            String source = (String) input.getData();
            JSONObject configData = new JSONObject(source);
            if (configData.has(CONF_ACTION)) {
                String action = configData.getString(CONF_ACTION);
                if (action.equals(CONF_ACTION_OPEN)) {
                    if (configData.has(CONF_FILENAME)) {
                        openFile(configData);
                    } else {
                        logger.error("config: missing '{}' parameter", CONF_FILENAME);
                    }
                } else if (action.equals(CONF_ACTION_CLOSE)) {
                    if (configData.has(CONF_FILENAME)) {
                        closeFile(configData);
                    } else {
                        logger.error("config: missing '{}' parameter", CONF_FILENAME);
                    }
                } else if (action.equals(CONF_ACTION_SKIP)) {
                    skipAll();
                } else {
                    logger.error("config: wrong '{}' parameter value = {}", CONF_ACTION, action);
                }
            } else {
                logger.error("config: missing '{}' parameter", CONF_ACTION);
            }
        } else {
            logger.error("config: wrong mimetype '{}'", input.getMimeType());
        }
        logger.info("config time: {} [ms]", System.currentTimeMillis() - startTime);
        return null;
    }


    private void openFile(JSONObject configData) {
        synchronized (writerLock) {
            if (writer != null) {
                writeAndClose();
            }
            fileName = configData.getString(CONF_FILENAME);
            logger.info("request to open file {}", fileName);
            try {
                File file = new File(fileName);
                File outputDir = file.getParentFile();
                if (outputDir != null) {
                    FileUtils.createDirectories(outputDir.toPath());
                }
                writer = createWriter(Paths.get(fileName), configData);
                eventCounter = 0;
                logger.info("opened file {}", fileName);
            } catch (IOException | EventWriterException e) {
                logger.error("could not open file {}", fileName, e);
                fileName = null;
                eventCounter = 0;
            }

            skipEvents = false;
        }
    }


    private void closeFile(JSONObject data) {
        synchronized (writerLock) {
            fileName = data.getString(CONF_FILENAME);
            logger.info("request to close file {}", fileName);
            if (writer != null) {
                writeAndClose();
            } else {
                logger.error("file {} not open", fileName);
            }
            openError = NO_FILE;
            fileName = null;
            eventCounter = 0;
        }
    }


    private void writeAndClose() {
        if (eventCounter > 0) {
            closeWriter();
        }
        logger.info("closed file {}", fileName);
        writer = null;
    }


    private void skipAll() {
        logger.info("request to skip events");
        synchronized (writerLock) {
            if (writer == null) {
                skipEvents = true;
                logger.info("skipping all events");
            } else {
                logger.error("file {} is already open", fileName);
            }
        }
    }


    /**
     * Creates a new writer and opens the given output file.
     *
     * @param file the path to the output file
     * @param opts extra options for the writer
     * @return a new writer ready to writer events to the output file
     * @throws EventWriterException if the writer could not be created
     */
    protected abstract Writer createWriter(Path file, JSONObject opts) throws EventWriterException;

    /**
     * Closes the writer and its output file.
     */
    protected abstract void closeWriter();


    @Override
    public EngineData execute(EngineData input) {
        EngineData output = new EngineData();

        String dt = input.getMimeType();
        if (!dt.equalsIgnoreCase(getDataType().mimeType())) {
            ServiceUtils.setError(output, String.format("Wrong input type '%s'", dt));
            return output;
        }

        if (skipEvents || input.getDescription().equals(EVENT_SKIP)) {
            output.setData(EngineDataType.STRING.mimeType(), OUTPUT_NEXT);
            output.setDescription("event skipped");
            return output;
        }

        synchronized (writerLock) {
            if (writer == null) {
                ServiceUtils.setError(output, openError);
            } else {
                try {
                    writeEvent(input.getData());
                    eventCounter++;
                    output.setData(EngineDataType.STRING.mimeType(), OUTPUT_NEXT);
                    output.setDescription("event saved");

                } catch (EventWriterException e) {
                    String msg = String.format("Error saving event to file %s%n%n%s",
                            fileName, ClaraUtil.reportException(e));
                    ServiceUtils.setError(output, msg);
                }
            }
        }

        return output;
    }


    @Override
    public EngineData executeGroup(Set<EngineData> inputs) {
        return null;
    }


    /**
     * Writes an event to the output file.
     * The event should be a Java object with the same type as the one defined
     * by the CLARA engine data-type returned by {@link #getDataType()}.
     *
     * @param event the event to be written
     * @throws EventWriterException if the file could not be read
     */
    protected abstract void writeEvent(Object event) throws EventWriterException;

    /**
     * Gets the CLARA engine data-type for the type of the events.
     * The data-type will be used to deserialize the events when the engine data
     * is received from services across the network.
     *
     * @return the data-type of the events
     */
    protected abstract EngineDataType getDataType();


    @Override
    public Set<EngineDataType> getInputDataTypes() {
        return ClaraUtil.buildDataTypes(getDataType(), EngineDataType.JSON);
    }

    @Override
    public Set<EngineDataType> getOutputDataTypes() {
        return ClaraUtil.buildDataTypes(EngineDataType.STRING);
    }

    @Override
    public void reset() {
        synchronized (writerLock) {
            if (writer != null) {
                writeAndClose();
            }
        }
    }

    @Override
    public void destroy() {
        synchronized (writerLock) {
            if (writer != null) {
                writeAndClose();
            }
        }
    }
}
