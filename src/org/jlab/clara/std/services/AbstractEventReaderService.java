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

import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * An abstract reader service that reads events from the configured input file.
 *
 * @param <Reader> the class for the user-defined reader of the given data-type
 */
public abstract class AbstractEventReaderService<Reader> extends AbstractService {

    private static final String CONF_ACTION = "action";
    private static final String CONF_FILENAME = "file";

    private static final String CONF_ACTION_OPEN = "open";
    private static final String CONF_ACTION_CLOSE = "close";

    private static final String CONF_EVENTS_SKIP = "skip";
    private static final String CONF_EVENTS_MAX = "max";

    private static final String REQUEST_NEXT = "next";
    private static final String REQUEST_NEXT_REC = "next-rec";
    private static final String REQUEST_ORDER = "order";
    private static final String REQUEST_COUNT = "count";

    private static final String NO_NAME = "";
    private static final String NO_FILE = "No open file";
    private static final String END_OF_FILE = "End of file";

    private static final int EOF_NOT_FROM_WRITER = 0;
    private static final int EOF_WAITING_REC = -1;

    private String fileName = NO_NAME;
    private String openError = NO_FILE;

    /** The reader object. */
    protected Reader reader;
    private final Object readerLock = new Object();

    private int currentEvent;
    private int lastEvent;
    private int eventCount;

    private Set<Integer> processingEvents = new HashSet<Integer>();
    private int eofRequestCount;


    @Override
    public EngineData configure(EngineData input) {
        final long startTime = System.currentTimeMillis();
        if (input.getMimeType().equalsIgnoreCase(EngineDataType.JSON.mimeType())) {
            String source = (String) input.getData();
            JSONObject data = new JSONObject(source);
            if (data.has(CONF_ACTION) && data.has(CONF_FILENAME)) {
                String action = data.getString(CONF_ACTION);
                if (action.equals(CONF_ACTION_OPEN)) {
                    openFile(data);
                } else if (action.equals(CONF_ACTION_CLOSE)) {
                    closeFile(data);
                } else {
                    logger.error("config: wrong '{}' parameter value = {}", CONF_ACTION, action);
                }
            } else {
                logger.error("config: missing '{}' or '{}' parameters", CONF_ACTION, CONF_FILENAME);
            }
        } else {
            logger.error("config: wrong mime-type {}", input.getMimeType());
        }
        logger.info("config time: {} [ms]", System.currentTimeMillis() - startTime);
        return null;
    }


    private void openFile(JSONObject configData) {
        synchronized (readerLock) {
            if (reader != null) {
                closeFile();
            }
            fileName = configData.getString(CONF_FILENAME);
            logger.info("request to open file {}", fileName);
            try {
                reader = createReader(Paths.get(fileName), configData);
                setLimits(configData);
                logger.info("opened file {}", fileName);
            } catch (EventReaderException e) {
                logger.error("could not open file {}", fileName, e);
                fileName = null;
            }
        }
    }


    private void setLimits(JSONObject configData) throws EventReaderException {
        eventCount = readEventCount();
        int skipEvents = getValue(configData, CONF_EVENTS_SKIP, 0, 0, eventCount);
        if (skipEvents != 0) {
            logger.info("config: skip first {} events", skipEvents);
        }
        currentEvent = skipEvents;

        int remEvents = eventCount - skipEvents;
        int maxEvents = getValue(configData, CONF_EVENTS_MAX, remEvents, 0, remEvents);
        if (maxEvents != remEvents) {
            logger.info("config: read {} events%n", maxEvents);
        }
        lastEvent = skipEvents + maxEvents;

        processingEvents.clear();
        eofRequestCount = 0;
    }


    private int getValue(JSONObject configData, String key, int defVal, int minVal, int maxVal) {
        if (configData.has(key)) {
            try {
                int value = configData.getInt(key);
                if (value >= minVal && value <= maxVal) {
                    return value;
                }
                logger.error("config: invalid value for '{}': {}", key, value);
            } catch (JSONException e) {
                logger.error("config: {}", e.getMessage());
            }
        }
        return defVal;
    }


    private void closeFile(JSONObject configData) {
        synchronized (readerLock) {
            fileName = configData.getString(CONF_FILENAME);
            logger.info("request to close file {}", fileName);
            if (reader != null) {
                closeFile();
            } else {
                logger.error("file {} not open", fileName);
            }
            openError = NO_FILE;
            fileName = null;
        }
    }


    private void closeFile() {
        closeReader();
        reader = null;
        logger.info("closed file {}", fileName);
    }


    /**
     * Creates a new reader and opens the given input file.
     *
     * @param file the path to the input file
     * @param opts extra options for the reader
     * @return a new reader ready to read events from the input file
     * @throws EventReaderException if the reader could not be created
     */
    protected abstract Reader createReader(Path file, JSONObject opts) throws EventReaderException;

    /**
     * Closes the reader and its input file.
     */
    protected abstract void closeReader();


    @Override
    public EngineData execute(EngineData input) {
        EngineData output = new EngineData();

        String dt = input.getMimeType();
        if (dt.equalsIgnoreCase(EngineDataType.STRING.mimeType())) {
            String request = (String) input.getData();
            if (request.equals(REQUEST_NEXT) || request.equals(REQUEST_NEXT_REC)) {
                getNextEvent(input, output);
            } else if (request.equals(REQUEST_ORDER)) {
                logger.info("execute request {}", REQUEST_ORDER);
                getFileByteOrder(output);
            } else if (request.equals(REQUEST_COUNT)) {
                logger.info("execute request {}", REQUEST_COUNT);
                getEventCount(output);
            } else {
                ServiceUtils.setError(output, String.format("Wrong input data = '%s'", request));
            }
        } else {
            String errorMsg = String.format("Wrong input type '%s'", dt);
            ServiceUtils.setError(output, errorMsg);
        }

        return output;
    }


    private boolean isReconstructionRequest(EngineData input) {
        String requestType = (String) input.getData();
        return requestType.equalsIgnoreCase(REQUEST_NEXT_REC);
    }


    private void getNextEvent(EngineData input, EngineData output) {
        synchronized (readerLock) {
            boolean fromRec = isReconstructionRequest(input);
            if (fromRec) {
                processingEvents.remove(input.getCommunicationId());
            }
            if (reader == null) {
                ServiceUtils.setError(output, openError, 1);
            } else if (currentEvent < lastEvent) {
                returnNextEvent(output);
            } else {
                ServiceUtils.setError(output, END_OF_FILE, 1);
                if (fromRec) {
                    if (processingEvents.isEmpty()) {
                        eofRequestCount++;
                        ServiceUtils.setError(output, END_OF_FILE, eofRequestCount + 1);
                        output.setData(EngineDataType.SFIXED32.mimeType(), eofRequestCount);
                    } else {
                        output.setData(EngineDataType.SFIXED32.mimeType(), EOF_WAITING_REC);
                    }
                } else {
                    output.setData(EngineDataType.SFIXED32.mimeType(), EOF_NOT_FROM_WRITER);
                }
            }
        }
    }


    private void returnNextEvent(EngineData output) {
        try {
            Object event = readEvent(currentEvent);
            output.setData(getDataType().toString(), event);
            output.setDescription("data");
            output.setCommunicationId(currentEvent);
            processingEvents.add(currentEvent);
            currentEvent++;
        } catch (EventReaderException e) {
            String msg = String.format("Error requesting event %d from file %s%n%n%s",
                    currentEvent, fileName, ClaraUtil.reportException(e));
            ServiceUtils.setError(output, msg, 1);
        }
    }


    private void getFileByteOrder(EngineData output) {
        synchronized (readerLock) {
            if (reader == null) {
                ServiceUtils.setError(output, openError, 1);
            } else {
                try {
                    output.setData(EngineDataType.STRING.mimeType(), readByteOrder().toString());
                    output.setDescription("byte order");
                } catch (EventReaderException e) {
                    String msg = String.format("Error requesting byte-order from file %s%n%n%s",
                            fileName, ClaraUtil.reportException(e));
                    ServiceUtils.setError(output, msg, 1);
                }
            }
        }
    }


    private void getEventCount(EngineData output) {
        synchronized (readerLock) {
            if (reader == null) {
                ServiceUtils.setError(output, openError, 1);
            } else {
                output.setData(EngineDataType.SFIXED32.mimeType(), eventCount);
                output.setDescription("event count");
            }
        }
    }


    @Override
    public EngineData executeGroup(Set<EngineData> inputs) {
        return null;
    }


    /**
     * Gets the total number of events that can be read from the input file.
     *
     * @return the total number of events in the file
     * @throws EventReaderException if the file could not be read
     */
    protected abstract int readEventCount() throws EventReaderException;

    /**
     * Gets the byte order of the events stored in the input file.
     *
     * @return the byte order of the events in the file
     * @throws EventReaderException if the file could not be read
     */
    protected abstract ByteOrder readByteOrder() throws EventReaderException;

    /**
     * Reads an event from the input file.
     * The event should be a Java object with the same type as the one defined
     * by the CLARA engine data-type returned by {@link #getDataType()}.
     *
     * @param eventNumber the index of the event in the file (starts from zero)
     * @return the read event as a Java object
     * @throws EventReaderException if the file could not be read
     */
    protected abstract Object readEvent(int eventNumber) throws EventReaderException;

    /**
     * Gets the CLARA engine data-type for the type of the events.
     * The data-type will be used to serialize the events when the engine data
     * result needs to be sent to services over the network.
     *
     * @return the data-type of the events
     */
    protected abstract EngineDataType getDataType();


    @Override
    public Set<EngineDataType> getInputDataTypes() {
        return ClaraUtil.buildDataTypes(
                EngineDataType.JSON,
                EngineDataType.STRING);
    }

    @Override
    public Set<EngineDataType> getOutputDataTypes() {
        return ClaraUtil.buildDataTypes(
                getDataType(),
                EngineDataType.STRING,
                EngineDataType.SFIXED32);
    }


    @Override
    public void reset() {
        synchronized (readerLock) {
            if (reader != null) {
                closeFile();
            }
        }
    }


    @Override
    public void destroy() {
        synchronized (readerLock) {
            if (reader != null) {
                closeFile();
            }
        }
    }
}
