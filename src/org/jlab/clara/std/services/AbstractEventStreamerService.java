package org.jlab.clara.std.services;

import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;

import java.util.Set;

/**
 * An abstract data streaming service that reads events from the stream of data.
 *
 * @param <Streamer> the class for the user-defined streamer of a given source
 */
public class AbstractEventStreamerService<Streamer> extends AbstractService {


    @Override
    public EngineData configure(EngineData input) {
        return null;
    }

    @Override
    public EngineData execute(EngineData input) {
        return null;
    }

    @Override
    public Set<EngineDataType> getInputDataTypes() {
        return null;
    }

    @Override
    public Set<EngineDataType> getOutputDataTypes() {
        return null;
    }

    @Override
    public void reset() {

    }

    @Override
    public void destroy() {

    }
}
