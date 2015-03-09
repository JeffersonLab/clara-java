package org.jlab.clara.examples.engines;

import org.jlab.clara.util.ACEngine;
import org.jlab.clara.util.CDataType;
import org.jlab.clara.util.EngineData;
import org.jlab.coda.xmsg.data.xMsgD;

import java.util.List;

/**
 * <p>
 *     User engine class example
 * </p>
 *
 * @author gurjyan
 * @version 1.x
 * @since 2/9/15
 */
public class E3 extends ACEngine {
    @Override
    public EngineData execute(EngineData x) {
        if(x.getDataType().equals(CDataType.T_STRING))
        System.out.println("E3 engine execute... "+x.getData());
        return x;
    }

    @Override
    public EngineData execute_group(List<EngineData> x) {
        System.out.println("E3 engine group execute...");
        return x.get(0);
    }

    @Override
    public void configure(EngineData x) {
        System.out.println("E3 engine configure...");
    }

    @Override
    public List<String> get_states() {
        return null;
    }

    @Override
    public String get_current_state() {
        return null;
    }

    @Override
    public xMsgD.Data.DType get_accepted_data_type() {
        return null;
    }

    @Override
    public xMsgD.Data.DType get_returned_data_type() {
        return null;
    }

    @Override
    public String get_description() {
        return ("E3 test engine");
    }

    @Override
    public String get_version() {
        return null;
    }

    @Override
    public String get_author() {
        return null;
    }

    @Override
    public void dispose() {

    }
}
