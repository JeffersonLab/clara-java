package org.jlab.clara.examples.engines;

import org.jlab.clara.util.ACEngine;
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
public class E1 extends ACEngine {
    @Override
    public xMsgD.Data.Builder execute(xMsgD.Data.Builder x) {
        System.out.println("E1 engine execute... "+x.getSTRING());
        return x;
    }

    @Override
    public xMsgD.Data.Builder execute_group(List<xMsgD.Data.Builder> x) {
        System.out.println("E1 engine group execute...");
        return x.get(0);
    }

    @Override
    public void configure(xMsgD.Data.Builder x) {
        System.out.println("E1 engine configure...");
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
        return ("E1 test engine");
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
