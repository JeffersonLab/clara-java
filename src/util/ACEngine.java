package util;

import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.data.xMsgD;

import java.util.List;

/**
 * An abstract class that service engines will implement.
 *
 * @author gurjyan
 * @version 1.x
 * @since 1/31/15
 */
public abstract class ACEngine {
    private String exception_string =
            xMsgConstants.UNDEFINED.getStringValue();
    private String exception_severity =
            xMsgConstants.UNDEFINED.getStringValue();

    public abstract xMsgD.Data.Builder execute(xMsgD.Data.Builder x);

    public abstract xMsgD.Data.Builder execute_group(List<xMsgD.Data.Builder> x);

    public abstract void configure(xMsgD.Data.Builder x);

    public abstract List<String> get_states();

    public abstract String get_current_state();

    public abstract xMsgD.Data.DType get_accepted_data_type();

    public abstract xMsgD.Data.DType get_returned_data_type();

    public abstract String get_description();

    public abstract String get_version();

    public abstract String get_author();

    public abstract void dispose();

    public String get_exception_string() {
        return exception_string;
    }

    public String get_exception_severity() {
        return exception_severity;
    }

    public void set_exception(String s) {
        exception_string = s;
    }

    public void set_exception(String exception_string,
                              String severity){
        this.exception_string = exception_string;
        this.exception_severity = severity;
    }

    public void reset(){
        this.exception_severity =
                xMsgConstants.UNDEFINED.getStringValue();
        this.exception_string =
                xMsgConstants.UNDEFINED.getStringValue();
    }

}
