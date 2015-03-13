/*
 * Copyright (C) 2015. Jefferson Lab, CLARA framework (JLAB). All Rights Reserved.
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for educational, research, and not-for-profit purposes,
 * without fee and without a signed licensing agreement.
 *
 * Author Vardan Gyurjyan
 * Department of Experimental Nuclear Physics, Jefferson Lab.
 *
 * IN NO EVENT SHALL JLAB BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
 * INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
 * THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF JLAB HAS BEEN ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * JLAB SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE. THE CLARA SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED
 * HEREUNDER IS PROVIDED "AS IS". JLAB HAS NO OBLIGATION TO PROVIDE MAINTENANCE,
 * SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */

package org.jlab.clara.util;

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

    public abstract EngineData execute(EngineData x);

    public abstract EngineData execute_group(List<EngineData> x);

    public abstract void configure(EngineData x);

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
