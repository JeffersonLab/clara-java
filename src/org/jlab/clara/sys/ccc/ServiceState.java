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
package org.jlab.clara.sys.ccc;

import org.jlab.coda.xmsg.core.xMsgConstants;

/**
 * <p>
 *     Defines service-name and state pair.
 * </p>
 *
 * @author gurjyan
 * @version 1.x
 * @since 5/21/15
 */
public class ServiceState {

    private String name = xMsgConstants.UNDEFINED.getStringValue();
    private String state = xMsgConstants.UNDEFINED.getStringValue();

    public String getName() {
        return name;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public ServiceState(String name, String state){
        this.name = name;
        this.state = state;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof ServiceState){
            ServiceState tms = (ServiceState)obj;
            if(tms.getName().equals(getName()) && tms.getState().equals(getState())){
                return true;
            }
        }
        return false;
    }
}
