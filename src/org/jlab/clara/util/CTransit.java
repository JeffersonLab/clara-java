package org.jlab.clara.util;

import org.jlab.coda.xmsg.data.xMsgD;

/**
 * Describe.....
 *
 * @author gurjyan
 * @version 1.x
 * @since 2/27/15
 */
public class CTransit {

    private xMsgD.Data.Builder transitData;
    private Object userObject;

    public CTransit(xMsgD.Data.Builder trData, Object usrObject) {
        this.transitData = trData;
        this.userObject = usrObject;
    }

    public xMsgD.Data.Builder getTransitData() {
        return transitData;
    }

    public void setTransitData(xMsgD.Data.Builder transitData) {
        this.transitData = transitData;
    }

    public Object getUserObject() {
        return userObject;
    }

    public void setUserObject(Object userObject) {
        this.userObject = userObject;
    }
}
