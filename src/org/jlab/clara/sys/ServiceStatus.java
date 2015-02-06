package org.jlab.clara.sys;

import org.jlab.coda.xmsg.core.xMsgConstants;

/**
 * <p>
 *     This class holds service status information,
 *     including static information, such as the
 *     name and the description, as well as dynamic
 *     information about the number of requests to
 *     this service and service engine execution time.
 *     Note: request number will be updated by the
 *     service container.
 * </p>
 *
 * @author gurjyan
 * @version 1.x
 * @since 2/2/15
 */
public class ServiceStatus {
    private String name =
            xMsgConstants.UNDEFINED.getStringValue();

    private String description =
            xMsgConstants.UNDEFINED.getStringValue();

    private volatile int requestNumber;

    private volatile long averageExecutionTime;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int getRequestNumber() {
        return requestNumber;
    }

    public void setRequestNumber(int requestNumber) {
        this.requestNumber = requestNumber;
    }

    public long getAverageExecutionTime() {
        return averageExecutionTime;
    }

    public void setAverageExecutionTime(long averageExecutionTime) {
        this.averageExecutionTime = averageExecutionTime;
    }
}
