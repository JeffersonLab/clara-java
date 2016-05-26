/*
 *   Copyright (c) 2016.  Jefferson Lab (JLab). All rights reserved. Permission
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

package org.jlab.clara.util.report;

import org.jlab.clara.base.core.ClaraComponent;
import org.jlab.clara.engine.Engine;

/**
 * @author gurjyan
 * @version 4.x
 */
public class ServiceReport extends BaseReport {

    private final String engineName;
    private final String className;
    private final String version;

    private int failureCount;
    private int shrmReads;
    private int shrmWrites;
    private int bytesReceived;
    private int bytesSent;
    private int executionTime;

    public ServiceReport(ClaraComponent comp, Engine engine) {
        super(comp.getCanonicalName(), engine.getAuthor(), engine.getDescription());
        this.engineName = comp.getEngineName();
        this.className = comp.getEngineClass();
        this.version = engine.getVersion();
    }

    public String getEngineName() {
        return engineName;
    }

    public String getClassName() {
        return className;
    }

    public int getFailureCount() {
        return failureCount;
    }

    public void setFailureCount(int failureCount) {
        this.failureCount = failureCount;
    }

    public int getShrmReads() {
        return shrmReads;
    }

    public void setShrmReads(int shrmReads) {
        this.shrmReads = shrmReads;
    }

    public int getShrmWrites() {
        return shrmWrites;
    }

    public void setShrmWrites(int shrmWrites) {
        this.shrmWrites = shrmWrites;
    }

    public int getBytesReceived() {
        return bytesReceived;
    }

    public void setBytesReceived(int bytesReceived) {
        this.bytesReceived = bytesReceived;
    }

    public int getBytesSent() {
        return bytesSent;
    }

    public void setBytesSent(int bytesSent) {
        this.bytesSent = bytesSent;
    }

    public int getExecutionTime() {
        return executionTime;
    }

    public void setExecutionTime(int executionTime) {
        this.executionTime = executionTime;
    }

    public String getVersion() {
        return version;
    }
}
