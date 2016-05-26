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
    private long bytesReceived;
    private long bytesSent;
    private long executionTime;

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

    public void incrementFailureCount() {
        failureCount++;
    }

    public int getShrmReads() {
        return shrmReads;
    }

    public void incrementShrmReads() {
        shrmReads++;
    }

    public int getShrmWrites() {
        return shrmWrites;
    }

    public void incrementShrmWrites() {
        shrmWrites++;
    }

    public long getBytesReceived() {
        return bytesReceived;
    }

    public void addBytesReceived(long bytes) {
        bytesReceived += bytes;
    }

    public long getBytesSent() {
        return bytesSent;
    }

    public void addBytesSent(long bytes) {
        bytesSent += bytes;
    }

    public long getExecutionTime() {
        return executionTime;
    }

    public void addExecutionTime(long deltaTime) {
        executionTime += deltaTime;
    }

    public String getVersion() {
        return version;
    }
}
