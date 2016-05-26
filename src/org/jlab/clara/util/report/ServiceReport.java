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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author gurjyan
 * @version 4.x
 */
public class ServiceReport extends BaseReport {

    private final String engineName;
    private final String className;
    private final String version;

    private final AtomicInteger failureCount = new AtomicInteger();
    private final AtomicInteger shrmReads = new AtomicInteger();
    private final AtomicInteger shrmWrites = new AtomicInteger();
    private final AtomicLong bytesReceived = new AtomicLong();
    private final AtomicLong bytesSent = new AtomicLong();
    private final AtomicLong executionTime = new AtomicLong();

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
        return failureCount.get();
    }

    public void incrementFailureCount() {
        failureCount.getAndIncrement();
    }

    public int getShrmReads() {
        return shrmReads.get();
    }

    public void incrementShrmReads() {
        shrmReads.getAndIncrement();
    }

    public int getShrmWrites() {
        return shrmWrites.get();
    }

    public void incrementShrmWrites() {
        shrmWrites.getAndIncrement();
    }

    public long getBytesReceived() {
        return bytesReceived.get();
    }

    public void addBytesReceived(long bytes) {
        bytesReceived.getAndAdd(bytes);
    }

    public long getBytesSent() {
        return bytesSent.get();
    }

    public void addBytesSent(long bytes) {
        bytesSent.getAndAdd(bytes);
    }

    public long getExecutionTime() {
        return executionTime.get();
    }

    public void addExecutionTime(long deltaTime) {
        executionTime.getAndAdd(deltaTime);
    }

    public String getVersion() {
        return version;
    }
}
