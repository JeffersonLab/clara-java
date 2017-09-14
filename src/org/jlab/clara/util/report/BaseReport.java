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

import org.jlab.clara.base.ClaraLang;
import org.jlab.clara.base.ClaraUtil;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author gurjyan
 * @version 4.x
 */
public class BaseReport {

    protected final String name;
    protected final String session;
    protected final String lang;
    protected final String description;
    protected final String startTime;

    private final AtomicInteger requestCount = new AtomicInteger();

    public BaseReport(String name, String session, String description) {
        this.name = name;
        this.session = session;
        this.lang = ClaraLang.JAVA.toString();
        this.description = description;
        this.startTime = ClaraUtil.getCurrentTime();
    }

    public String getName() {
        return name;
    }

    public String getLang() {
        return lang;
    }

    public String getSession() {
        return session;
    }

    public String getDescription() {
        return description;
    }

    public String getStartTime() {
        return startTime;
    }

    public int getRequestCount() {
        return requestCount.get();
    }

    public void incrementRequestCount() {
        requestCount.getAndIncrement();
    }
}
