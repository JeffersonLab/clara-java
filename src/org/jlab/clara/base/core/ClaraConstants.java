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

package org.jlab.clara.base.core;

/**
 * CLARA internal constants.
 *
 * @author gurjyan
 * @version 4.x
 */
public final class ClaraConstants {

    private ClaraConstants() { }

    public static final int JAVA_PORT = 7771;
    public static final int CPP_PORT = 7781;
    public static final int PYTHON_PORT = 7791;
    public static final int REG_PORT_SHIFT = 4;

    public static final String DPE = "dpe";
    public static final String SESSION = "claraSession";
    public static final String START_DPE = "startDpe";
    public static final String STOP_DPE = "stopDpe";
    public static final String STOP_REMOTE_DPE = "stopRemoteDpe";
    public static final String DPE_EXIT = "dpeExit";
    public static final String PING_DPE = "pingDpe";
    public static final String PING_REMOTE_DPE = "pingRemoteDpe";
    public static final String DPE_ALIVE = "dpeAlive";
    public static final String DPE_REPORT = "dpeReport";

    public static final String CONTAINER = "container";
    public static final String STATE_CONTAINER = "getContainerState";
    public static final String START_CONTAINER = "startContainer";
    public static final String START_REMOTE_CONTAINER = "startRemoteContainer";
    public static final String STOP_CONTAINER = "stopContainer";
    public static final String STOP_REMOTE_CONTAINER = "stopRemoteContainer";
    public static final String CONTAINER_DOWN = "containerIsDown";
    public static final String REMOVE_CONTAINER = "removeContainer";

    public static final String STATE_SERVICE = "getServiceState";
    public static final String START_SERVICE = "startService";
    public static final String START_REMOTE_SERVICE = "startRemoteService";
    public static final String STOP_SERVICE = "stopService";
    public static final String STOP_REMOTE_SERVICE = "stopRemoteService";
    public static final String DEPLOY_SERVICE = "deployService";
    public static final String REMOVE_SERVICE = "removeService";
    public static final String SERVICE_REPORT_DONE = "serviceReportDone";
    public static final String SERVICE_REPORT_DATA = "serviceReportData";

    public static final String SET_FRONT_END = "setFrontEnd";
    public static final String SET_FRONT_END_REMOTE = "setFrontEndRemote";

    public static final String SET_SESSION = "setSession";

    public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static final String REPORT_REGISTRATION = "reportRegistration";
    public static final String REPORT_RUNTIME = "reportRuntime";
    public static final String REPORT_JSON = "reportJson";

    public static final String REGISTRATION_KEY = "DPERegistration";
    public static final String RUNTIME_KEY = "DPERuntime";

    public static final String SHARED_MEMORY_KEY = "clara/shmkey";

    public static final String MAPKEY_SEP = "#";
    public static final String DATA_SEP = "?";
    public static final String LANG_SEP = "_";
    public static final String PORT_SEP = "%";

    public static final String INFO = "INFO";
    public static final String WARNING = "WARNING";
    public static final String ERROR = "ERROR";
    public static final String DONE = "done";
    public static final String DATA = "data";

    public static final int BENCHMARK = 10000;

    public static final String JAVA_LANG = "java";
    public static final String PYTHON_LANG = "python";
    public static final String CPP_LANG = "cpp";

    public static final String UNDEFINED = "undefined";

    public static final String ENV_MONITOR_FE = "CLARA_MONITOR_FE";
}
