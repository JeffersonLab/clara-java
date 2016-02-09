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

package org.jlab.clara.util;

/**
 *     Clara internal constants
 *
 * @author gurjyan
 * @version 4.x
 */
public class CConstants {

    public static final String DPE = "dpe";
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


    public static final String SHARED_MEMORY_KEY = "clara/shmkey";

    public static final String MAPKEY_SEP = "#";

    public static final int BENCHMARK = 10000;

    public static final String JAVA_LANG = "java";
    public static final String PYTHON_LANG = "python";
    public static final String CPP_LANG = "cpp";

    public static final String UNDEFINED = "undefined";

}
