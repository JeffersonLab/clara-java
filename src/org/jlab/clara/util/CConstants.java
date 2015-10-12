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

/**
 * <p>
 *     Clara internal constants
 * </p>
 *
 * @author gurjyan
 * @version 1.x
 * @since 2/7/15
 */
public class CConstants {

    public static final String ACCEPT_FE = "acceptFe";
    public static final String DPE = "dpe";
    public static final String START_DPE = "startDpe";
    public static final String STOP_DPE = "stopDpe";
    public static final String DPE_UP = "dpeIsUp";
    public static final String DPE_DOWN = "dpeIsDown";
    public static final String DPE_EXIT = "dpeExit";
    public static final String DPE_PING = "dpePing";
    public static final String DPE_ALIVE = "dpeAlive";
    public static final String LIST_DPES = "listDpes";

    public static final String CONTAINER = "container";
    public static final String START_CONTAINER = "startContainer";
    public static final String STOP_CONTAINER = "stopContainer";
    public static final String CONTAINER_UP = "containerIsUp";
    public static final String CONTAINER_DOWN = "containerIsDown";
    public static final String REMOVE_CONTAINER = "removeContainer";
    public static final String LIST_CONTAINERS = "listContainers";

    public static final String SERVICE = "service";
    public static final String START_SERVICE = "startService";
    public static final String STOP_SERVICE = "stopService";
    public static final String DEPLOY_SERVICE = "deployService";
    public static final String REMOVE_SERVICE = "removeService";
    public static final String SERVICE_UP = "serviceIsUp";
    public static final String SERVICE_DOWN = "serviceIsDown";
    public static final String LIST_SERVICES = "listServices";


    public static final String SHARED_MEMORY_KEY = "clara/shmkey";
    public static final String ALIVE = "alive";

    public static final String TOPIC_SEP = ":";
    public static final String DATA_SEP = "?";
    public static final String LANG_SEP = "_";
    public static final String PRXHOSTPORT_SEP = "%";

    public static final int BENCHMARK = 10000;

    public static final String JAVA_LANG = "java";
    public static final String PYTHON_LANG = "python";
    public static final String CPP_LANG = "cpp";

    public static final String UNDEFINED = "undefined";

}
