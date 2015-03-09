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

    public static final String DPE = "dpe";
    public static final String DPE_UP = "dpeIsUp";
    public static final String DPE_DOWN = "dpeIsDown";
    public static final String DPE_PING = "dpePing";
    public static final String START_CONTAINER = "startContainer";
    public static final String REMOVE_CONTAINER = "removeContainer";

    public static final String CONTAINER = "container";
    public static final String CONTAINER_UP = "containerIsUp";
    public static final String CONTAINER_DOWN = "containerIsDown";
    public static final String DEPLOY_SERVICE = "deployService";
    public static final String REMOVE_SERVICE = "removeService";
    public static final String RUN_SERVICE = "runService";

    public static final String SERVICE = "service";
    public static final String SERVICE_UP = "serviceIsUp";
    public static final String SERVICE_DOWN = "serviceIsDown";

    public static final String ALIVE = "alive";

    public static enum CDataType {
        T_VLSINT32,
        T_VLSINT64,
        T_FLSINT32,
        T_FLSINT64,
        T_FLOAT,
        T_DOUBLE,
        T_STRING,
        T_BYTES,
        T_VLSINT32A,
        T_VLSINT64A,
        T_FLSINT32A,
        T_FLSINT64A,
        T_FLOATA,
        T_DOUBLEA,
        T_STRINGA,
        T_BYTESA,
        T_PAYLOAD,
        JOBJECT,
        COBJECT,
        POBJECT,
        NETCDF,
        HDF,
        EVIO
    }

}
