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

package org.jlab.clara.engine;

/**
 * Enum for engine data types
 *
 * @author gurjyan
 * @version 1.x
 * @since 5/14/15
 */
public enum EDataType {
    UNDEFINED,
    J_Object,
    P_Object,
    NCDFS_Object,
    T_VLSINT32,
    T_VLSINT64 ,
    T_FLSINT32 ,
    T_FLSINT64 ,
    T_FLOAT ,
    T_DOUBLE ,
    T_STRING ,
    T_BYTES ,
    T_VLSINT32A ,
    T_VLSINT64A ,
    T_FLSINT32A ,
    T_FLSINT64A ,
    T_FLOATA ,
    T_DOUBLEA ,
    T_STRINGA ,
    T_BYTESA ,
    T_PAYLOAD;

}
