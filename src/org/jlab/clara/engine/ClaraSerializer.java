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

package org.jlab.clara.engine;

import org.jlab.clara.base.error.ClaraException;

import java.nio.ByteBuffer;

/**
 * Provides the custom serialization methods to send user defined data through
 * the network.
 */
public interface ClaraSerializer {

    /**
     * Serializes the user object into a byte buffer and returns it.
     *
     * @param data the user object stored on the {@link EngineData}
     * @throws ClaraException if the data could not be serialized
     * @return the serialized user object
     */
    ByteBuffer write(Object data) throws ClaraException;

    /**
     * De-serializes the byte buffer into the user object and returns it.
     *
     * @param buffer the serialized data
     * @throws ClaraException if the data could not be deserialized
     * @return the user-object
     */
    Object read(ByteBuffer buffer) throws ClaraException;
}
