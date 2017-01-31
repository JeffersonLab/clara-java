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

package org.jlab.clara.sys;

import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.engine.Engine;
import org.jlab.clara.engine.EngineDataType;

import java.util.Set;

/**
 * CLARA dynamic class loader.
 *
 * @author gurjyan
 * @version 4.x
 * @since 2/9/15
 */
class EngineLoader {

    private ClassLoader classLoader;

    EngineLoader(ClassLoader cl) {
        classLoader = cl;
    }

    public Engine load(String className) throws ClaraException {
        try {
            Class<?> aClass = classLoader.loadClass(className);
            Object aInstance = aClass.newInstance();
            if (aInstance instanceof Engine) {
                Engine engine = (Engine) aInstance;
                validateEngine(engine);
                return engine;
            } else {
                throw new ClaraException("not a CLARA engine: " + className);
            }
        } catch (ClassNotFoundException e) {
            throw new ClaraException("class not found: " + className);
        } catch (IllegalAccessException | InstantiationException e) {
            throw new ClaraException("could not create instance: " + className, e);
        }
    }

    private void validateEngine(Engine engine) throws ClaraException {
        validateDataTypes(engine.getInputDataTypes(), "input data types");
        validateDataTypes(engine.getOutputDataTypes(), "output data types");
        validateString(engine.getDescription(), "description");
        validateString(engine.getVersion(), "version");
        validateString(engine.getAuthor(), "author");
    }

    private void validateString(String value, String field) throws ClaraException {
        if (value == null || value.isEmpty()) {
            throw new ClaraException("missing engine " + field);
        }
    }

    private void validateDataTypes(Set<EngineDataType> types, String field) throws ClaraException {
        if (types == null || types.isEmpty()) {
            throw new ClaraException("missing engine " + field);
        }
        for (EngineDataType dt : types) {
            if (dt == null) {
                throw new ClaraException("null data type on engine " + field);
            }
        }
    }
}
