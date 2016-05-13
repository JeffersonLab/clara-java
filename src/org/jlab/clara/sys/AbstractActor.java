/*
 * Copyright (C) 2015. Jefferson Lab, CLARA framework (JLAB). All Rights Reserved.
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for educational, research, and not-for-profit purposes,
 * without fee and without a signed licensing agreement.
 *
 * Contact Vardan Gyurjyan
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

package org.jlab.clara.sys;

import org.jlab.clara.base.core.ClaraBase;
import org.jlab.clara.base.core.ClaraComponent;
import org.jlab.clara.base.error.ClaraException;

abstract class AbstractActor implements AutoCloseable {

    protected final ClaraBase base;

    AbstractActor(ClaraComponent component, ClaraComponent fe) {
        this.base = new ClaraBase(component, fe) {
            @Override
            public void start() throws ClaraException { }

            @Override
            protected void end() { }
        };
    }

    public void start() throws ClaraException {
        initialize();
    }

    @Override
    public void close() {
        end();
        base.close();
    }

    /**
     * Initializes the CLARA actor.
     */
    protected abstract void initialize() throws ClaraException;

    /**
     * Runs before closing the actor.
     */
    protected abstract void end();
}
