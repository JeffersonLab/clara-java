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

import org.jlab.coda.xmsg.data.xMsgM;

/**
 *
 * @author smancill
 * @since 4.x
 */
public abstract class EngineDataAccessor {

    // CHECKSTYLE.OFF: StaticVariableName
    private static volatile EngineDataAccessor DEFAULT;
    // CHECKSTYLE.ON: StaticVariableName

    public static EngineDataAccessor getDefault() {
        new EngineData(); // Load the accessor
        EngineDataAccessor a = DEFAULT;
        if (a == null) {
            throw new IllegalStateException("EngineDataAccessor should not be null");
        }
        return a;
    }

    public static void setDefault(EngineDataAccessor accessor) {
        if (DEFAULT != null) {
            throw new IllegalStateException("EngineDataAccessor should be null");
        }
        DEFAULT = accessor;
    }

    protected abstract xMsgM.xMsgMeta.Builder getMetadata(EngineData data);

    protected abstract EngineData build(Object data, xMsgM.xMsgMeta.Builder metadata);
}
