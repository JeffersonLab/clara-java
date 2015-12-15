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

package org.jlab.clara.sys;

import org.jlab.clara.base.error.ClaraException;
import org.jlab.coda.xmsg.core.xMsgConstants;
import org.jlab.coda.xmsg.core.xMsgMessage;

import java.util.StringTokenizer;

final class RequestParser {

    private final String cmdData;
    private final StringTokenizer tokenizer;

    static RequestParser build(xMsgMessage msg) throws ClaraException {
        String mimeType = msg.getMetaData().getDataType();
        if (mimeType.equals("text/string")) {
            return new RequestParser(new String(msg.getData()));
        }
        throw new ClaraException("Clara-Error: Invalid mime-type = " + mimeType);
    }

    private RequestParser(String data) {
        cmdData = data;
        tokenizer = new StringTokenizer(cmdData, xMsgConstants.DATA_SEP);
    }

    public String nextString() throws ClaraException {
            return tokenizer.nextToken();
    }

    public String nextString(String defaultValue) {
        return tokenizer.hasMoreElements() ? tokenizer.nextToken() : defaultValue;
    }

    public int nextInteger() throws ClaraException {
            return Integer.parseInt(tokenizer.nextToken());
    }
}