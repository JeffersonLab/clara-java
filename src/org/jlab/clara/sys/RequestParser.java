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

import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.coda.xmsg.core.xMsgMessage;
import org.jlab.coda.xmsg.data.xMsgM.xMsgMetaOrBuilder;

import java.util.NoSuchElementException;
import java.util.StringTokenizer;

final class RequestParser {

    private final xMsgMetaOrBuilder cmdMeta;
    private final String cmdData;
    private final StringTokenizer tokenizer;


    private RequestParser(xMsgMetaOrBuilder meta, String data) {
        cmdMeta = meta;
        cmdData = data;
        tokenizer = new StringTokenizer(cmdData, ClaraConstants.DATA_SEP);
    }

    static RequestParser build(xMsgMessage msg) throws RequestException {
        String mimeType = msg.getMimeType();
        if (mimeType.equals("text/string")) {
            return new RequestParser(msg.getMetaData(), new String(msg.getData()));
        }
        throw new RequestException("Invalid mime-type = " + mimeType);
    }

    public String nextString() throws RequestException {
        try {
            return tokenizer.nextToken();
        } catch (NoSuchElementException e) {
            throw new RequestException(invalidRequestMsg() + ": " + cmdData);
        }
    }

    public String nextString(String defaultValue) {
        return tokenizer.hasMoreElements() ? tokenizer.nextToken() : defaultValue;
    }

    public int nextInteger() throws RequestException {
        try {
            return Integer.parseInt(tokenizer.nextToken());
        } catch (NoSuchElementException | NumberFormatException e) {
            throw new RequestException(invalidRequestMsg() + ": " + cmdData);
        }
    }

    public String request() {
        return cmdData;
    }

    private String invalidRequestMsg() {
        StringBuilder sb = new StringBuilder();
        sb.append("Invalid request");
        if (cmdMeta.hasAuthor()) {
            sb.append(" from author = ").append(cmdMeta.getAuthor());
        }
        return sb.toString();
    }


    static class RequestException extends Exception {
        RequestException(String msg) {
            super(msg);
        }
    }
}
