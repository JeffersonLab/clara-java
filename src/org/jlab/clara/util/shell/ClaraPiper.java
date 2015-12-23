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
package org.jlab.clara.util.shell;

import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.util.log.ClaraLogger;

/**
 * @author gurjyan
 * @version 4.x
 */
public class ClaraPiper implements java.lang.Runnable {

    private java.io.InputStream input;

    private java.io.OutputStream output;

    // local instance of the logger object
    private ClaraLogger lg = ClaraLogger.getInstance();

    public ClaraPiper(java.io.InputStream input,
                      java.io.OutputStream output) {
        this.input = input;
        this.output = output;
    }

    public void run() {
        try {
            // Create 512 bytes buffer
            byte[] b = new byte[512];
            int read = 1;
            // As long as data is read; -1 means EOF
            while (read > -1) {
                // Read bytes into buffer
                read = input.read(b, 0, b.length);
                if (read > -1) {
                    // Write bytes to output
                    output.write(b, 0, read);
                }
            }
        } catch (Exception e) {
            // Something happened while reading
            // or writing streams; fork_pipe is broken
            throw new RuntimeException("Broken fork_pipe", e);
        } finally {
            try {
                input.close();
                output.close();
            } catch (Exception e) {
                lg.logger.severe(ClaraUtil.stack2str(e));
            }
        }
    }
}