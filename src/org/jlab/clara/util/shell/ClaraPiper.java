package org.jlab.clara.util.shell;

import org.jlab.clara.util.ClaraUtil;
import org.jlab.clara.util.log.ClaraLogger;

/**
 * Created by gurjyan on 10/9/15.
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