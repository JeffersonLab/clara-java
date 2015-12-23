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
package org.jlab.clara.util.log;

import java.io.File;
import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * @author gurjyan
 * @version 4.x
 */
public class ClaraLogger {

    private static ClaraLogger instance = null;
    private static String logFileDir =
            System.getenv("CLARA_LOG");
    public final Logger logger = Logger.getLogger("Clara");

    public static ClaraLogger getInstance(){
        if(instance == null) {

            instance = new ClaraLogger();
            instance.initLogger();
        }
        return instance;
    }

    private void initLogger(){

        FileHandler myFileHandler;
        File f = new File(logFileDir);

        if(!f.exists()){
            if (f.mkdirs()) System.out.println("Clara-Error: Can not create the log file.");
        }

        try {
            myFileHandler = new FileHandler(logFileDir+ File.separator+"clara.log");
            myFileHandler.setFormatter(new SimpleFormatter());
            logger.addHandler(myFileHandler);
            logger.setUseParentHandlers(false);
            logger.setLevel(Level.FINEST);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }


}
