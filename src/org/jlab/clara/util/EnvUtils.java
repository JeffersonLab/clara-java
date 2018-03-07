/*
 *   Copyright (c) 2018.  Jefferson Lab (JLab). All rights reserved. Permission
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

package org.jlab.clara.util;

public final class EnvUtils {

    private EnvUtils() { }

    /**
     * Gets the value of the CLARA_HOME environment variable.
     *
     * @return the CLARA home directory
     */
    public static String claraHome() {
        String claraHome = System.getenv("CLARA_HOME");
        if (claraHome == null) {
            throw new RuntimeException("Missing CLARA_HOME enviroment variable");
        }
        return claraHome;
    }

    /**
     * Gets the user account name.
     *
     * @return the account name of the user running CLARA.
     */
    public static String userName() {
        String userName = System.getProperty("user.name");
        if (userName == null) {
            throw new RuntimeException("Missing 'user.name' system property");
        }
        return userName;
    }

    /**
     * Gets the user home directory.
     *
     * @return the home directory of the user running CLARA.
     */
    public static String userHome() {
        String userHome = System.getProperty("user.home");
        if (userHome == null) {
            throw new RuntimeException("Missing 'user.home' system property");
        }
        return userHome;
    }
}
