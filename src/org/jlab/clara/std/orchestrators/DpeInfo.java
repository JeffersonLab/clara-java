/*
 *   Copyright (c) 2017.  Jefferson Lab (JLab). All rights reserved. Permission
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

package org.jlab.clara.std.orchestrators;

import org.jlab.clara.base.DpeName;

/**
 * Stores properties of a DPE.
 * <p>
 * Currently, these properties are:
 * <ul>
 * <li>name (IP address)
 * <li>number of cores
 * <li>value of {@code $CLARA_HOME}
 * </ul>
 */
class DpeInfo {

    final DpeName name;
    final int cores;
    final String claraHome;

    static final String DEFAULT_CLARA_HOME = System.getenv("CLARA_HOME");

    DpeInfo(DpeName name, int cores, String claraHome) {
        if (name == null) {
            throw new IllegalArgumentException("Null DPE name");
        }
        if (cores < 0) {
            throw new IllegalArgumentException("Invalid number of cores");
        }
        this.name = name;
        this.cores = cores;
        this.claraHome = claraHome;
    }


    DpeInfo(String name, int cores, String claraHome) {
        this(new DpeName(name), cores, claraHome);
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + name.hashCode();
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof DpeInfo)) {
            return false;
        }
        DpeInfo other = (DpeInfo) obj;
        if (!name.equals(other.name)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "[" + name + "," + cores + "]";
    }
}
