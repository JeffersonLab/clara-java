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

import org.jlab.clara.base.ClaraLang;

/**
 * Stores the general properties of a service.
 * <p>
 * Currently, these properties are:
 * <ul>
 * <li>name (ex: {@code ECReconstruction})
 * <li>the full classpath (ex: {@code org.jlab.clas12.ec.services.ECReconstruction})
 * <li>the container where the service should be deployed (ex: {@code ec-cont})
 * <li>the language of the service
 * </ul>
 * Note that this class doesn't represent a deployed service in a DPE, but a
 * template that keeps the name and container of the service. Orchestrators should
 * use the data of this class combined with the values in {@link DpeInfo} to
 * fully identify individual deployed services (i.e. the canonical name).
 */
class ServiceInfo {

    final String name;
    final String classpath;
    final String cont;
    final ClaraLang lang;


    ServiceInfo(String classpath, String cont, String name, ClaraLang lang) {
        if (classpath == null) {
            throw new IllegalArgumentException("Null service classpath name");
        }
        if (cont == null) {
            throw new IllegalArgumentException("Null container name");
        }
        if (name == null) {
            throw new IllegalArgumentException("Null service name");
        }
        if (lang == null) {
            throw new IllegalArgumentException("Null service language");
        }
        this.classpath = classpath;
        this.cont = cont;
        this.name = name;
        this.lang = lang;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + lang.hashCode();
        result = prime * result + cont.hashCode();
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
        if (!(obj instanceof ServiceInfo)) {
            return false;
        }
        ServiceInfo other = (ServiceInfo) obj;
        if (!lang.equals(other.lang)) {
            return false;
        }
        if (!cont.equals(other.cont)) {
            return false;
        }
        if (!name.equals(other.name)) {
            return false;
        }
        return true;
    }
}
