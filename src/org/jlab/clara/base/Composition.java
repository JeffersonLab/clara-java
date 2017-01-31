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

package org.jlab.clara.base;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;


/**
 * A CLARA composition of services.
 * The orchestrator should send a request to the first service of the
 * composition, and the output of each service will be sent to the next service
 * in the composition, until all are executed.
 * <p>
 * The result of each service can be compared against given states to provide
 * custom routing logic.
 */
public class Composition {

    private List<String> allServices = new ArrayList<>();
    private String text;

    /**
     * Parses a composition from the given string.
     *
     * @param composition a string defining a valid composition
     */
    public Composition(String composition) {
        text = composition;

        // TODO: doesn't handle conditionals
        StringTokenizer st = new StringTokenizer(composition, "+;&,");
        while (st.hasMoreTokens()) {
            allServices.add(st.nextToken().trim());
        }
    }

    /**
     * Gets the first service of this composition.
     * This is the service that starts the composition.
     *
     * @return the canonical name of the first service
     */
    public String firstService() {
        return allServices.get(0);
    }

    @Override
    public String toString() {
        return text;
    }
}
