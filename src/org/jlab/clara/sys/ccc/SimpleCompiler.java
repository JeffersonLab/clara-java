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

package org.jlab.clara.sys.ccc;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.jlab.clara.base.ClaraUtil;

public class SimpleCompiler {

    private final String serviceName;
    private List<String> prev;
    private List<String> next;

    public SimpleCompiler(String serviceName) {
        this.serviceName = serviceName;
    }

    public void compile(String composition) {
        prev = new ArrayList<>();
        next = new ArrayList<>();
        List<String> subComposition = prev;
        boolean serviceFound = false;
        StringTokenizer st = new StringTokenizer(composition, "+");
        while (st.hasMoreTokens()) {
            String service = st.nextToken();
            if (service.equals(serviceName)) {
                subComposition = next;
                serviceFound = true;
                continue;
            }
            if (!ClaraUtil.isCanonicalName(service)) {
                throw new IllegalArgumentException("Invalid composition: " + composition);
            }
            subComposition.add(service);
        }
        if (!serviceFound) {
            throw new IllegalArgumentException(serviceName + " not in: " + composition);
        }
    }

    public Set<String> getOutputs() {
        Set<String> outputs = new HashSet<>();
        if (!next.isEmpty()) {
            outputs.add(next.get(0));
        }
        return outputs;
    }
}
