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

package org.jlab.clara.examples.engines;

import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.base.core.ClaraConstants;
import org.jlab.clara.engine.Engine;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;

import java.util.Set;

/**
 * User engine class example.
 *
 * @author gurjyan
 * @version 1.x
 * @since 2/9/15
 */
public class E1 implements Engine {
    private long nr = 0;
    private long t1;
    private long t2;

    @Override
    public EngineData execute(EngineData x) {
        if (nr == 0) {
            t1 = System.currentTimeMillis();
        }
        nr = nr + 1;
        if (nr >= ClaraConstants.BENCHMARK) {
            t2 = System.currentTimeMillis();
            long dt = t2 - t1;
            double pt = (double) dt / (double) nr;
            long pr = (nr * 1000) / dt;
            System.out.println("E1 processing time = " + pt + " ms");
            System.out.println("E1 rate = " + pr + " Hz");
            nr = 0;
        }
        return x;
    }

    @Override
    public EngineData executeGroup(Set<EngineData> x) {
        System.out.println("E1 engine group execute...");
        return x.iterator().next();
    }

    @Override
    public EngineData configure(EngineData x) {
        System.out.println("E1 engine configure...");
        return x;
    }

    @Override
    public Set<String> getStates() {
        return null;
    }

    @Override
    public Set<EngineDataType> getInputDataTypes() {
        return ClaraUtil.buildDataTypes(EngineDataType.STRING);
    }

    @Override
    public Set<EngineDataType> getOutputDataTypes() {
        return ClaraUtil.buildDataTypes(EngineDataType.STRING);
    }

    @Override
    public String getDescription() {
        return "Sample service E1";
    }

    @Override
    public String getVersion() {
        return "1.0";
    }

    @Override
    public String getAuthor() {
        return "Vardan Gyurjyan";
    }

    @Override
    public void reset() {

    }

    @Override
    public void destroy() {

    }
}
