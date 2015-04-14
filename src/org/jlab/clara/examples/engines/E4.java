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

package org.jlab.clara.examples.engines;

import org.jlab.clara.util.ACEngine;
import org.jlab.clara.util.CConstants;
import org.jlab.clara.util.EngineData;
import org.jlab.coda.xmsg.data.xMsgD;

import java.util.List;

/**
 * <p>
 *     User engine class example
 * </p>
 *
 * @author gurjyan
 * @version 1.x
 * @since 2/9/15
 */
public class E4 extends ACEngine {

    private int nr = 0;
    private long t1;
    private long t2;

    @Override
    public EngineData execute(EngineData x) {
        nr = nr + 1;
        if (nr == 1) {
            t1 = System.currentTimeMillis();
        } else if (nr == CConstants.BENCHMARK) {
            t2 = System.currentTimeMillis();
            System.out.println("E4 rate = " + (CConstants.BENCHMARK * 1000) / (t2 - t1));
            nr = 0;
        }
        return x;
    }

    @Override
    public EngineData execute_group(List<EngineData> x) {
        System.out.println("E4 engine group execute...");
        return x.get(0);
    }

    @Override
    public void configure(EngineData x) {
        System.out.println("E4 engine configure...");
    }

    @Override
    public List<String> get_states() {
        return null;
    }

    @Override
    public String get_current_state() {
        return null;
    }

    @Override
    public xMsgD.Data.DType get_accepted_data_type() {
        return null;
    }

    @Override
    public xMsgD.Data.DType get_returned_data_type() {
        return null;
    }

    @Override
    public String get_description() {
        return ("E4 test engine");
    }

    @Override
    public String get_version() {
        return null;
    }

    @Override
    public String get_author() {
        return null;
    }

    @Override
    public void dispose() {

    }
}
