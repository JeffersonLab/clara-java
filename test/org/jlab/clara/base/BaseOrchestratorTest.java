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

package org.jlab.clara.base;

import java.net.SocketException;

import javax.annotation.ParametersAreNonnullByDefault;

import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.sys.CBase;
import org.jlab.coda.xmsg.excp.xMsgException;
import org.junit.Before;

import static org.mockito.Mockito.mock;

public class BaseOrchestratorTest {

    private CBase baseMock;
    private BaseOrchestrator orchestrator;

    @Before
    public void setUp() throws Exception {
        baseMock = mock(CBase.class);
        orchestrator = new OrchestratorMock();
    }


    @ParametersAreNonnullByDefault
    private class OrchestratorMock extends BaseOrchestrator {
        public OrchestratorMock() throws ClaraException {
            super();
        }

        @Override
        CBase getClaraBase(String frontEndHost) throws SocketException, xMsgException {
            return baseMock;
        }
    }
}
