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

package org.jlab.clara.sys;

class DpeConfig {

    private final int maxCores;
    private final int poolSize;
    private final long reportPeriod;

    DpeConfig(int maxCores, int poolSize, long reportPeriod) {
        this.maxCores = maxCores;
        this.poolSize = poolSize;
        this.reportPeriod = reportPeriod;
    }

    int maxCores() {
        return maxCores;
    }

    int poolSize() {
        return poolSize;
    }

    long reportPeriod() {
        return reportPeriod;
    }


    static int calculatePoolSize(int cores) {
        int halfCores = cores / 2;
        if (halfCores <= 2) {
            return 2;
        }
        int poolSize = (halfCores % 2 == 0) ? halfCores : halfCores + 1;
        if (poolSize > 16) {
            return 16;
        }
        return poolSize;
    }
}
