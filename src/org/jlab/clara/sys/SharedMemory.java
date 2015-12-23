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

package org.jlab.clara.sys;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jlab.clara.engine.EngineData;

final class SharedMemory {

    private SharedMemory() { }

    /*
      key = <receiver-service>
      value = map where:
          key = <sender-service>:<communication-id>,
          value = EngineData object
    */
    // CHECKSTYLE.OFF: ConstantName
    private static final Map<String, Map<String, EngineData>> sharedData =
            new ConcurrentHashMap<>();


    static void putEngineData(String receiver, String sender, int id, EngineData data) {
        Map<String, EngineData> inputs = sharedData.get(receiver);
        if (inputs != null) {
            String key = sender + ":" + id;
            inputs.put(key, data);
        } else {
            throw new IllegalStateException("Receiver not registered: " + receiver);
        }
    }


    static EngineData getEngineData(String receiver, String sender, int id) {
        Map<String, EngineData> inputs = sharedData.get(receiver);
        EngineData data = null;
        if (inputs != null) {
            String key = sender + ":" + id;
            data = inputs.get(key);
            inputs.remove(key);
        }
        return data;
    }

    static void addReceiver(String receiver) {
        sharedData.put(receiver, new ConcurrentHashMap<String, EngineData>());
    }

    static void removeReceiver(String receiver) {
        sharedData.remove(receiver);
    }

    static boolean containsReceiver(String receiver) {
        return sharedData.containsKey(receiver);
    }
}
