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

package org.jlab.clara.util.xml;

import java.util.ArrayList;
import java.util.List;

/**
 * Class that plays the role of a container for
 * {@link XMLTagValue} objects
 *
 * @author gurjyan
 * @version 4.x
 * @since 4/9/15
 */
public class XMLContainer {
    private List<XMLTagValue> container;

    public XMLContainer() {
        container = new ArrayList<>();
    }

    public List<XMLTagValue> getContainer() {
        return container;
    }

    public void setContainer(List<XMLTagValue> container) {
        this.container = container;
    }

    public void addTagValue(XMLTagValue value) {
        container.add(value);
    }

    @Override
    public String toString() {
        return "XMLContainer{" +
                "container=" + container +
                '}';
    }
}