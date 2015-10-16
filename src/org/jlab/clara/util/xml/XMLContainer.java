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