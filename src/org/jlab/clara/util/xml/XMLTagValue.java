package org.jlab.clara.util.xml;

/**
 * Describe.....
 *
 * @author gurjyan
 * @version 1.x
 * @since 4/9/15
 */
public class XMLTagValue {
    private String tag;
    private String value;

    public XMLTagValue(String tag, String value) {
        this.tag = tag;
        this.value = value;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "XMLTagValue{" +
                "tag='" + tag + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}