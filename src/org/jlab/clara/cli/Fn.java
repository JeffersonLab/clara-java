package org.jlab.clara.cli;

/**
 * Class description here....
 * <p>
 *
 * @author gurjyan
 *         Date 1/10/17
 * @version 3.x
 */
public interface Fn<I, R> {
    R apply(I input);
}
