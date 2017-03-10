package org.jlab.clara.std.orchestrators;

import org.jlab.clara.base.DpeName;

/**
 * Stores properties of a DPE.
 * <p>
 * Currently, these properties are:
 * <ul>
 * <li>name (IP address)
 * <li>number of cores
 * <li>value of {@code $CLARA_HOME}
 * </ul>
 */
class DpeInfo {

    final DpeName name;
    final int cores;
    final String claraHome;

    static final String DEFAULT_CLARA_HOME = System.getenv("CLARA_HOME");

    DpeInfo(DpeName name, int cores, String claraHome) {
        if (name == null) {
            throw new IllegalArgumentException("Null DPE name");
        }
        if (cores < 0) {
            throw new IllegalArgumentException("Invalid number of cores");
        }
        this.name = name;
        this.cores = cores;
        this.claraHome = claraHome;
    }


    DpeInfo(String name, int cores, String claraHome) {
        this(new DpeName(name), cores, claraHome);
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + name.hashCode();
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof DpeInfo)) {
            return false;
        }
        DpeInfo other = (DpeInfo) obj;
        if (!name.equals(other.name)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "[" + name + "," + cores + "]";
    }
}
