package org.jlab.clara.std.orchestrators;

import org.jlab.clara.base.ServiceName;

class DeployInfo {

    final ServiceName name;
    final String classPath;
    final int poolSize;

    DeployInfo(ServiceName name, String classPath, int poolSize) {
        this.name = name;
        this.classPath = classPath;
        this.poolSize = poolSize;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + classPath.hashCode();
        result = prime * result + name.hashCode();
        result = prime * result + poolSize;
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
        if (getClass() != obj.getClass()) {
            return false;
        }
        DeployInfo other = (DeployInfo) obj;
        if (!classPath.equals(other.classPath)) {
            return false;
        }
        if (!name.equals(other.name)) {
            return false;
        }
        if (poolSize != other.poolSize) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return String.format("[name=%s, classPath=%s, poolSize=%d]", name, classPath, poolSize);
    }
}
