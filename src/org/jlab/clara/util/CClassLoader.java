package org.jlab.clara.util;

import org.jlab.clara.base.CException;

/**
 * <p>
 *     Clara dynamic class loader
 * </p>
 *
 * @author gurjyan
 * @version 1.x
 * @since 2/9/15
 */
public class CClassLoader {

    private ClassLoader classLoader;

    public CClassLoader(ClassLoader cl){
        classLoader = cl;
    }

    public ACEngine load(String className)
            throws CException, ClassNotFoundException, IllegalAccessException, InstantiationException {
            Class aClass = classLoader.loadClass(className);

            Object aInstance = aClass.newInstance();

            if(aInstance instanceof ACEngine){
                return (ACEngine)aInstance;
            } else {
                throw new CException("not a Clara service engine");
            }
    }

}
