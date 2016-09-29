package org.jlab.clara.examples.engines;

import org.jlab.clara.base.ClaraUtil;
import org.jlab.clara.engine.Engine;
import org.jlab.clara.engine.EngineData;
import org.jlab.clara.engine.EngineDataType;

import java.util.Set;

/**
 * Simple factorial engine
 * <p>
 *
 * @author gurjyan
 *         Date 9/26/16
 * @version 3.x
 */
public class Factorial1 implements Engine {

    private double fact(double n){
        double result;
        if(n==1) return 1;
        result = fact(n-1) * n;
        return result;
    }

    @Override
    public EngineData configure(EngineData input) {
        return null;
    }

    @Override
    public EngineData execute(EngineData input) {
        for(int i =0; i<1000; i++) {
            fact(7778);
        }
        return input;
    }

    @Override
    public EngineData executeGroup(Set<EngineData> inputs) {
        return null;
    }

    @Override
    public Set<EngineDataType> getInputDataTypes() {
        return ClaraUtil.buildDataTypes(EngineDataType.STRING);
    }

    @Override
    public Set<EngineDataType> getOutputDataTypes() {
        return ClaraUtil.buildDataTypes(EngineDataType.STRING);
    }

    @Override
    public Set<String> getStates() {
        return null;
    }

    @Override
    public String getDescription() {
        return "Sample service E1";
    }

    @Override
    public String getVersion() {
        return "1.0";
    }

    @Override
    public String getAuthor() {
        return "Vardan Gyurgyan";
    }
    @Override
    public void reset() {

    }

    @Override
    public void destroy() {

    }
}
