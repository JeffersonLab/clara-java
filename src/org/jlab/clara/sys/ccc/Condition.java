/*
 * Copyright (C) 2015. Jefferson Lab, CLARA framework (JLAB). All Rights Reserved.
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for educational, research, and not-for-profit purposes,
 * without fee and without a signed licensing agreement.
 *
 * Author Vardan Gyurjyan
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
package org.jlab.clara.sys.ccc;

import java.util.HashSet;
import java.util.Set;

/**
 * <p>
 *     Defines it's own as well as all input service states that if
 *     exists in run-time will make this CLARA composition condition
 *     a true condition.
 * </p>
 *
 * @author gurjyan
 * @version 1.x
 * @since 5/21/15
 */
public class Condition {

    // Required state of the service for this condition to be true
    private ServiceState state;

    // Required NOT state of the service for this condition to be true
    private ServiceState notState;

    // States of services that are required to be present in order this condition to be true
    private Set<ServiceState> andStates = new HashSet<>();

    // NOT states of services that are required to be present in order this condition to be true
    private Set<ServiceState> andNotStates = new HashSet<>();

    // Required states of services that will make this statement true
    private Set<ServiceState> orStates = new HashSet<>();

    // The name of the service that this condition is relevant to.
    private String serviceName;

    public Condition(String serviceName){
        this.serviceName = serviceName;
    }

    public String getServiceName() {
        return serviceName;
    }

    public ServiceState getState() {
        return state;
    }

    public void setState(ServiceState state) {
        this.state = state;
    }

    public ServiceState getNotState() {
        return notState;
    }

    public void setNotState(ServiceState notState) {
        this.notState = notState;
    }

    public Set<ServiceState> getAndStates() {
        return andStates;
    }

    public void setAndStates(Set<ServiceState> andStates) {
        this.andStates = andStates;
    }

    public Set<ServiceState> getAndNotStates() {
        return andNotStates;
    }

    public void setAndNotStates(Set<ServiceState> andNotStates) {
        this.andNotStates = andNotStates;
    }

    public Set<ServiceState> getOrStates() {
        return orStates;
    }

    public void setOrStates(Set<ServiceState> orStates) {
        this.orStates = orStates;
    }


    /**
     * <p>
     *     Returns true if passed states make this condition true
     * </p>
     * @param ownState state of this service
     * @param inputStates state of all input services
     * @return true/false
     */
    public boolean isTrue(ServiceState ownState, Set<ServiceState> inputStates){
        boolean checkMyState = (getState()==null) || (ownState.equals(getState()));
        boolean checkMyNotState = (getNotState()==null) || (!ownState.equals(getNotState()));

        boolean checkAnd = checkANDCondition(inputStates);
        boolean checkAndNot = checkANDNotCondition(inputStates);
        boolean checkOr = checkORCondition(inputStates);
        return checkMyState && checkMyNotState && checkAnd && checkAndNot && checkOr;
    }

    private boolean checkANDCondition(Set<ServiceState> j){
        boolean b;
        if(getAndStates().isEmpty()){
            b = true;
        } else {
            if(getAndStates().size()==j.size()) {
                b = true;
                for (ServiceState ss : getAndStates()) {
                    for (ServiceState ssi : j) {
                        if (!ss.equals(ssi)) {
                            b = false;
                            break;
                        }
                    }
                }
            } else {
                b = false;
            }
        }
        return b;
    }

    private boolean checkANDNotCondition(Set<ServiceState> j){
        boolean b;
        if(getAndNotStates().isEmpty()){
            b = true;
        } else {
            if(getAndNotStates().size()==j.size()) {
                b = true;
                for (ServiceState ss : getAndNotStates()) {
                    for (ServiceState ssi : j) {
                        if (ss.equals(ssi)) {
                            b = false;
                            break;
                        }
                    }
                }
            } else {
                b = false;
            }
        }
        return b;
    }

    private boolean checkORCondition(Set<ServiceState> j){
        boolean b;
        if(getOrStates().isEmpty()){
            b = true;
        } else {
            b = false;
                for (ServiceState ssi : j) {
                    for (ServiceState ss : getOrStates()) {
                        if (ss.equals(ssi)) {
                            b = true;
                            break;
                        }
                    }
                }
        }
        return b;
    }
}
