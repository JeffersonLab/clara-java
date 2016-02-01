package org.jlab.clara.claraol.impl;


import java.util.Collection;

import org.jlab.clara.claraol.Condition;
import org.jlab.clara.claraol.Service;
import org.jlab.clara.claraol.State;
import org.jlab.clara.claraol.Vocabulary;
import org.protege.owl.codegeneration.impl.WrappedIndividualImpl;

import org.protege.owl.codegeneration.inference.CodeGenerationInference;

import org.semanticweb.owlapi.model.IRI;


/**
 * Generated by Protege (http://protege.stanford.edu).<br>
 * Source Class: DefaultCondition <br>
 * @version generated on Sat Jan 30 17:43:59 EST 2016 by gurjyan
 */
public class DefaultCondition extends WrappedIndividualImpl implements Condition {

    public DefaultCondition(CodeGenerationInference inference, IRI iri) {
        super(inference, iri);
    }





    /* ***************************************************
     * Object Property http://claraweb.jlab.org/ontology/2015/11/ClaraOL#executionState
     */
     
    public Collection<? extends State> getExecutionState() {
        return getDelegate().getPropertyValues(getOwlIndividual(),
                                               Vocabulary.OBJECT_PROPERTY_EXECUTIONSTATE,
                                               DefaultState.class);
    }

    public boolean hasExecutionState() {
	   return !getExecutionState().isEmpty();
    }

    public void addExecutionState(State newExecutionState) {
        getDelegate().addPropertyValue(getOwlIndividual(),
                                       Vocabulary.OBJECT_PROPERTY_EXECUTIONSTATE,
                                       newExecutionState);
    }

    public void removeExecutionState(State oldExecutionState) {
        getDelegate().removePropertyValue(getOwlIndividual(),
                                          Vocabulary.OBJECT_PROPERTY_EXECUTIONSTATE,
                                          oldExecutionState);
    }


    /* ***************************************************
     * Object Property http://claraweb.jlab.org/ontology/2015/11/ClaraOL#ifFalseSend
     */
     
    public Collection<? extends Service> getIfFalseSend() {
        return getDelegate().getPropertyValues(getOwlIndividual(),
                                               Vocabulary.OBJECT_PROPERTY_IFFALSESEND,
                                               DefaultService.class);
    }

    public boolean hasIfFalseSend() {
	   return !getIfFalseSend().isEmpty();
    }

    public void addIfFalseSend(Service newIfFalseSend) {
        getDelegate().addPropertyValue(getOwlIndividual(),
                                       Vocabulary.OBJECT_PROPERTY_IFFALSESEND,
                                       newIfFalseSend);
    }

    public void removeIfFalseSend(Service oldIfFalseSend) {
        getDelegate().removePropertyValue(getOwlIndividual(),
                                          Vocabulary.OBJECT_PROPERTY_IFFALSESEND,
                                          oldIfFalseSend);
    }


    /* ***************************************************
     * Object Property http://claraweb.jlab.org/ontology/2015/11/ClaraOL#ifTrueSend
     */
     
    public Collection<? extends Service> getIfTrueSend() {
        return getDelegate().getPropertyValues(getOwlIndividual(),
                                               Vocabulary.OBJECT_PROPERTY_IFTRUESEND,
                                               DefaultService.class);
    }

    public boolean hasIfTrueSend() {
	   return !getIfTrueSend().isEmpty();
    }

    public void addIfTrueSend(Service newIfTrueSend) {
        getDelegate().addPropertyValue(getOwlIndividual(),
                                       Vocabulary.OBJECT_PROPERTY_IFTRUESEND,
                                       newIfTrueSend);
    }

    public void removeIfTrueSend(Service oldIfTrueSend) {
        getDelegate().removePropertyValue(getOwlIndividual(),
                                          Vocabulary.OBJECT_PROPERTY_IFTRUESEND,
                                          oldIfTrueSend);
    }


    /* ***************************************************
     * Object Property http://claraweb.jlab.org/ontology/2015/11/ClaraOL#receivedState
     */
     
    public Collection<? extends State> getReceivedState() {
        return getDelegate().getPropertyValues(getOwlIndividual(),
                                               Vocabulary.OBJECT_PROPERTY_RECEIVEDSTATE,
                                               DefaultState.class);
    }

    public boolean hasReceivedState() {
	   return !getReceivedState().isEmpty();
    }

    public void addReceivedState(State newReceivedState) {
        getDelegate().addPropertyValue(getOwlIndividual(),
                                       Vocabulary.OBJECT_PROPERTY_RECEIVEDSTATE,
                                       newReceivedState);
    }

    public void removeReceivedState(State oldReceivedState) {
        getDelegate().removePropertyValue(getOwlIndividual(),
                                          Vocabulary.OBJECT_PROPERTY_RECEIVEDSTATE,
                                          oldReceivedState);
    }


    /* ***************************************************
     * Data Property http://claraweb.jlab.org/ontology/2015/11/ClaraOL#logicalRelationship
     */
     
    public Collection<? extends String> getLogicalRelationship() {
		return getDelegate().getPropertyValues(getOwlIndividual(), Vocabulary.DATA_PROPERTY_LOGICALRELATIONSHIP, String.class);
    }

    public boolean hasLogicalRelationship() {
		return !getLogicalRelationship().isEmpty();
    }

    public void addLogicalRelationship(String newLogicalRelationship) {
	    getDelegate().addPropertyValue(getOwlIndividual(), Vocabulary.DATA_PROPERTY_LOGICALRELATIONSHIP, newLogicalRelationship);
    }

    public void removeLogicalRelationship(String oldLogicalRelationship) {
		getDelegate().removePropertyValue(getOwlIndividual(), Vocabulary.DATA_PROPERTY_LOGICALRELATIONSHIP, oldLogicalRelationship);
    }


}
