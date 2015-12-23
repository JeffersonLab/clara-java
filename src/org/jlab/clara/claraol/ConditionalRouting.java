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

package org.jlab.clara.claraol;

import java.util.Collection;

import org.protege.owl.codegeneration.WrappedIndividual;

import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLOntology;

/**
 * 
 * <p>
 * Generated by Protege (http://protege.stanford.edu). <br>
 * Source Class: ConditionalRouting <br>
 * @version generated on Tue Dec 22 14:51:01 EST 2015 by gurjyan
 */

public interface ConditionalRouting extends WrappedIndividual {

    /* ***************************************************
     * Property http://claraweb.jlab.org/ontology/2015/11/ClaraOL#hasCondition
     */
     
    /**
     * Gets all property values for the hasCondition property.<p>
     * 
     * @returns a collection of values for the hasCondition property.
     */
    Collection<? extends Condition> getHasCondition();

    /**
     * Checks if the class has a hasCondition property value.<p>
     * 
     * @return true if there is a hasCondition property value.
     */
    boolean hasHasCondition();

    /**
     * Adds a hasCondition property value.<p>
     * 
     * @param newHasCondition the hasCondition property value to be added
     */
    void addHasCondition(Condition newHasCondition);

    /**
     * Removes a hasCondition property value.<p>
     * 
     * @param oldHasCondition the hasCondition property value to be removed.
     */
    void removeHasCondition(Condition oldHasCondition);


    /* ***************************************************
     * Property http://claraweb.jlab.org/ontology/2015/11/ClaraOL#ifFalseSend
     */
     
    /**
     * Gets all property values for the ifFalseSend property.<p>
     * 
     * @returns a collection of values for the ifFalseSend property.
     */
    Collection<? extends Service> getIfFalseSend();

    /**
     * Checks if the class has a ifFalseSend property value.<p>
     * 
     * @return true if there is a ifFalseSend property value.
     */
    boolean hasIfFalseSend();

    /**
     * Adds a ifFalseSend property value.<p>
     * 
     * @param newIfFalseSend the ifFalseSend property value to be added
     */
    void addIfFalseSend(Service newIfFalseSend);

    /**
     * Removes a ifFalseSend property value.<p>
     * 
     * @param oldIfFalseSend the ifFalseSend property value to be removed.
     */
    void removeIfFalseSend(Service oldIfFalseSend);


    /* ***************************************************
     * Property http://claraweb.jlab.org/ontology/2015/11/ClaraOL#ifTrueSend
     */
     
    /**
     * Gets all property values for the ifTrueSend property.<p>
     * 
     * @returns a collection of values for the ifTrueSend property.
     */
    Collection<? extends Service> getIfTrueSend();

    /**
     * Checks if the class has a ifTrueSend property value.<p>
     * 
     * @return true if there is a ifTrueSend property value.
     */
    boolean hasIfTrueSend();

    /**
     * Adds a ifTrueSend property value.<p>
     * 
     * @param newIfTrueSend the ifTrueSend property value to be added
     */
    void addIfTrueSend(Service newIfTrueSend);

    /**
     * Removes a ifTrueSend property value.<p>
     * 
     * @param oldIfTrueSend the ifTrueSend property value to be removed.
     */
    void removeIfTrueSend(Service oldIfTrueSend);


    /* ***************************************************
     * Common interfaces
     */

    OWLNamedIndividual getOwlIndividual();

    OWLOntology getOwlOntology();

    void delete();

}
