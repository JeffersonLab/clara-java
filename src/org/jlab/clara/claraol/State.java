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
 * Source Class: State <br>
 * @version generated on Tue Dec 22 14:51:01 EST 2015 by gurjyan
 */

public interface State extends WrappedIndividual {

    /* ***************************************************
     * Property http://claraweb.jlab.org/ontology/2015/11/ClaraOL#hasSeverity
     */
     
    /**
     * Gets all property values for the hasSeverity property.<p>
     * 
     * @return a collection of values for the hasSeverity property.
     */
    Collection<? extends Object> getHasSeverity();

    /**
     * Checks if the class has a hasSeverity property value.<p>
     * 
     * @return true if there is a hasSeverity property value.
     */
    boolean hasHasSeverity();

    /**
     * Adds a hasSeverity property value.<p>
     * 
     * @param newHasSeverity the hasSeverity property value to be added
     */
    void addHasSeverity(Object newHasSeverity);

    /**
     * Removes a hasSeverity property value.<p>
     * 
     * @param oldHasSeverity the hasSeverity property value to be removed.
     */
    void removeHasSeverity(Object oldHasSeverity);



    /* ***************************************************
     * Property http://claraweb.jlab.org/ontology/2015/11/ClaraOL#hasSeverityID
     */
     
    /**
     * Gets all property values for the hasSeverityID property.<p>
     * 
     * @return a collection of values for the hasSeverityID property.
     */
    Collection<? extends Object> getHasSeverityID();

    /**
     * Checks if the class has a hasSeverityID property value.<p>
     * 
     * @return true if there is a hasSeverityID property value.
     */
    boolean hasHasSeverityID();

    /**
     * Adds a hasSeverityID property value.<p>
     * 
     * @param newHasSeverityID the hasSeverityID property value to be added
     */
    void addHasSeverityID(Object newHasSeverityID);

    /**
     * Removes a hasSeverityID property value.<p>
     * 
     * @param oldHasSeverityID the hasSeverityID property value to be removed.
     */
    void removeHasSeverityID(Object oldHasSeverityID);



    /* ***************************************************
     * Property http://claraweb.jlab.org/ontology/2015/11/ClaraOL#hasStateName
     */
     
    /**
     * Gets all property values for the hasStateName property.<p>
     * 
     * @return a collection of values for the hasStateName property.
     */
    Collection<? extends String> getHasStateName();

    /**
     * Checks if the class has a hasStateName property value.<p>
     * 
     * @return true if there is a hasStateName property value.
     */
    boolean hasHasStateName();

    /**
     * Adds a hasStateName property value.<p>
     * 
     * @param newHasStateName the hasStateName property value to be added
     */
    void addHasStateName(String newHasStateName);

    /**
     * Removes a hasStateName property value.<p>
     * 
     * @param oldHasStateName the hasStateName property value to be removed.
     */
    void removeHasStateName(String oldHasStateName);



    /* ***************************************************
     * Common interfaces
     */

    OWLNamedIndividual getOwlIndividual();

    OWLOntology getOwlOntology();

    void delete();

}
