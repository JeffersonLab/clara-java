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

import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLOntology;

/**
 * 
 * <p>
 * Generated by Protege (http://protege.stanford.edu). <br>
 * Source Class: EngineConfig <br>
 * @version generated on Tue Dec 22 14:51:01 EST 2015 by gurjyan
 */

public interface EngineConfig extends Config {

    /* ***************************************************
     * Property http://claraweb.jlab.org/ontology/2015/11/ClaraOL#hasData
     */
     
    /**
     * Gets all property values for the hasData property.<p>
     * 
     * @returns a collection of values for the hasData property.
     */
    Collection<? extends DataSource> getHasData();

    /**
     * Checks if the class has a hasData property value.<p>
     * 
     * @return true if there is a hasData property value.
     */
    boolean hasHasData();

    /**
     * Adds a hasData property value.<p>
     * 
     * @param newHasData the hasData property value to be added
     */
    void addHasData(DataSource newHasData);

    /**
     * Removes a hasData property value.<p>
     * 
     * @param oldHasData the hasData property value to be removed.
     */
    void removeHasData(DataSource oldHasData);


    /* ***************************************************
     * Property http://claraweb.jlab.org/ontology/2015/11/ClaraOL#hasInputData
     */
     
    /**
     * Gets all property values for the hasInputData property.<p>
     * 
     * @returns a collection of values for the hasInputData property.
     */
    Collection<? extends DataSource> getHasInputData();

    /**
     * Checks if the class has a hasInputData property value.<p>
     * 
     * @return true if there is a hasInputData property value.
     */
    boolean hasHasInputData();

    /**
     * Adds a hasInputData property value.<p>
     * 
     * @param newHasInputData the hasInputData property value to be added
     */
    void addHasInputData(DataSource newHasInputData);

    /**
     * Removes a hasInputData property value.<p>
     * 
     * @param oldHasInputData the hasInputData property value to be removed.
     */
    void removeHasInputData(DataSource oldHasInputData);


    /* ***************************************************
     * Property http://claraweb.jlab.org/ontology/2015/11/ClaraOL#hasOutputData
     */
     
    /**
     * Gets all property values for the hasOutputData property.<p>
     * 
     * @returns a collection of values for the hasOutputData property.
     */
    Collection<? extends DataSource> getHasOutputData();

    /**
     * Checks if the class has a hasOutputData property value.<p>
     * 
     * @return true if there is a hasOutputData property value.
     */
    boolean hasHasOutputData();

    /**
     * Adds a hasOutputData property value.<p>
     * 
     * @param newHasOutputData the hasOutputData property value to be added
     */
    void addHasOutputData(DataSource newHasOutputData);

    /**
     * Removes a hasOutputData property value.<p>
     * 
     * @param oldHasOutputData the hasOutputData property value to be removed.
     */
    void removeHasOutputData(DataSource oldHasOutputData);


    /* ***************************************************
     * Common interfaces
     */

    OWLNamedIndividual getOwlIndividual();

    OWLOntology getOwlOntology();

    void delete();

}
