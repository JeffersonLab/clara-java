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
 * Source Class: DataSource <br>
 * @version generated on Tue Dec 22 14:51:01 EST 2015 by gurjyan
 */

public interface DataSource extends WrappedIndividual {

    /* ***************************************************
     * Property http://claraweb.jlab.org/ontology/2015/11/ClaraOL#hasIdentity
     */
     
    /**
     * Gets all property values for the hasIdentity property.<p>
     * 
     * @return a collection of values for the hasIdentity property.
     */
    Collection<? extends Identity> getHasIdentity();

    /**
     * Checks if the class has a hasIdentity property value.<p>
     * 
     * @return true if there is a hasIdentity property value.
     */
    boolean hasHasIdentity();

    /**
     * Adds a hasIdentity property value.<p>
     * 
     * @param newHasIdentity the hasIdentity property value to be added
     */
    void addHasIdentity(Identity newHasIdentity);

    /**
     * Removes a hasIdentity property value.<p>
     * 
     * @param oldHasIdentity the hasIdentity property value to be removed.
     */
    void removeHasIdentity(Identity oldHasIdentity);


    /* ***************************************************
     * Property http://claraweb.jlab.org/ontology/2015/11/ClaraOL#hasType
     */
     
    /**
     * Gets all property values for the hasType property.<p>
     * 
     * @return a collection of values for the hasType property.
     */
    Collection<? extends Object> getHasType();

    /**
     * Checks if the class has a hasType property value.<p>
     * 
     * @return true if there is a hasType property value.
     */
    boolean hasHasType();

    /**
     * Adds a hasType property value.<p>
     * 
     * @param newHasType the hasType property value to be added
     */
    void addHasType(Object newHasType);

    /**
     * Removes a hasType property value.<p>
     * 
     * @param oldHasType the hasType property value to be removed.
     */
    void removeHasType(Object oldHasType);



    /* ***************************************************
     * Property http://claraweb.jlab.org/ontology/2015/11/ClaraOL#hasValue
     */
     
    /**
     * Gets all property values for the hasValue property.<p>
     * 
     * @return a collection of values for the hasValue property.
     */
    Collection<? extends String> getHasValue();

    /**
     * Checks if the class has a hasValue property value.<p>
     * 
     * @return true if there is a hasValue property value.
     */
    boolean hasHasValue();

    /**
     * Adds a hasValue property value.<p>
     * 
     * @param newHasValue the hasValue property value to be added
     */
    void addHasValue(String newHasValue);

    /**
     * Removes a hasValue property value.<p>
     * 
     * @param oldHasValue the hasValue property value to be removed.
     */
    void removeHasValue(String oldHasValue);



    /* ***************************************************
     * Property http://claraweb.jlab.org/ontology/2015/11/ClaraOL#isDirectory
     */
     
    /**
     * Gets all property values for the isDirectory property.<p>
     * 
     * @return a collection of values for the isDirectory property.
     */
    Collection<? extends Boolean> getIsDirectory();

    /**
     * Checks if the class has a isDirectory property value.<p>
     * 
     * @return true if there is a isDirectory property value.
     */
    boolean hasIsDirectory();

    /**
     * Adds a isDirectory property value.<p>
     * 
     * @param newIsDirectory the isDirectory property value to be added
     */
    void addIsDirectory(Boolean newIsDirectory);

    /**
     * Removes a isDirectory property value.<p>
     * 
     * @param oldIsDirectory the isDirectory property value to be removed.
     */
    void removeIsDirectory(Boolean oldIsDirectory);



    /* ***************************************************
     * Property http://claraweb.jlab.org/ontology/2015/11/ClaraOL#isFile
     */
     
    /**
     * Gets all property values for the isFile property.<p>
     * 
     * @return a collection of values for the isFile property.
     */
    Collection<? extends Boolean> getIsFile();

    /**
     * Checks if the class has a isFile property value.<p>
     * 
     * @return true if there is a isFile property value.
     */
    boolean hasIsFile();

    /**
     * Adds a isFile property value.<p>
     * 
     * @param newIsFile the isFile property value to be added
     */
    void addIsFile(Boolean newIsFile);

    /**
     * Removes a isFile property value.<p>
     * 
     * @param oldIsFile the isFile property value to be removed.
     */
    void removeIsFile(Boolean oldIsFile);



    /* ***************************************************
     * Common interfaces
     */

    OWLNamedIndividual getOwlIndividual();

    OWLOntology getOwlOntology();

    void delete();

}
