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
 * Source Class: Identity <br>
 * @version generated on Tue Dec 22 14:51:01 EST 2015 by gurjyan
 */

public interface Identity extends WrappedIndividual {

    /* ***************************************************
     * Property http://claraweb.jlab.org/ontology/2015/11/ClaraOL#author
     */
     
    /**
     * Gets all property values for the author property.<p>
     * 
     * @returns a collection of values for the author property.
     */
    Collection<? extends Object> getAuthor();

    /**
     * Checks if the class has a author property value.<p>
     * 
     * @return true if there is a author property value.
     */
    boolean hasAuthor();

    /**
     * Adds a author property value.<p>
     * 
     * @param newAuthor the author property value to be added
     */
    void addAuthor(Object newAuthor);

    /**
     * Removes a author property value.<p>
     * 
     * @param oldAuthor the author property value to be removed.
     */
    void removeAuthor(Object oldAuthor);



    /* ***************************************************
     * Property http://claraweb.jlab.org/ontology/2015/11/ClaraOL#description
     */
     
    /**
     * Gets all property values for the description property.<p>
     * 
     * @returns a collection of values for the description property.
     */
    Collection<? extends String> getDescription();

    /**
     * Checks if the class has a description property value.<p>
     * 
     * @return true if there is a description property value.
     */
    boolean hasDescription();

    /**
     * Adds a description property value.<p>
     * 
     * @param newDescription the description property value to be added
     */
    void addDescription(String newDescription);

    /**
     * Removes a description property value.<p>
     * 
     * @param oldDescription the description property value to be removed.
     */
    void removeDescription(String oldDescription);



    /* ***************************************************
     * Property http://claraweb.jlab.org/ontology/2015/11/ClaraOL#name
     */
     
    /**
     * Gets all property values for the name property.<p>
     * 
     * @returns a collection of values for the name property.
     */
    Collection<? extends String> getName();

    /**
     * Checks if the class has a name property value.<p>
     * 
     * @return true if there is a name property value.
     */
    boolean hasName();

    /**
     * Adds a name property value.<p>
     * 
     * @param newName the name property value to be added
     */
    void addName(String newName);

    /**
     * Removes a name property value.<p>
     * 
     * @param oldName the name property value to be removed.
     */
    void removeName(String oldName);



    /* ***************************************************
     * Property http://claraweb.jlab.org/ontology/2015/11/ClaraOL#version
     */
     
    /**
     * Gets all property values for the version property.<p>
     * 
     * @returns a collection of values for the version property.
     */
    Collection<? extends String> getVersion();

    /**
     * Checks if the class has a version property value.<p>
     * 
     * @return true if there is a version property value.
     */
    boolean hasVersion();

    /**
     * Adds a version property value.<p>
     * 
     * @param newVersion the version property value to be added
     */
    void addVersion(String newVersion);

    /**
     * Removes a version property value.<p>
     * 
     * @param oldVersion the version property value to be removed.
     */
    void removeVersion(String oldVersion);



    /* ***************************************************
     * Common interfaces
     */

    OWLNamedIndividual getOwlIndividual();

    OWLOntology getOwlOntology();

    void delete();

}
