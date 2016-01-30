package claraol;

import java.util.Collection;

import org.protege.owl.codegeneration.WrappedIndividual;

import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLOntology;

/**
 * 
 * <p>
 * Generated by Protege (http://protege.stanford.edu). <br>
 * Source Class: DataSet <br>
 * @version generated on Sat Jan 30 12:43:23 EST 2016 by gurjyan
 */

public interface DataSet extends WrappedIndividual {

    /* ***************************************************
     * Property http://claraweb.jlab.org/ontology/2015/11/ClaraOL#hasInputSource
     */
     
    /**
     * Gets all property values for the hasInputSource property.<p>
     * 
     * @returns a collection of values for the hasInputSource property.
     */
    Collection<? extends DataSource> getHasInputSource();

    /**
     * Checks if the class has a hasInputSource property value.<p>
     * 
     * @return true if there is a hasInputSource property value.
     */
    boolean hasHasInputSource();

    /**
     * Adds a hasInputSource property value.<p>
     * 
     * @param newHasInputSource the hasInputSource property value to be added
     */
    void addHasInputSource(DataSource newHasInputSource);

    /**
     * Removes a hasInputSource property value.<p>
     * 
     * @param oldHasInputSource the hasInputSource property value to be removed.
     */
    void removeHasInputSource(DataSource oldHasInputSource);


    /* ***************************************************
     * Property http://claraweb.jlab.org/ontology/2015/11/ClaraOL#hasOutputSource
     */
     
    /**
     * Gets all property values for the hasOutputSource property.<p>
     * 
     * @returns a collection of values for the hasOutputSource property.
     */
    Collection<? extends DataSource> getHasOutputSource();

    /**
     * Checks if the class has a hasOutputSource property value.<p>
     * 
     * @return true if there is a hasOutputSource property value.
     */
    boolean hasHasOutputSource();

    /**
     * Adds a hasOutputSource property value.<p>
     * 
     * @param newHasOutputSource the hasOutputSource property value to be added
     */
    void addHasOutputSource(DataSource newHasOutputSource);

    /**
     * Removes a hasOutputSource property value.<p>
     * 
     * @param oldHasOutputSource the hasOutputSource property value to be removed.
     */
    void removeHasOutputSource(DataSource oldHasOutputSource);


    /* ***************************************************
     * Property http://claraweb.jlab.org/ontology/2015/11/ClaraOL#outputFilePrefix
     */
     
    /**
     * Gets all property values for the outputFilePrefix property.<p>
     * 
     * @returns a collection of values for the outputFilePrefix property.
     */
    Collection<? extends String> getOutputFilePrefix();

    /**
     * Checks if the class has a outputFilePrefix property value.<p>
     * 
     * @return true if there is a outputFilePrefix property value.
     */
    boolean hasOutputFilePrefix();

    /**
     * Adds a outputFilePrefix property value.<p>
     * 
     * @param newOutputFilePrefix the outputFilePrefix property value to be added
     */
    void addOutputFilePrefix(String newOutputFilePrefix);

    /**
     * Removes a outputFilePrefix property value.<p>
     * 
     * @param oldOutputFilePrefix the outputFilePrefix property value to be removed.
     */
    void removeOutputFilePrefix(String oldOutputFilePrefix);



    /* ***************************************************
     * Property http://claraweb.jlab.org/ontology/2015/11/ClaraOL#outputFileSuffix
     */
     
    /**
     * Gets all property values for the outputFileSuffix property.<p>
     * 
     * @returns a collection of values for the outputFileSuffix property.
     */
    Collection<? extends String> getOutputFileSuffix();

    /**
     * Checks if the class has a outputFileSuffix property value.<p>
     * 
     * @return true if there is a outputFileSuffix property value.
     */
    boolean hasOutputFileSuffix();

    /**
     * Adds a outputFileSuffix property value.<p>
     * 
     * @param newOutputFileSuffix the outputFileSuffix property value to be added
     */
    void addOutputFileSuffix(String newOutputFileSuffix);

    /**
     * Removes a outputFileSuffix property value.<p>
     * 
     * @param oldOutputFileSuffix the outputFileSuffix property value to be removed.
     */
    void removeOutputFileSuffix(String oldOutputFileSuffix);



    /* ***************************************************
     * Common interfaces
     */

    OWLNamedIndividual getOwlIndividual();

    OWLOntology getOwlOntology();

    void delete();

}
