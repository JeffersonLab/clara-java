/*
 *   Copyright (c) 2016.  Jefferson Lab (JLab). All rights reserved. Permission
 *   to use, copy, modify, and distribute  this software and its documentation for
 *   educational, research, and not-for-profit purposes, without fee and without a
 *   signed licensing agreement.
 *
 *   IN NO EVENT SHALL JLAB BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL
 *   INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING
 *   OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF JLAB HAS
 *   BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *   JLAB SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *   THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *   PURPOSE. THE CLARA SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY,
 *   PROVIDED HEREUNDER IS PROVIDED "AS IS". JLAB HAS NO OBLIGATION TO PROVIDE
 *   MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *   This software was developed under the United States Government license.
 *   For more information contact author at gurjyan@jlab.org
 *   Department of Experimental Nuclear Physics, Jefferson Lab.
 */

package org.jlab.clara.claraol.parser;

import org.apache.jena.rdf.model.*;
import org.jlab.clara.base.error.ClaraException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * ClaraOL parser.
 * Creates in-memory representation of a data processing application
 * ClaraOL description.
 * <p/>
 *
 * @author gurjyan
 *         Created on 2/3/16
 * @version Clara-java-v4.3
 */
public class ClaraOLParser {

    // Jena model
    private Model GModel;

    // List of the included cool configuration file jena models
    private Map<String, Model> _models = new HashMap<>();

    public static void main(String[] args) {
        ClaraOLParser parser = new ClaraOLParser();
        try {
            System.out.println(args[0]);
            parser.parseApplication(args[0]);
        } catch (IOException | ClaraException e) {
            e.printStackTrace();
        }

        System.out.println(parser);
    }

    public void parseApplication(String appName) throws IOException, ClaraException {

        // reset previous parsing results
        reset();

        String appColFile = ColPUtil.getClaraOlHome() + File.separator +
                appName + File.separator +
                appName + ".rdf";
        System.out.println(appColFile);

        extractModels(appColFile);
        uniteModels();
    }

    /**
     * Resets models map and global, unified
     * model of the previous app configuration.
     */
    private void reset() {
        // reset the model
        _models.clear();
        if (GModel != null) {
            GModel.removeAll();
            GModel.close();
        }
    }

    /**
     * Create the final model as a union of all models
     */
    private void uniteModels() {
        if (!_models.isEmpty()) {
            // create union of the jena models
            GModel = ModelFactory.createDefaultModel();
            for (String s : _models.keySet()) {
                GModel = GModel.union(_models.get(s));
            }
        }
    }

    /**
     * Creates Jena model from the ClaraOLRDF description file
     *
     * @param fileName direct path to the ClaraOL RDF file
     */
    private void extractModels(String fileName) throws ClaraException, IOException {
        FileInputStream fis;
        try {
            fis = new FileInputStream(fileName);
        } catch (FileNotFoundException e) {
            throw new ClaraException("No Ontology definition file found.");
        }

        // create the jena model
        Model model = ModelFactory.createDefaultModel();
        model.read(fis, ColConstants.URI, "RDF/XML");

        fis.close();

        // add this model to the model list
        if (!_models.containsKey(fileName)) {
            _models.put(fileName, model);

            System.out.println("include: " + fileName);

            // get iterator over statements in the Model
            StmtIterator iterator = model.listStatements();

            // print out the predicate, subject and object of each statement
            while (iterator.hasNext()) {
                Statement stmt = iterator.nextStatement();
                RDFNode node = stmt.getObject();

                if (node instanceof Resource) {
                    if ((node.toString().endsWith(".rdf"))) {
                        String includeNodeName = node.toString();
                        if (includeNodeName.contains(ColConstants.URI)) {

                            // replace URI, which is the location of the ClaraOL home
                            includeNodeName = ColPUtil.replace(includeNodeName,
                                    ColConstants.URI,
                                    ColPUtil.getClaraOlHome());

                            // get model for the included rdf file
                            extractModels(includeNodeName);

                        } else {
                            return;
                        }
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (GModel != null) {

            // list the statements in the Model
            StmtIterator iterator = GModel.listStatements();

            // print out the predicate, subject and object of each statement
            while (iterator.hasNext()) {
                Statement stmt = iterator.nextStatement();
                Resource subject = stmt.getSubject();
                Property predicate = stmt.getPredicate();
                RDFNode node = stmt.getObject();
                sb.append("subject   = " + subject.toString() + "\n");
                sb.append("predicate = " + predicate.toString() + "\n");
                if (node instanceof Resource) {
                    sb.append("resource = " + node.toString() + "\n");
                } else {
                    sb.append("literal = " + node.toString() + "\n");
                }
                sb.append("\n");
            }
        }
        return sb.toString();
    }
}
