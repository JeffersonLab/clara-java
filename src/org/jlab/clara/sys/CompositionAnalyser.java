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

package org.jlab.clara.sys;

import org.jlab.clara.base.CException;
import org.jlab.clara.engine.EngineData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

/**
 * <p>
 * Parser of the composition. One parser for each service.
 * Currently parses a simple composition chain.
 * todo: soon will add parsing of the CLARA condition statement.
 * </p>
 *
 * @author gurjyan
 * @version 1.x
 * @since 5/15/15
 */
public class CompositionAnalyser {
    // The dynamic (updated for every request) repository/map
    // (mapped by the composition) of input-linked service
    // names that are required to be logically AND-ed
    private HashMap<String, List<String>>
            in_and_name_list = new HashMap<>();
    // The dynamic ( updated for every request) repository/map
    // (mapped by the composition string) of input-linked service
    // data that are required to be logically AND-ed
    private HashMap<String, HashMap<String, EngineData>>
            in_and_data_list = new HashMap<>();
    // Local map of input-linked services for every
    // composition in multi-composition application.
    // Note: by design compositions are separated by ";"
    private HashMap<String, List<String>>
            in_links = new HashMap<>();
    // Local map of output-linked services for every
    // composition in multi-composition application.
    private HashMap<String, List<String>>
            out_links = new HashMap<>();

    private String myServiceName;

    public CompositionAnalyser(String name) {
        myServiceName = name;
    }

    /**
     * <p>
     * Analyses of the composition string
     * </p>
     *
     * @param composition string
     * @throws org.jlab.clara.base.CException
     */
    public void analyzeComposition(String composition) throws CException {
        // This is new routing (composition)  request
        // clear local input-link dictionary and output-links list
        in_links.clear();
        out_links.clear();
        in_and_name_list.clear();
        in_and_data_list.clear();

        // parse the new composition to find input and output
        // linked service names, but first check to see if we
        // have multiple parallel compositions (branching)

        if (composition.contains(";")) {
            StringTokenizer st = new StringTokenizer(composition, ";");
            while (st.hasMoreTokens()) {
                String sub_comp = st.nextToken();
                if (sub_comp.contains(myServiceName)) {

                    List<String> il = parse_linked(myServiceName, sub_comp, 0);
                    in_links.put(sub_comp, il);

                    List<String> ol = parse_linked(myServiceName, sub_comp, 1);
                    out_links.put(sub_comp, ol);

                    if (is_log_and(myServiceName, sub_comp)) {
                        in_and_name_list.put(sub_comp, il);
                    }
                }
            }
        } else {
            if (composition.contains(myServiceName)) {
                List<String> il = parse_linked(myServiceName, composition, 0);
                in_links.put(composition, il);

                List<String> ol = parse_linked(myServiceName, composition, 1);
                out_links.put(composition, ol);

                if (is_log_and(myServiceName, composition)) {
                    in_and_name_list.put(composition, il);
                }
            }
        }
    }

    /**
     * <p>
     * Parses composition field of the transient data
     * and returns the list of services output linked
     * to this service, i.e. that are getting output
     * data of this service.
     * Attention: service name CAN NOT appear twice
     * in the composition.
     * </p>
     *
     * @param service_name   the name of the service
     *                       for which we find input/output links
     * @param composition    the string of the composition
     * @param link_direction 0 = input-inked, >0 = output-linked
     * @return the list containing names of linked services
     */
    public List<String> parse_linked(String service_name,
                                     String composition,
                                     int link_direction) throws CException {

        // List of output service names
        List<String> out_list = new ArrayList<>();

        // List that contains composition elements
        List<String> elm_list = new ArrayList<>();

        StringTokenizer st = new StringTokenizer(composition, "+");
        while (st.hasMoreTokens()) {
            elm_list.add(st.nextToken());
        }

        // List that contains service names
        List<String> elm2_list = new ArrayList<>();
        for (String s : elm_list) {
            // remove  '&' from the service name
            // (e.g. 129.57.81.247:cont1:&Engine3 to 129.57.81.247:cont1:Engine3
            if (s.startsWith("&")) {
                s = s.replace("&", "");
            }
            elm2_list.add(s);
        }

        // See if the string contains this service name, and record the index,
        // and analyze index+1 element.
        // Note: multiple services can send to a single service, like: s1,s2+s3.
        // (this is the reason we use in:contains)
        int index = -1;
        for (String s : elm2_list) {
            if (s.contains(service_name)) {
                index = elm2_list.indexOf(s);
            }
        }
        if (index == -1) {
            throw new CException("Composition parsing exception. " +
                    "Service name can not be found in the composition.");
        } else {
            if (link_direction == 0 && index > 0) {
                // index of the next component in the composition
                index -= 1;
                String element = elm2_list.get(index);
                // the case to fan out the output of this service
                if (element.contains(",")) {
                    StringTokenizer st1 = new StringTokenizer(element, ",");
                    while (st1.hasMoreTokens()) {
                        out_list.add(st1.nextToken());
                    }
                } else {
                    out_list.add(element);
                }
                return out_list;
            } else if (link_direction > 0) {
                index += 1;
                if (elm2_list.size() > index) {
                    String element = elm2_list.get(index);
                    // the case to fan out the output of this service
                    if (element.contains(",")) {
                        StringTokenizer st1 = new StringTokenizer(element, ",");
                        while (st1.hasMoreTokens()) {
                            out_list.add(st1.nextToken());
                        }
                    } else {
                        out_list.add(element);
                    }
                }

                return out_list;
            }
        }
        // returns empty list. Most likely this service
        // is the first service in the composition
        return out_list;
    }

    /**
     * <p>
     * Check to see in the composition this service
     * is required to logically AND inputs before
     * executing its service
     * </p>
     *
     * @param service_name in the composition
     * @param composition  the string of the composition
     * @return true if component name is programmed
     * as "&<service_name>"
     */
    public boolean is_log_and(String service_name,
                              String composition) {
        String ac = "&" + service_name;

        // List that contains composition elements
        List<String> elm_list = new ArrayList<>();

        StringTokenizer st = new StringTokenizer(composition, "+");
        while (st.hasMoreTokens()) {
            elm_list.add(st.nextToken());
        }

        for (String s : elm_list) {
            if (s.equals(ac)) {
                return true;
            }
        }
        return false;
    }


    public HashMap<String, List<String>> getInAndNameList() {
        return in_and_name_list;
    }

    public HashMap<String, HashMap<String, EngineData>> getInAndDataList() {
        return in_and_data_list;
    }

    public HashMap<String, List<String>> getInLinks() {
        return in_links;
    }

    public HashMap<String, List<String>> getOutLinks() {
        return out_links;
    }
}
