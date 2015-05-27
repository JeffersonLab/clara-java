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

import java.util.LinkedHashMap;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by gurjyan on 5/21/15.
 */
public class CCCompiler {

    /**
     * <p>
     *     IP address regex
     * </p>
     */
    public static String IP = "([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3})";

    /**
     * <p>
     *     String that starts with a character
     *     and can have preceding number
     * </p>
     */
    public static String STR = "([A-Z|a-z]+[0-9]*)";


    /**
     * <p>
     *     Any white space, including tab
     *     and no character symbol
     * </p>
     */
    public static String WS = "((\\s|\\t|\\n|\\W)*)";

    /**
     * <p>
     *   Service canonical name:
     *   <li>
     *       dpe_ip : container_name : service_engine_name
     *   </li>
     * </p>
     */
    public static String Sn = "(" + IP + "_(java|python|cpp):" + STR + ":" + STR + ")";

    /**
     * <p>
     *    Routing statement, such as:
     *    <li>
     *       S1 + S2 + S3;
     *    </li>
     *    <li>
     *       S1 , S2 + S3;
     *    </li>
     *    <li>
     *       S1 + S2 , S3;
     *    </li>
     *    <li>
     *       S1 , S2 + &S3;
     *    </li>
     * </p>
     */
    public static String RStmt = WS + Sn + WS + "(,"+ WS + Sn + ")*" + WS +
            "(\\+"+ WS + Sn + "(,"+ WS + Sn + ")*" + WS +")" +
            "| (\\+ "+ WS + "&"+Sn + WS+");" + WS;

    /**
     * <p>
     *     CLARA Condition, such as:
     *     <li>
     *         Service in_state "state_name"
     *     </li>
     *     <li>
     *         Service not_in_state "state_name"
     *     </li>
     *     <li>
     *         Service1 in_state "state_name1" && Service2 in_state "state_name2"
     *     </li>
     *     <li>
     *         Service1 in_state "state_name1" || Service2 in_state "state_name2"
     *     </li>
     *     Note. parenthesis are optional
     * </p>
     */
    public static String Cond = "\\(*" + WS + Sn + WS +"in_state|not_in_state" + WS + STR + WS + "\\)*" + WS +
            "&&|\\|\\|" + WS +
            "(" + "\\(*" + WS + Sn + WS +"in_state|not_in_state" + WS + STR + WS + "\\)*" + WS + ")*";

    /**
     * <p>
     *     CLARA conditional statement, such as:

     *     if ( Condition ) {
     *         routing_statement_1;
     *         routing_statement_n;
     *     } elseif ( condition ) {
     *         routing_statement_1;
     *         routing_statement_n;
     *     } else {
     *         routing_statement_1;
     *         routing_statement_n;
     *     }
     * </p>
     */
    public static String CStmt = WS + "if" + WS +"\\(" + Cond + "\\)" + WS +
            "\\{" + WS + "("+RStmt+")+" + WS + "\\}" + WS +
            "(elseif" + WS +"\\(" + Cond + "\\)" + WS +
            "\\{" + WS + "("+RStmt+")+" + WS + "\\}" + WS +")*" +
            "else" + WS + "\\{" + WS + "("+RStmt+")+" + WS + "\\}" + WS;


    public static String program = "(" + RStmt + ")*(" + CStmt + ")*";

    // Instructions of the Clara composition
    LinkedHashMap<Condition, Set<Statement>> instructions = new LinkedHashMap<>();

    public static void main(String[] args) {
        String x = "129.57.81.247_java:C1:S1+ 129.57.81.247_java:C2:S2 ";
//        System.out.println(x);
        System.out.println(RStmt);
//        System.out.println("=====================");
        Pattern p = Pattern.compile(RStmt);
        Matcher m = p.matcher(x);
        System.out.println(m.matches());
        System.out.println(m.groupCount());
        System.out.println(m.group(3));
        System.out.println(m.group(29));
        for(int i= 0;i<m.groupCount();i++){
            System.out.println(m.group(i)+" - "+i);
        }
    }

}

