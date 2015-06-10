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

import org.jlab.clara.base.CException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * <p>
 *     Clara compiler. Compiles the application logical description,
 *     i.e. simple/conditional routing schema in a sets of instructions
 *     for a specified service. Below is an example of the application
 *     code, written in the specific Clara language:
 *
 *     S1 + S2;
 *     if ( S1 ? "abc" && S2 ! "xyz") {
 *       S2 + S3;
 *     } elseif ( S1 ? "fred" ) {
 *         S2 + S4;
 *     } else {
 *         S2 + S5,S6,S7;
 *     }
 *     S4,S5 + &S8;
 * </p>
 *
 * @author gurjyan
 * @version 1.x
 * @since 5/29/15
 */
public class CCompiler {

    public Set<Instruction> instructions = new HashSet<>();


    /**
     * <p>
     *     IP address regex
     * </p>
     */
    public static final String IP = "([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3})";

    /**
     * <p>
     *     String that starts with a character
     *     and can have preceding number
     * </p>
     */
    public static final String STR = "([A-Z|a-z]+[0-9]*)";

    /**
     * <p>
     *   Service canonical name:
     *   <li>
     *       dpe_ip : container_name : service_engine_name
     *   </li>
     * </p>
     */
//    public static final String Sn = "(" + IP + "_(java|python|cpp):" + STR + ":" + STR + ")";
    public static final String Sn = IP + "_(java|python|cpp):" + STR + ":" + STR;

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
     *    Note that regular expression does not include end of statement operator.
     * </p>
     */
    public static final String RStmt = Sn + "(,"+ Sn + ")*"+ "((\\+"+Sn+")+|(\\+"+ Sn+"(,"+ Sn +")*)+)";

    /**
     * <p>
     *     CLARA simple Condition, such as:
     *     <li>
     *         Service ? "state_name"
     *     </li>
     *     <li>
     *         Service ! "state_name"
     *     </li>
     * </p>
     */
    public static final String sCond = Sn +"?|!" + STR;

    /**
     * <p>
     *     CLARA complex Condition, such as:
     *     <li>
     *         Service1 ? "state_name1" && Service2 ? "state_name2"
     *     </li>
     *     <li>
     *         Service1 ? "state_name1" || Service2 ? "state_name2"
     *     </li>
     * </p>
     */
    public static final String Cond = "if|elseif|else\\( (" + sCond + "&&|\\|\\|" + sCond + ")+ \\)";

    // The name of the service relative to which compilation will be done.
    private String myServiceName;


    /**
     * Constructor
     * @param service the name of the service relative to which to compile.
     */
    public CCompiler(String service){
        myServiceName = service;
    }

    public void compile(String iCode) throws CException {

        // This is a new request reset
        reset();

        // Create a single string with no blanks
        String pCode = noBlanks(iCode);

        System.out.println("DDD-1 ");
        System.out.println(pCode);
        System.out.println("------------ \n");

        // split single string program using
        // Clara ; enf of statement operator
        Set<String> pp = preProcess(pCode);

        // start analysing and building compiled instructions
        Iterator<String> ppi = pp.iterator();
        while(ppi.hasNext()){
            String scs1 = ppi.next();
            System.out.println("DDD-2 ");
            System.out.println(scs1);
            System.out.println("------------ \n");

            if(!parseStatement(scs1)){
                Instruction ins = parseCondition(scs1);
                while(ppi.hasNext()){
                    String scs2 = ppi.next();
                    if(!scs2.startsWith("if(") &&
                            !scs2.startsWith("}elseif(") &&
                            !scs2.startsWith("}else")) {
                        parseStatement(scs2, ins);
                    } else {
                        break;
                    }
                }
                instructions.add(ins);
            }
        }
    }

    /**
     * <p>
     *     Tokenize code by Clara end of statement operator ";"
     * </p>
     * @param pCode code string
     * @return set of tokens, including simple
     * routing statements as well as conditionals
     * @throws CException
     */
    private Set<String> preProcess(String pCode) throws CException {
        Set<String> r = new HashSet<>();
        // tokenize by ;
        StringTokenizer st = new StringTokenizer(pCode,";");
        while (st.hasMoreTokens()){
            r.add(st.nextToken());
        }
        if(r.isEmpty())throw new CException("syntax error: missing ;");
        return r;
    }

    private boolean parseStatement(String iStmt) throws CException {
        boolean b = false;
        Instruction ti = new Instruction(myServiceName);

        if(!iStmt.startsWith("if(") &&
                !iStmt.startsWith("}elseif(") &&
                !iStmt.startsWith("}else")){

            //unconditional routing statement
            try {
                Pattern p = Pattern.compile(RStmt);
                Matcher m = p.matcher(iStmt);

                if(m.matches()) {
                    Statement ts = new Statement(iStmt, myServiceName);
                    ti.addUnCondStatement(ts);
                    instructions.add(ti);
                    b = true;
                } else {
                    throw new CException("syntax error: malformed routing statement");
                }
            } catch (PatternSyntaxException e){
                System.err.println(e.getDescription());
            }
        }
        return b;
    }

    private boolean parseStatement(String iStmt, Instruction ti) throws CException {
        boolean b = false;

        if(!iStmt.startsWith("if(") &&
                !iStmt.startsWith("}elseif(") &&
                !iStmt.startsWith("}else")){

            //unconditional routing statement
            Pattern p = Pattern.compile(RStmt);
            Matcher m = p.matcher(iStmt);
            if(m.matches()) {
                Statement ts = new Statement(iStmt, myServiceName);
                ti.addUnCondStatement(ts);
                b = true;
            } else {
                throw new CException("syntax error: malformed routing statement");
            }
        }
        return b;
    }

    private Instruction parseCondition(String iCnd) throws CException {
        Instruction ti = null;
        if (iCnd.startsWith("if(") ||
                iCnd.startsWith("}elseif(") ||
                iCnd.startsWith("}else")) {

            Pattern p = Pattern.compile(Cond);
            Matcher m = p.matcher(iCnd);
            if(m.matches()) {
                try {
                    // get condition and analyze it
                    String conditionStr = iCnd.substring(iCnd.indexOf("("), iCnd.lastIndexOf(")"));
                    Condition tc = new Condition(conditionStr, myServiceName);

                    // get first statement  and analyze it
                    String statementStr = iCnd.substring(iCnd.indexOf("{"));
                    Statement ts = new Statement(statementStr, myServiceName);

                    // create Instruction
                    ti = new Instruction(myServiceName);
                    if(iCnd.startsWith("if(")) {
                        ti.setIfCondition(tc);
                        ti.addIfCondStatement(ts);
                    }
                    else if(iCnd.startsWith("}elseif(")) {
                        ti.setElseifCondition(tc);
                        ti.addElseifCondStatement(ts);
                    }
                    else if(iCnd.startsWith("}else")) {
                        ti.addElseCondStatement(ts);
                    }
                } catch (StringIndexOutOfBoundsException e) {
                    throw new CException("syntax error: missing parenthesis");
                }
            } else {
                throw new CException("syntax error: malformed conditional statement");
            }
        }
        return ti;
    }

    public void reset(){
        instructions.clear();
    }


    /**
     * <p>
     *     returns an entire program one consequent string
     * </p>
     * @param x input program text
     * @return single string representation of the program
     */
    private String noBlanks(String x){
        StringTokenizer st = new StringTokenizer(x);
        StringBuilder sb = new StringBuilder();
        while(st.hasMoreTokens()){
            sb.append(st.nextToken().trim());
        }
        return sb.toString();
    }

    public Set<Instruction> getInstructions() {
        return instructions;
    }

    public static void main(String[] args) {
        CCompiler compiler = new CCompiler("10.2.9.96_java:container1:engine2");
        try {
            String t = new String(Files.readAllBytes(Paths.get("/users/gurjyan//Devel/Test/data/example1.cmp")), StandardCharsets.UTF_8);
            compiler.compile(t);

        } catch (IOException | CException e) {
            e.printStackTrace();
        }
    }
}

