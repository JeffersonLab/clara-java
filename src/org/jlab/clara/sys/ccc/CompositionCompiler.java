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

package org.jlab.clara.sys.ccc;

import org.jlab.clara.base.error.ClaraException;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * CLARA compiler. Compiles the application logical description, i.e.
 * simple/conditional routing schema in a sets of instructions for a specified
 * service. Below is an example of the application code, written in the
 * specific CLARA language:
 * <pre>
 * S1 + S2;
 * if ( S1 == "abc" && S2 != "xyz") {
 *   S2 + S3;
 * } elseif ( S1 == "fred" ) {
 *     S2 + S4;
 * } else {
 *     S2 + S5,S6,S7;
 * }
 * S4,S5 + &S8;
 * </pre>
 *
 * @author gurjyan
 * @version 4.x
 * @since 5/29/15
 */
public class CompositionCompiler {

    /**
     * IP address regex.
     */
    public static final String IP = "([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3})";

    /**
     * String that starts with a character and can have preceding number.
     */
    public static final String WORD = "([a-zA-Z_0-9-])";
//    public static final String WORD = "([A-Z|a-z]+[0-9]*)";
    public static final String PORT = "(%+[0-9]*)*";

    /**
     * Service canonical name.
     * Format: {@code dpe_name:container_name:engine_name}
     */
    public static final String SERV_NAME = IP + PORT + "_(java|python|cpp):" + WORD + ":" + WORD;

    /**
     * Routing statement. Example:
     * <ul>
     * <li>{@code S1 + S2 + S3;}</li>
     * <li>{@code S1 , S2 + S3;}</li>
     * <li>{@code S1 + S2 , S3;}</li>
     * <li>{@code S1 , S2 + &S3;}</li>
     * <li>{@code S1;}</li>
     * </ul>
     * Note that regular expression does not include end of statement operator.
     */
    public static final String STATEMENT = SERV_NAME + "(," + SERV_NAME + ")*"
                                         + "((\\+&?" + SERV_NAME + ")*|(\\+" + SERV_NAME
                                         + "(," + SERV_NAME + ")*)*)";

    /**
     * CLARA simple Condition. Example:
     * <li>{@code Service == "state_name}"</li>
     * <li>{@code Service != "state_name"</li>
     */
    public static final String SIMP_COND = SERV_NAME + "(==|!=)\"" + WORD + "\"";

    /**
     * CLARA complex Condition. Example:
     * <li>{@code (Service1 == "state_name1" && Service2 == "state_name2)}</li>
     * <li>{@code (Service1 == "state_name1" !!
     *             Service2 == "state_name2" !!
     *             Service2 != "state_name3")}</li>
     */
    public static final String COMP_COND = SIMP_COND + "((&&|!!)" + SIMP_COND + ")*";

    /**
     * CLARA conditional statement.
     */
    public static final String COND = "((\\}?if|\\}elseif)\\(" + COMP_COND + "\\)\\{"
                                    + STATEMENT + ")|(\\}else\\{" + STATEMENT + ")";

    public Set<Instruction> instructions = new LinkedHashSet<>();

    // The name of the service relative to which compilation will be done.
    private String myServiceName;

    /**
     * Constructor.
     *
     * @param service the name of the service relative to which to compile.
     */
    public CompositionCompiler(String service) {
        myServiceName = service;
    }

    public void compile(String iCode) throws ClaraException {

        // This is a new request reset
        reset();

        // Create a single string with no blanks
        String pCode = noBlanks(iCode);

        // split single string program using
        // CLARA ; end of statement operator
        // in case of the conditional statements the }
        // scope operator can be the first after tokenize with,
        // so preProcess will take of that too.
        Set<String> pp = preProcess(pCode);

        // start analysing and building compiled instructions
        String[] ppi = pp.toArray(new String[pp.size()]);

        int i = -1;
        while (++i < ppi.length) {

            String scs1 = ppi[i];

            // conditional statement
            if (scs1.startsWith("if(")
                    || scs1.startsWith("}if(")
                    || scs1.startsWith("}elseif(")
                    || scs1.startsWith("}else")) {

                Instruction instruction = parseCondition(scs1);

                // ADB: assuming the intention here was to allow multiple
                // statements under one conditional -- otherwise why make it
                // nested?
                while (++i < ppi.length) {

                    String scs2 = ppi[i];

                    if (!scs2.startsWith("}")
                            && !scs2.startsWith("if(")
                            && !scs2.startsWith("}if(")
                            && !scs2.startsWith("}elseif(")
                            && !scs2.startsWith("}else")) {

                        // if ignoring the conditional, then ignore its statements also
                        if (instruction != null) {
                            parseConditionalStatement(scs2, instruction);
                        }
                    } else {
                        break;
                    }
                }
                if (instruction != null) {
                    instructions.add(instruction);
                }
                i--;
                // routing statement
            } else {
                parseStatement(scs1);
            }
        }

        if (instructions.isEmpty()) {
            throw new ClaraException("Composition is irrelevant for a service.");
        }

    }

    /**
     * Tokenize code by CLARA end of statement operator ";".
     *
     * @param pCode code string
     * @return set of tokens, including simple routing statements as well as conditionals
     * @throws ClaraException
     */
    private Set<String> preProcess(String pCode) throws ClaraException {
        if (!pCode.contains(";") && !pCode.endsWith(";")) {
            throw new ClaraException("Syntax error in the CLARA routing program. "
                    + "Missing end of statement operator = \";\"");
        }
        Set<String> r = new LinkedHashSet<>();
        // tokenize by ;
        StringTokenizer st = new StringTokenizer(pCode, ";");

        while (st.hasMoreTokens()) {

            String text = st.nextToken();

            // ADB: by stripping out the closing brace here you lose the ability
            //      to correctly parse multiple statements in a block
            //
            // this will get read of very last }
            //text = CUtility.removeFirst(text, "}");

            // ignore
            if (!text.equals("") && !text.equals("}")) {
                r.add(text);
            }
        }
        return r;
    }

    private boolean parseStatement(String iStmt) throws ClaraException {
        boolean b = false;
        Instruction ti = new Instruction(myServiceName);

        // ignore a leading }
        iStmt = CompositionParser.removeFirst(iStmt, "}");

        // unconditional routing statement
        try {
            Pattern p = Pattern.compile(STATEMENT);
            Matcher m = p.matcher(iStmt);

            if (m.matches()) {

                // ignore conditional statements not concerning me
                if (!iStmt.contains(myServiceName)) {
                    return false;
                }

                Statement ts = new Statement(iStmt, myServiceName);
                ti.addUnCondStatement(ts);
                instructions.add(ti);
                b = true;
            } else {
                System.out.println("DDD ----- > statement = " + iStmt);

                throw new ClaraException("Syntax error in the CLARA routing program. "
                        + "Malformed routing statement");
            }
        } catch (PatternSyntaxException e) {
            System.err.println(e.getDescription());
        }
        return b;
    }

    private boolean parseConditionalStatement(String iStmt, Instruction ti) throws ClaraException {
        boolean b = false;

        // unconditional routing statement
        Pattern p = Pattern.compile(STATEMENT);
        Matcher m = p.matcher(iStmt);
        if (m.matches()) {

            // ignore conditional statements not concerning me
            if (!iStmt.contains(myServiceName)) {
                return false;
            }

            Statement ts = new Statement(iStmt, myServiceName);

            // inside condition, so add as the corect type
            if (ti.getIfCondition() != null) {
                ti.addIfCondStatement(ts);
            } else if (ti.getElseifCondition() != null) {
                ti.addElseifCondStatement(ts);
            } else {
                ti.addElseCondStatement(ts);
            }
            b = true;
        } else {
            System.out.println("DDD ----- > statement = " + iStmt);

            throw new ClaraException("Syntax error in the CLARA routing program. "
                    + "Malformed routing statement");
        }

        return b;
    }

    private Instruction parseCondition(String iCnd) throws ClaraException {
        Instruction ti;

        Pattern p = Pattern.compile(COND);
        Matcher m = p.matcher(iCnd);

        if (m.matches()) {
            try {
                // get first statement and analyze it
                String statementStr = iCnd.substring(iCnd.indexOf("{"));

                // ignore conditions not concerning me
                if (!statementStr.contains(myServiceName)) {
                    return null;
                }

                Statement ts = new Statement(statementStr, myServiceName);

                // create Instruction
                ti = new Instruction(myServiceName);
                if (iCnd.startsWith("}if(") || iCnd.startsWith("if(")) {
                    String conditionStr = iCnd.substring(iCnd.indexOf("(") + 1,
                                                         iCnd.lastIndexOf(")"));
                    Condition tc = new Condition(conditionStr, myServiceName);
                    ti.setIfCondition(tc);
                    ti.addIfCondStatement(ts);
                } else if (iCnd.startsWith("}elseif(")) {
                    String conditionStr = iCnd.substring(iCnd.indexOf("(") + 1,
                                                         iCnd.lastIndexOf(")"));
                    Condition tc = new Condition(conditionStr, myServiceName);
                    ti.setElseifCondition(tc);
                    ti.addElseifCondStatement(ts);
                } else if (iCnd.startsWith("}else")) {
                    ti.addElseCondStatement(ts);
                }
            } catch (StringIndexOutOfBoundsException e) {
                throw new ClaraException("Syntax error in the CLARA routing program. "
                            + "Missing parenthesis");
            }
        } else {
            throw new ClaraException("Syntax error in the CLARA routing program. "
                    + "Malformed conditional statement");
        }
        return ti;
    }

    public void reset() {
        instructions.clear();
    }

    /**
     * Returns an entire program one consequent string.
     *
     * @param x input program text
     * @return single string representation of the program
     */
    private String noBlanks(String x) {
        StringTokenizer st = new StringTokenizer(x);
        StringBuilder sb = new StringBuilder();
        while (st.hasMoreTokens()) {
            sb.append(st.nextToken().trim());
        }
        return sb.toString();
    }

    public Set<Instruction> getInstructions() {
        return instructions;
    }

    public Set<String> getUnconditionalLinks() {
        Set<String> outputs = new HashSet<>();
        for (Instruction inst : instructions) {
            // NOTE: instruction routing statements are exclusive: will be
            //       either unconditional, if, elseif, or else.
            if (inst.getUnCondStatements() != null && !inst.getUnCondStatements().isEmpty()) {
                for (Statement stmt : inst.getUnCondStatements()) {
                    outputs.addAll(stmt.getOutputLinks());
                }
            }
        }
        return outputs;
    }

    public Set<String> getLinks(ServiceState ownerSS, ServiceState inputSS) {

        Set<String> outputs = new HashSet<>();

        // The list of routing instructions supply the output links
        //
        // Instructions with unconditional routing always provide output links
        //
        // Conditional routing evaluates a sequence of instructions:
        //
        //   * one if-conditional instruction
        //   * zero-or-more else-if conditional instructions
        //   * zero-or-one else conditional instruction
        //
        // In a sequence, only the first conditional to evaluate to "true"
        // supplies output links

        // keep track of when one of the if/elseif/else conditions has been chosen
        boolean inCondition = false;
        boolean conditionChosen = false;

        for (Instruction inst : instructions) {
            // NOTE: instruction routing statements are exclusive: will be
            //       either unconditional, if, elseif, or else.
            if (inst.getUnCondStatements() != null && !inst.getUnCondStatements().isEmpty()) {
                // no longer in a conditional now
                inCondition = false;
                for (Statement stmt : inst.getUnCondStatements()) {
                    outputs.addAll(stmt.getOutputLinks());
                }
                continue;
            }

            if (inst.getIfCondition() != null) {
                inCondition = true;
                conditionChosen = false;
                if (inst.getIfCondition().isTrue(ownerSS, inputSS)) {
                    conditionChosen = true;
                    for (Statement stmt : inst.getIfCondStatements()) {
                        outputs.addAll(stmt.getOutputLinks());
                    }
                }
                continue;
            }

            // must be in a conditional already to process an elseif or else
            if (inCondition && !conditionChosen) {
                if (inst.getElseifCondition() != null) {
                    if (inst.getElseifCondition().isTrue(ownerSS, inputSS)) {
                        conditionChosen = true;
                        for (Statement stmt : inst.getElseifCondStatements()) {
                            outputs.addAll(stmt.getOutputLinks());
                        }
                    }
                    continue;
                }

                if (!inst.getElseCondStatements().isEmpty()) {
                    conditionChosen = true;
                    for (Statement stmt : inst.getElseCondStatements()) {
                        outputs.addAll(stmt.getOutputLinks());
                    }
                }
            }
        }

        return outputs;
    }
}
