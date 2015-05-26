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

/**
 * Created by gurjyan on 5/21/15.
 */
public class CCCompiler {

    public final static String IP = "([0-9]{1,3}[\\.]){3}[0-9]{1,3}";

    public final static String STR = "^[A-Z|a-z]+[0-9]+$";

    public final static String WS = "(\\s|\\t|\\n|\\W)*";

    public static final String Sn = "^"+ IP +":"+ STR +":"+ STR +"$";

    public static String Stmt = "^("+Sn+"(,"+Sn+")*))\\+((&"+Sn+")|(("+Sn+"(,("+Sn+")*)))$";

    public static String IF = "if"+ WS +"\\("+ WS + Sn + WS + STR + WS +"\\)"+ WS +"\\{"+ WS +"("+ Stmt +")+"+ WS +"\\}";

    public static String ELSEIF = "elseif"+ WS +"\\("+ WS + Sn + WS + STR + WS +"\\)"+ WS +"\\{"+ WS +"("+ Stmt +")+"+ WS +"\\}";

    public static String ELSE = "else"+ WS +"\\("+ WS + Sn + WS + STR + WS +"\\)"+ WS +"\\{"+ WS +"("+ Stmt +")+"+ WS +"\\}";

    public static String Cond = IF + "(" + ELSEIF + ")*(" + ELSE + ")*";


    // Instructions of the Clara composition
    LinkedHashMap<Condition, Set<Statement>> instructions = new LinkedHashMap<>();
}
