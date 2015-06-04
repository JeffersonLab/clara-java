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

import java.util.HashSet;
import java.util.Set;

/**
 * <p>
 *     Correlates CLARA condition with the set of routing statements.
 * </p>
 *
 * @author gurjyan
 * @version 1.x
 * @since 5/21/15
 */
public class Instruction {

    // Conditions of a composition
    private Condition ifCondition;
    private Set<Statement> ifCondStatements = new HashSet<>();

    private Condition elseifCondition;
    private Set<Statement> elseifCondStatements = new HashSet<>();

    private Set<Statement> elseCondStatements = new HashSet<>();

    private Set<Statement> unCondStatements = new HashSet<>();

    // The name of the service that this instruction is relevant to.
    private String serviceName;

    public Instruction(String serviceName){
        this.serviceName = serviceName;
    }

    public String getServiceName() {
        return serviceName;
    }

    public Condition getIfCondition() {
        return ifCondition;
    }

    public void setIfCondition(Condition ifCondition) {
        this.ifCondition = ifCondition;
    }

    public Set<Statement> getIfCondStatements() {
        return ifCondStatements;
    }

    public void setIfCondStatements(Set<Statement> ifCondStatements) {
        this.ifCondStatements = ifCondStatements;
    }

    public void addIfCondStatement(Statement ifCondstatement) {
        this.ifCondStatements.add(ifCondstatement);
    }

    public Condition getElseifCondition() {
        return elseifCondition;
    }

    public void setElseifCondition(Condition elseifCondition) {
        this.elseifCondition = elseifCondition;
    }

    public Set<Statement> getElseifCondStatements() {
        return elseifCondStatements;
    }

    public void setElseifCondStatements(Set<Statement> elseifCondStatements) {
        this.elseifCondStatements = elseifCondStatements;
    }

    public void addElseifCondStatement(Statement elseifCondstatement) {
        this.elseifCondStatements.add(elseifCondstatement);
    }

    public Set<Statement> getElseCondStatements() {
        return elseCondStatements;
    }

    public void setElseCondStatements(Set<Statement> elseCondStatements) {
        this.elseCondStatements = elseCondStatements;
    }

    public void addElseCondStatement(Statement elseCondstatement) {
        this.elseCondStatements.add(elseCondstatement);
    }

    public Set<Statement> getUnCondStatements() {
        return unCondStatements;
    }

    public void setUnCondStatements(Set<Statement> unCondStatements) {
        this.unCondStatements = unCondStatements;
    }

    public void addUnCondStatement(Statement unCondstatement) {
        this.unCondStatements.add(unCondstatement);
    }

}
