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

import java.util.HashSet;
import java.util.Set;

/**
 * Correlates CLARA condition with the set of routing statements.
 *
 * @author gurjyan
 * @version 4.x
 * @since 5/21/15
 */
class Instruction {

    // The name of the service that this instruction is relevant to.
    private String serviceName;

    // Conditions of a composition
    private Condition ifCondition;
    private Set<Statement> ifCondStatements = new HashSet<>();

    private Condition elseifCondition;
    private Set<Statement> elseifCondStatements = new HashSet<>();

    private Set<Statement> elseCondStatements = new HashSet<>();

    private Set<Statement> unCondStatements = new HashSet<>();

    Instruction(String serviceName) {
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

    public void addElseifCondStatement(Statement elseifCondstatement) {
        this.elseifCondStatements.add(elseifCondstatement);
    }

    public Set<Statement> getElseCondStatements() {
        return elseCondStatements;
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

    @Override
    public String toString() {
        return "Instruction{"
                + "ifCondition=" + ifCondition
                + ", ifCondStatements=" + ifCondStatements
                + ", elseifCondition=" + elseifCondition
                + ", elseifCondStatements=" + elseifCondStatements
                + ", elseCondStatements=" + elseCondStatements
                + ", unCondStatements=" + unCondStatements
                + ", serviceName='" + serviceName + '\''
                + '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Instruction)) {
            return false;
        }

        Instruction other = (Instruction) obj;

        if (!serviceName.equals(other.serviceName)) {
            return false;
        }
        if (ifCondition != null ? !ifCondition.equals(other.ifCondition)
                                : other.ifCondition != null) {
            return false;
        }
        if (!ifCondStatements.equals(other.ifCondStatements)) {
            return false;
        }
        if (elseifCondition != null ? !elseifCondition.equals(other.elseifCondition)
                                    : other.elseifCondition != null) {
            return false;
        }
        if (!elseifCondStatements.equals(other.elseifCondStatements)) {
            return false;
        }
        if (!elseCondStatements.equals(other.elseCondStatements)) {
            return false;
        }
        if (!unCondStatements.equals(other.unCondStatements)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = serviceName.hashCode();
        result = 31 * result + (ifCondition != null ? ifCondition.hashCode() : 0);
        result = 31 * result + ifCondStatements.hashCode();
        result = 31 * result + (elseifCondition != null ? elseifCondition.hashCode() : 0);
        result = 31 * result + elseifCondStatements.hashCode();
        result = 31 * result + elseCondStatements.hashCode();
        result = 31 * result + unCondStatements.hashCode();
        return result;
    }
}
