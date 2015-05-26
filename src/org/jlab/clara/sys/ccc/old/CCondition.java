package org.jlab.clara.sys.ccc.old;

import java.util.ArrayList;

/**
 * <font size = 1 >JSA: Thomas Jefferson National Accelerator Facility<br>
 * This software was developed under a United States Government license,<br>
 * described in the NOTICE file included as part of this distribution.<br>
 * Copyright (c), Aug 18, 2009<br></font>
 * </p>
 *
 * @author Vardan Gyurjyan
 * @version 1.x
 */

public class CCondition {
    private String keyWord;
    private ArrayList<String> conditionalOperators = new ArrayList<String>();
    private ArrayList<CStatement> conditionalStatements = new ArrayList<CStatement>();
    private ArrayList<CStatement> actionStatements = new ArrayList<CStatement>();


    public String getKeyWord() {
        return keyWord;
    }

    public void setKeyWord(String keyWord) {
        this.keyWord = keyWord;
    }

    public ArrayList<String> getConditionalOperators() {
        return conditionalOperators;
    }

    public void setConditionalOperators(ArrayList<String> conditionalOperators) {
        this.conditionalOperators = conditionalOperators;
    }

    public ArrayList<CStatement> getConditionalStatements() {
        return conditionalStatements;
    }

    public void setConditionalStatements(ArrayList<CStatement> conditionalStatements) {
        this.conditionalStatements = conditionalStatements;
    }

    public ArrayList<CStatement> getActionStatements() {
        return actionStatements;
    }

    public void setActionStatements(ArrayList<CStatement> actionStatements) {
        this.actionStatements = actionStatements;
    }

    public String toString(){
        StringBuffer sb= new StringBuffer();
        sb.append(keyWord).append("\n");
        CStatement stmt;
        String oper;
        System.out.println("\n......... New ACondition ..... ");
        for(int i=0; i< conditionalStatements.size();i++){
            stmt = conditionalStatements.get(i);
            sb.append(stmt.getLeft()).append(" ").
                    append(stmt.getActionOperator()).append(" ").append(stmt.getRight());
            sb.append("\n");
            if(i<conditionalOperators.size()){
                sb.append(conditionalOperators.get(i)).append("\n");
            }
        }
        for (CStatement actionStatement : actionStatements) {
            stmt = actionStatement;
            sb.append(" -> ").append(stmt.getLeft()).append(" ").
                    append(stmt.getActionOperator()).append(" ").append(stmt.getRight());
            sb.append("\n");
        }
        return sb.toString();
    }
}
