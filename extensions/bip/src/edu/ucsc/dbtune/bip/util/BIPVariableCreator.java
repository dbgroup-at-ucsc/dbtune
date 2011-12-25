package edu.ucsc.dbtune.bip.util;

import java.util.ArrayList;
import java.util.List;


public class BIPVariableCreator 
{
    public static final int  VAR_Y = 0;
    public static final int  VAR_X = 1;
    public static final int  VAR_S = 2;
    public static final int  VAR_PRESENT = 3;
    public static final int  VAR_CREATE = 4;
    public static final int  VAR_DROP = 5;
    public static final int  VAR_DEPLOY = 6;
    public static final int  VAR_DEFAULT = 100;    
    private String[] strHeaderVariable = {"y", "x", "s", "present", "create", "drop", "deploy"};
    
    /**
     * 
     * Construct the variable name in the form: y(w, q, k), x(w,q, k,i,a), s(w, a), present(w,a),  create(w,a), drop(w,a)
     *  
     *
     * @param typeVarible
     *      The type of variable, the value is in the set {y, x, s, present, create, drop, deploy}, 
     * @param window
     *      The window time in the materialized schedule (SIM formulation), or the replica ID (DIV)
     * @param queryId
     *      The identifier of the processing query if {@code typeVariable = VAR_Y, VAR_X}; 
     *      Or the identifier of the index if {@code typeVariable = VAR_S, VAR_PRESENT, VAR_CREATE, VAR_DROP}
     *      Not applicable if {@code typeVariable = VAR_DEPLOY}
     * @param k
     *      The template plan identifier
     *      Only enable when {@code typeVariable = VAR_X, VAR_Y}
     * @param i 
     *      The position of slot in the template plan
     *      Only enable when {@code typeVariable = VAR_X}
     * @param a 
     *      The position of the index in the corresponding slot
     *      Only enable when {@code typeVariable = VAR_X}
     * 
     * @return
     *      The variable name
     */
    public String constructVariableName(int typeVariable, int window, int queryId, int k, int i, int a)
    {
        String result = "";
        result = result.concat(strHeaderVariable[typeVariable]);
        result = result.concat("(");
        
        List<String> nameComponent = new ArrayList<String>();
        
        nameComponent.add(Integer.toString(window));
        if (typeVariable != VAR_DEPLOY) {
            nameComponent.add(Integer.toString(queryId));
        }
        
        if (typeVariable == VAR_X || typeVariable == VAR_Y) {
            nameComponent.add(Integer.toString(k));
        }
        
        if (typeVariable == VAR_X) {
            nameComponent.add(Integer.toString(i));
            nameComponent.add(Integer.toString(a));
        }
        
        result = result.concat(StringConcatenator.concatenate(",", nameComponent));
        result = result.concat(")");
                
        return result;  
    }
    
    public static int getVarType(final String name)
    {
        if (name.contains("y(")) {
            return VAR_Y;
        }

        if (name.contains("x(")) {
            return VAR_X;
        }

        if (name.contains("s(")) {
            return VAR_S;
        }
        
        if (name.contains("create(")) {
            return VAR_CREATE;
        }
        
        if (name.contains("drop(")) {
            return VAR_DROP;
        }
        
        if (name.contains("present(")) {
            return VAR_PRESENT;
        }
        
        if (name.contains("deploy(")) {
            return VAR_DEPLOY;
        }
        return VAR_DEFAULT;     
    }
    
}
