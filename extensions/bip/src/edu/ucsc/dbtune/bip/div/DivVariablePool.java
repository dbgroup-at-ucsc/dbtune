package edu.ucsc.dbtune.bip.div;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import edu.ucsc.dbtune.bip.util.BIPVariablePool;
import edu.ucsc.dbtune.bip.util.StringConcatenator;

public class DivVariablePool extends BIPVariablePool 
{
    public static final int VAR_Y = 0;
    public static final int VAR_X = 1;
    public static final int VAR_S = 2;
    public static final int VAR_DEPLOY = 3;
    public static final int VAR_DIV = 4;
    public static final int VAR_MOD = 5;
    public static final int VAR_DEFAULT = 100;    
    private String[] strHeaderVariable = {"y", "x", "s", "deploy", "div", "mod"};
    private Map<DivVariableIndex, DivVariable> mapHighDimensionVar = new HashMap<DivVariableIndex, DivVariable>();
    
    /**
     * 
     * Construct the variable name in the form: y(r, q, k), x(r, q, k,i,a), s(r, a), deploy(r)
     *  
     * @param typeVarible
     *      The type of variable, the value is in the set {y, x, s, deploy}, 
     * @param replica
     *      The replica ID
     * @param queryId
     *      The identifier of the processing query if {@code typeVariable = VAR_Y, VAR_X}; 
     *      Or the identifier of the index if {@code typeVariable = VAR_S}
     *      Not applicable for {@code typeVariable = VAR_DEPLOY}
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
    public DivVariable createAndStoreBIPVariable(int typeVariable, int replica, int queryId, int k, int i, int a)
    {
        String varName = "";
        varName = varName.concat(strHeaderVariable[typeVariable]);
        varName = varName.concat("(");
        
        List<String> nameComponent = new ArrayList<String>();        
        nameComponent.add(Integer.toString(replica));        
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
        
        varName = varName.concat(StringConcatenator.concatenate(",", nameComponent));
        varName = varName.concat(")");
                
        DivVariable var = new DivVariable(varName, typeVariable, replica);
        this.listVar.add(var);
        this.mapNameVar.put(var.getName(), var);
        
        // Create a mapping from 
        DivVariableIndex iai = new DivVariableIndex(typeVariable, replica, queryId, k, i, a);
        this.mapHighDimensionVar.put(iai, var);
        
        return var;
    }
    
    /**
     * Get the corresponding variable
     * 
     * @param typeVariable
     *      either Y, X, S, or deploy
     * @param replica
     *      replica ID
     * @queryId     
     *      the ID of the select-statement      
     * @param k
     *      template plan ID
     * @param i
     *      slot ID
     * @param a
     *      position of index in the corresponding slot
     * @return
     *      BIP Variable
     */
    public DivVariable getDivVariable(int typeVariable, int replica, int queryId, int k, int i, int a)
    {   
        DivVariableIndex iai = new DivVariableIndex(typeVariable, replica, queryId, k, i, a);
        Object found = mapHighDimensionVar.get(iai);
        DivVariable var = null;
        if (found != null) {
            var = (DivVariable) found;
        } 
        
        return var;
    }
}