package edu.ucsc.dbtune.bip.sim;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.ucsc.dbtune.bip.core.AbstractBIPVariablePool;
import edu.ucsc.dbtune.bip.util.BIPVariable;
import edu.ucsc.dbtune.bip.util.StringConcatenator;

public class SimVariablePool extends AbstractBIPVariablePool 
{
    public static final int  VAR_Y = 0;
    public static final int  VAR_X = 1;
    public static final int  VAR_S = 2;
    public static final int  VAR_PRESENT = 3;
    public static final int  VAR_CREATE = 4;
    public static final int  VAR_DROP = 5;
    public static final int  VAR_DEFAULT = 100;    
    private String[] strHeaderVariable = {"y", "x", "s", "present", "create", "drop"};
    private Map<SimVariableIndicator, BIPVariable> mapHighDimensionVar;
    
    public SimVariablePool()
    {
        mapHighDimensionVar = new HashMap<SimVariableIndicator, BIPVariable>();
    }
    /**
     * 
     * Construct the variable name in the format: y(w, q, k), x(w,q, k,i,a), s(w, a), 
     * present(w,a), create(w,a), drop(w,a)      
     *
     * @param typeVarible
     *      The type of variable, the value is in the set {y, x, s, present, create, drop}, 
     * @param window
     *      The window time in the materialized schedule
     * @param queryId
     *      The identifier of the processing query if {@code typeVariable = VAR_Y, VAR_X}; 
     *      Or the identifier of the index if {@code typeVariable = VAR_S, VAR_PRESENT, VAR_CREATE, VAR_DROP}
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
    public SimVariable createAndStore(int typeVariable, int window, int queryId, int k, int i, int a)
    {
        String varName = "";
        varName = varName.concat(strHeaderVariable[typeVariable]);
        varName = varName.concat("(");
        
        List<String> nameComponent = new ArrayList<String>();        
        nameComponent.add(Integer.toString(window));
        nameComponent.add(Integer.toString(queryId));
        
        if (typeVariable == VAR_X || typeVariable == VAR_Y) {
            nameComponent.add(Integer.toString(k));
        }
        
        if (typeVariable == VAR_X) {
            nameComponent.add(Integer.toString(i));
            nameComponent.add(Integer.toString(a));
        }
        
        varName = varName.concat(StringConcatenator.concatenate(",", nameComponent));
        varName = varName.concat(")");
         
        // store the variable with the derived name
        SimVariable var = new SimVariable(varName, typeVariable, window);
        this.add(var);
        
        // Create a mapping for this variable 
        SimVariableIndicator iai = new SimVariableIndicator(typeVariable, window, queryId, k, i, a);
        this.mapHighDimensionVar.put(iai, var);
        
        return var;
    }
    
    /**
     * Get the corresponding variable
     * 
     * @param typeVariable
     *      either Y, X, S, create, drop, or present
     * @param window
     *      window ID
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
    public SimVariable get(int typeVariable, int window, int queryId, int k, int i, int a)
    {   
        SimVariableIndicator iai = new SimVariableIndicator(typeVariable, window, queryId, k, i, a);
        Object found = mapHighDimensionVar.get(iai);
        SimVariable var = null;
        if (found != null) {
            var = (SimVariable) found;
        } 
        
        return var;
    }
}
