package edu.ucsc.dbtune.bip.sim;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.ucsc.dbtune.bip.core.AbstractBIPVariablePool;
import edu.ucsc.dbtune.bip.util.BIPVariable;
import edu.ucsc.dbtune.bip.util.StringConcatenator;

public class SimVariablePool  extends AbstractBIPVariablePool
{
    public static final int  VAR_Y = 0;
    public static final int  VAR_X = 1;
    public static final int  VAR_PRESENT = 2;
    public static final int  VAR_CREATE = 3;
    public static final int  VAR_DROP = 4;
    public static final int  VAR_DEFAULT = 100;    
    private String[] strHeaderVariable = {"y", "x", "present", "create", "drop"};
    private Map<SimVariableIndicator, BIPVariable> mapHighDimensionVar;
    
    public SimVariablePool()
    {
        mapHighDimensionVar = new HashMap<SimVariableIndicator, BIPVariable>();
    }
    /**
     * 
     * Construct the variable name in the format: y(w, q, k), x(w, q, k, a),  
     * present(w,a), create(w,a), drop(w,a)      
     *
     * @param typeVarible
     *      The type of variable, the value is in the set {y, x, present, create, drop}, 
     * @param window
     *      The window time in the materialized schedule
     * @param queryId
     *      The identifier of the processing query when {@code typeVariable} = VAR_Y, VAR_X
     * @param k
     *      The template plan identifier
     *      Only enable when {@code typeVariable} = VAR_Y, VAR_X
     * @param a 
     *      The identifier of the index if {@code typeVariable} = VAR_X, VAR_PRESENT, VAR_CREATE, VAR_DROP
     * 
     * @return
     *      The variable name
     */
    public SimVariable createAndStore(int typeVariable, int window, int queryId, int k, int a)
    {
        StringBuilder varName = new StringBuilder();
        varName.append(strHeaderVariable[typeVariable]);
        varName.append("(");
        
        List<String> nameComponent = new ArrayList<String>();        
        nameComponent.add(Integer.toString(window));
        
        if (typeVariable == VAR_X || typeVariable == VAR_Y) {
            nameComponent.add(Integer.toString(queryId));
            nameComponent.add(Integer.toString(k));
        }
        
        if (typeVariable == VAR_X || typeVariable == VAR_PRESENT || typeVariable == VAR_CREATE || typeVariable == VAR_DROP) {
            nameComponent.add(Integer.toString(a));
        }
        
        varName.append(StringConcatenator.concatenate(",", nameComponent));
        varName.append(")");
         
        // store the variable with the derived name
        SimVariable var = new SimVariable(varName.toString(), typeVariable, window);
        this.add(var);
        
        // Create a mapping for this variable 
        SimVariableIndicator iai = new SimVariableIndicator(typeVariable, window, queryId, k, a);
        this.mapHighDimensionVar.put(iai, var);
        
        return var;
    }
    
    /**
     * Get the corresponding variable
     * 
     * @param typeVariable
     *      either Y, X, create, drop, or present
     * @param window
     *      window ID
     * @queryId     
     *      the ID of the select-statement      
     * @param k
     *      template plan ID
     * @param a
     *      index ID
     * @return
     *      BIP Variable
     */
    public SimVariable get(int typeVariable, int window, int queryId, int k, int a)
    {   
        SimVariableIndicator iai = new SimVariableIndicator(typeVariable, window, queryId, k, a);
        Object found = mapHighDimensionVar.get(iai);
        SimVariable var = null;
        if (found != null) {
            var = (SimVariable) found;
        } 
        
        return var;
    }
}
