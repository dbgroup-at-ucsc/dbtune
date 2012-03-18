package edu.ucsc.dbtune.bip.div;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.ucsc.dbtune.bip.core.AbstractBIPVariablePool;
import edu.ucsc.dbtune.util.Strings;

public class DivVariablePool extends AbstractBIPVariablePool 
{
    public static final int VAR_Y      = 0;
    public static final int VAR_X      = 1;
    public static final int VAR_S      = 2;
    public static final int VAR_DEPLOY = 3;
    public static final int VAR_DIV    = 4;
    public static final int VAR_MOD    = 5;
    public static final int VAR_U      = 6;
    
    public static final int VAR_DEFAULT = 100;    
    private String[] strHeaderVariable = {"y", "x", "s", "deploy", "div", "mod", "u"};
    private Map<DivVariableIndicator, DivVariable> mapHighDimensionVar;
    
    public DivVariablePool()
    {
        mapHighDimensionVar = new HashMap<DivVariableIndicator, DivVariable>();
    }
    
    /**
     * 
     * Construct the variable name in the form: y(r, q, k), x(r, q, k,i,a), s(r, a), deploy(r),
     * div(r, a), mod(r, a)
     *  
     * @param typeVarible
     *      The type of variable, the value is in the set {y, x, s, deploy}, 
     * @param replica
     *      The replica ID
     * @param queryId
     *      The identifier of the processing query if {@code typeVariable = VAR_Y, VAR_X, VAR_U};
     * @param k
     *      The template plan identifier
     *      Only enable when {@code typeVariable = VAR_X, VAR_Y, VAR_U}
     * @param a 
     *      The index ID
     *      Enable when {@code typeVariable = VAR_X, VAR_S, VAR_U}
     * 
     * @return
     *      The variable name
     */
    public DivVariable createAndStore(int typeVariable, int replica, int queryId, 
                                      int k, int a)
    {
        StringBuilder varName = new StringBuilder();
        
        varName.append(strHeaderVariable[typeVariable])
               .append("(");
        
        List<String> nameComponent = new ArrayList<String>();        
        nameComponent.add(Integer.toString(replica));        
        
        if (typeVariable == VAR_X || typeVariable == VAR_Y ||typeVariable == VAR_U) {
            nameComponent.add(Integer.toString(queryId));
            nameComponent.add(Integer.toString(k));
        }
        
        if (typeVariable == VAR_X || typeVariable == VAR_S ||typeVariable == VAR_U
            || typeVariable == VAR_DIV || typeVariable == VAR_MOD)
            nameComponent.add(Integer.toString(a));
        
        varName.append(Strings.concatenate(",", nameComponent))
               .append(")");
                
        DivVariable var = new DivVariable(varName.toString(), typeVariable, replica);
        add(var);
        
        // Create a mapping  
        DivVariableIndicator iai = new DivVariableIndicator(typeVariable, replica, queryId, k, a);
        mapHighDimensionVar.put(iai, var);
        
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
     *      index's ID
     * @return
     *      BIP Variable
     */
    public DivVariable get(int typeVariable, int replica, int queryId, int k, int a)
    {   
        DivVariableIndicator iai = new DivVariableIndicator(typeVariable, replica, queryId, k, a);
        return mapHighDimensionVar.get(iai);
    }
    
    
}