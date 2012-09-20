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
    public static final int VAR_YO     = 6;
    public static final int VAR_XO     = 7;
    public static final int VAR_U      = 8;
    public static final int VAR_SUM_Y  = 9;
    public static final int VAR_COMBINE_Y = 10;
    public static final int VAR_COMBINE_X = 11;
    
    public static final int VAR_DEFAULT = 100;    
    private String[] strHeaderVariable = {"y", "x", "s",                      // basic  
                                          "deploy", "div", "mod",             // elastic
                                          "yo", "xo", "u",                    // imbalance factor
                                          "sum_y", "combine_y", "combine_x"}; // node failure
    
    private Map<DivVariableIndicator, DivVariable> mapHighDimensionVar;
    
    public DivVariablePool()
    {
        mapHighDimensionVar = new HashMap<DivVariableIndicator, DivVariable>();
    }
    
    /**
     * 
     * Construct the variable name.   
     *  
     * @param typeVarible
     *      The type of variable 
     * @param replica
     *      The replica ID
     * @param queryId
     *      The identifier of the processing query 
     * @param planId
     *      The template plan identifier
     * @param slotId
     *      The slot Id     
     * @param idx 
     *      The index ID
     * 
     * @return
     *      The variable object
     */
    public DivVariable createAndStore(int typeVariable, int replica, int queryId, 
                                      int planId, int slotId, int idx)
    {
        // Name of variables in the form:
        //
        // y(r, q, k), x(r, q, k, i, a), s(r, a),
        // deploy(r), div(r, a), mod(r, a), 
        // yo(r, q, k), xo(r, q, k, i, a), u(r, q, k, i, a), 
        // sum_y(r, q), combine_y(r, q, k), combine_x(r, q, k, i, a)
        //

        StringBuilder varName = new StringBuilder();
        
        varName.append(strHeaderVariable[typeVariable])
               .append("(");
        
        List<String> nameComponent = new ArrayList<String>();        
        nameComponent.add(Integer.toString(replica));        
        
        if (typeVariable == VAR_X || typeVariable == VAR_Y 
            || typeVariable == VAR_XO || typeVariable == VAR_YO || typeVariable == VAR_U
            || typeVariable == VAR_COMBINE_X || typeVariable == VAR_COMBINE_Y) {
            
            nameComponent.add(Integer.toString(queryId));
            nameComponent.add(Integer.toString(planId));
        }
        
        if (typeVariable == VAR_SUM_Y)
            nameComponent.add(Integer.toString(queryId));
        
        if (typeVariable == VAR_X || typeVariable == VAR_S 
            || typeVariable == VAR_DIV || typeVariable == VAR_MOD
            || typeVariable == VAR_XO || typeVariable == VAR_U
            || typeVariable == VAR_COMBINE_X) {
            nameComponent.add(Integer.toString(slotId));
            nameComponent.add(Integer.toString(idx));
        }
        
        varName.append(Strings.concatenate(",", nameComponent))
               .append(")");
                
        DivVariable var = new DivVariable(varName.toString(), typeVariable, replica);
        add(var);
        
        // Create a mapping
        mapHighDimensionVar.put
                (new DivVariableIndicator(typeVariable, replica, queryId, 
                            planId, slotId, idx), var);
        
        return var;
    }
    
    /**
     * Get the corresponding variable
     * 
     * @param typeVariable
     *      the type of variables defined in the model
     * @param replica
     *      replica ID
     * @param queryId     
     *      the ID of the select-statement      
     * @param planId
     *      template plan ID
     * @param slotId
     *      slot ID
     * @param idx
     *      index's ID
     * @return
     *      BIP Variable
     */
    public DivVariable get(int typeVariable, int replica, int queryId, 
                        int planId, int slotId, int idx)
    {   
        return mapHighDimensionVar.get(new DivVariableIndicator
                                            (typeVariable, replica, queryId, 
                                            planId, slotId, idx));
    }
}