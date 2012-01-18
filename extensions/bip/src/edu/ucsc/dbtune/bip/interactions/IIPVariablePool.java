package edu.ucsc.dbtune.bip.interactions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.ucsc.dbtune.bip.core.AbstractBIPVariablePool;
import edu.ucsc.dbtune.bip.util.BIPVariable;
import edu.ucsc.dbtune.bip.util.StringConcatenator;

public class IIPVariablePool extends AbstractBIPVariablePool 
{
    public static final int  VAR_Y = 0;
    public static final int  VAR_X = 1;
    public static final int  VAR_S = 2;
    public static final int  VAR_U = 3;
    public static final int  VAR_DEFAULT = 100;    
    private String[] strTheta = {"empty", "c", "d", "cd"};
    private String[] strHeaderVariable = {"y", "x", "s", "u"};    
    private Map<IIPVariableIndicator, BIPVariable> mapHighDimensionVar;
    
    public IIPVariablePool()
    {
        mapHighDimensionVar = new HashMap<IIPVariableIndicator, BIPVariable>();
    }
    /**
     * 
     * Create the corresponding variable name in the form: y(empty, k), x(c,k,i,a), s(d, a), or u(cd, k,i,a).
     *  
     * @param theta
     *      The value of @theta in the set of {empty, c, d, cd}
     * @param typeVarible
     *      The type of variable, the value is in the set {y, x, u, s}
     * @param k
     *      The identifier of the corresponding template plan if @typeVariable = VAR_Y, VAR_X, VAR_U; 
     *      Or the identifier of the index if @typeVariable = VAR_S
     * @param i 
     *      The position of slot in the template plan
     *      Only enable when @typeVariable = VAR_X, VAR_U
     * @param a 
     *      The position of the index in the corresponding slot
     *      Only enable when @typeVariable = VAR_X, VAR_U
     * 
     * @return
     *      The variable name
     */
    public BIPVariable createAndStore(int theta, int typeVariable, int k, int i, int a)
    {
        String varName = "";
        varName = varName.concat(strHeaderVariable[typeVariable]);
        varName = varName.concat("(");
        List<String> nameComponent = new ArrayList<String>();
        
        nameComponent.add(strTheta[theta]);
        nameComponent.add(Integer.toString(k));
        
        if (typeVariable == VAR_X || typeVariable == VAR_U) {           
            nameComponent.add(Integer.toString(i));
            nameComponent.add(Integer.toString(a));
        }
        
        varName = varName.concat(StringConcatenator.concatenate(",", nameComponent));
        varName = varName.concat(")");
                
        BIPVariable var = new BIPVariable(varName, typeVariable);
        this.add(var); 
        
        // Create a mapping from 
        IIPVariableIndicator iai = new IIPVariableIndicator(theta, typeVariable, k, i, a);
        this.mapHighDimensionVar.put(iai, var);
        
        return var;
    }
    
    /**
     * Get the corresponding variable
     * 
     * @param theta
     *      EMPTY, C,D, or CD
     * @param typeVariable
     *      either Y,X,S, or U
     * @param k
     *      template plan ID
     * @param i
     *      slot ID
     * @param a
     *      position of index in the corresponding slot
     * @return
     *      BIP Variable
     */
    public BIPVariable get(int theta, int typeVariable, int k, int i, int a)
    {
        // Create a mapping from 
        IIPVariableIndicator iai = new IIPVariableIndicator(theta, typeVariable, k, i, a);
        Object found = mapHighDimensionVar.get(iai);
        BIPVariable var = null;
        if (found != null) {
            var = (BIPVariable) found;
        } 
        
        return var;
    }
}
