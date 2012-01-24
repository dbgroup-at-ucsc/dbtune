package edu.ucsc.dbtune.bip.interactions;

import ilog.concert.IloException;
import ilog.concert.IloNumVar;
import java.util.Map;

import edu.ucsc.dbtune.bip.core.CPlexImplementer;

public class CPlexInteraction extends CPlexImplementer 
{   
    /**
     * Replace the last constraint in the formulated BIP by the alternative constraint
     * <p>
     * <ol> 
     *  <li> Remove the last constraint in the CPEX </li> 
     *  <li> Retrieve the matrix, and assign the value for the coefficient in the matrix</li>
     *  <li> Add the alternative index interaction constraint </li>
     * </ol>
     * </p>
     * @param mapVarCoef
     *      The coefficient for variable in the alternative constraint
     * @return 
     *      a map variables to their assigned values if CPLEX has a solution
     *      or {@code null}, otherwise     
     */
    public Map<String, Integer> solveAlternativeInteractionConstraint(Map<String, Double> mapVarCoef)
    {
        Map<String, Integer> mapVariableValue = null;
        try {
            matrix = getMatrix(cplex);
            vars = matrix.getNumVars();
            // Remove the last constraint for the index interaction
            // and replace by the alternative one
            int last_row_id = matrix.getNrows() - 1;        
            matrix.removeRow(last_row_id);
            
            double[] listCoef = new double[vars.length];        
            for (int i = 0; i < vars.length; i++) {
                IloNumVar var = vars[i];
                double coef = 0.0;
                Object found = mapVarCoef.get(var.getName());
                if (found != null) {
                    coef = ((Double)found).doubleValue();
                }
                      
                listCoef[i] = coef;
            }
            cplex.addLe(cplex.scalProd(listCoef, vars), 0);
            
            if (cplex.solve()){
                mapVariableValue = super.getMapVariableValue();
            }
        } catch (IloException e) {
            e.printStackTrace();
        }
        
        return mapVariableValue;
    }
}
