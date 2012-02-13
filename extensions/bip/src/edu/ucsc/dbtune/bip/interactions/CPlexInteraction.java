package edu.ucsc.dbtune.bip.interactions;

import ilog.concert.IloException;
import ilog.concert.IloLPMatrix;
import ilog.concert.IloNumVar;
import ilog.cplex.IloCplex;

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
     * 
     * @param mapVarCoef
     *      The coefficient for variable in the alternative constraint
     * @param isSolvealternativeOnly
     *      The boolean value to indicate whether we only need to solve the alternative constraint
     * @param inputFile
     *      This parameter is taken into account if {@code isSolvealternativeOnly = true}
     *                 
     * @return 
     *      a map variables to their assigned values if CPLEX has a solution
     *      or {@code null}, otherwise     
     */
    public Map<String, Integer> solveAlternativeInteractionConstraint(
                                Map<String, Double> mapVarCoef, 
                                boolean isSolvealternativeOnly,
                                String inputFile)
    {
        Map<String, Integer> mapVariableValue;
        IloNumVar            var;
        IloLPMatrix          matrix;
        IloNumVar[]          vars;
        double[]             listCoef;
        
        int    last_row_id;
        double coef;
        Object found;
        
        mapVariableValue = null;
        
        try {
            
            if(isSolvealternativeOnly) {
                // We have not imported the model from the file before
                cplex = new IloCplex();
                
                cplex.setParam(IloCplex.DoubleParam.EpGap, 0.05);
                
                // not output the log of CPLEX
                cplex.setOut(null);
                // not output the warning
                cplex.setWarning(null);
                
                // Read model from file
                cplex.importModel(inputFile);
            }
            
            matrix      = getMatrix(cplex);
            vars        = matrix.getNumVars();
            last_row_id = matrix.getNrows() - 1;
            listCoef    = new double[vars.length];
            
            // Remove the last constraint for the index interaction
            // and replace by the alternative one                    
            matrix.removeRow(last_row_id);
            
            for (int i = 0; i < vars.length; i++) {
                
                var   = vars[i];
                coef  = 0.0;
                found = mapVarCoef.get(var.getName());
                
                if (found != null) 
                    coef = (Double) found;
                      
                listCoef[i] = coef;
                
            }
            cplex.addLe(cplex.scalProd(listCoef, vars), 0);
            
            if (cplex.solve()) 
                mapVariableValue = super.getMapVariableValue();
            
            
        } catch (IloException e) {
            e.printStackTrace();
        }
        
        return mapVariableValue;
    }
}
