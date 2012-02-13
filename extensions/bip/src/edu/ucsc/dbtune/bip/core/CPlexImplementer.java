package edu.ucsc.dbtune.bip.core;

import ilog.concert.IloException;
import ilog.concert.IloLPMatrix;
import ilog.concert.IloNumVar;
import ilog.cplex.IloCplex;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CPlexImplementer implements CPlexSolver 
{   
    public static double OBJ_VALUE_UNKNOWN = -999;
    protected IloCplex cplex;
    protected boolean isSolvable;
    protected int nRows;
    protected int nCols;
    protected double obj;
    
    
    public CPlexImplementer()
    {
        isSolvable = false;
    }
 
    /**
     * Clear all the data structures used by CPLEX to explicitly release the memory space
     *  
     */
    public void clearModel()
    {
        try {
            cplex.clearModel();
            cplex.endModel();
            cplex.end();
            cplex = null;
        } catch (IloException e) {
            System.err.println("Concert exception caught: " + e);
        }    
    }
    
    @Override
    public int getNumberOfConstraints() 
    {
        return nRows;
    }

    @Override
    public int getNumberOfVariables() 
    {
        return nCols;
    }

    @Override
    public double getObjectiveValue() 
    {
        return obj;
    }

    
    @Override
    public Map<String, Integer> solve(String inputFile) 
    {
        Map<String, Integer> mapVariableValue = null;
        isSolvable = false;
        try {
            // start CPLEX
            cplex = new IloCplex();
            
            // allow the solution differed 5% from the actual optimal value
            cplex.setParam(IloCplex.DoubleParam.EpGap, 0.05);
            // not output the log of CPLEX
            cplex.setOut(null);
            // not output the warning
            cplex.setWarning(null);
            // set the mode to be parallel barrier (determistic)
            //cplex.setParam(IloCplex.IntParam.RootAlg, IloCplex.Algorithm.Barrier);   
            //cplex.setParam(IloCplex.IntParam.ParallelMode, IloCplex.ParallelMode.Deterministic);
            // Read model from file into cplex optimizer object
            cplex.importModel(inputFile);
            
            if (cplex.solve()) {
                mapVariableValue = getMapVariableValue();
                obj = cplex.getObjValue();
                isSolvable = true;
            } else {
                isSolvable = false;
                obj = OBJ_VALUE_UNKNOWN;
            }

            // collect some statistics    
            nRows = cplex.getNrows();
            nCols = cplex.getNcols();
        }
        catch (IloException e) {
            System.err.println("Concert exception caught: " + e);
        }
        
        return mapVariableValue;
    }
    
    
    /**
     * Retrieve the result from CPLEX
     * 
     * @return
     *      A mapping that maps variable names to their values
     */
    protected Map<String, Integer> getMapVariableValue()
    {
        Map<String, Integer> mapVariableValue = new HashMap<String, Integer>();
        try {
            IloLPMatrix matrix = getMatrix(cplex);
            IloNumVar [] vars = matrix.getNumVars();
            double val;
            IloNumVar var;
            for (int i = 0; i < vars.length; i++) {
                var = vars[i];
                val = cplex.getValue(var);
                mapVariableValue.put(var.getName(), (int)Math.round(val));
            }
        } catch (IloException e) {
            System.err.println("Concert exception caught: " + e);
        }
        
        return mapVariableValue;
    }
     
    /**
     * Retrieve the matrix used in the BIP problem
     * 
     * @param cplex
     *      The model of the BIP problem
     * @return
     *      The matrix of @cplex      
     */ 
    protected IloLPMatrix getMatrix(IloCplex cplex) throws IloException 
    {
        @SuppressWarnings("unchecked")
        Iterator iter = cplex.getModel().iterator();
        
        while (iter.hasNext()) {
            Object o = iter.next();
            if (o instanceof IloLPMatrix) {
                IloLPMatrix matrix = (IloLPMatrix) o;
                return matrix;
            }
        }
        return null;
    }

    @Override
    public String toString() 
    {
        StringBuilder result = new StringBuilder("CPlexImplementer: \n");
        result.append(" Number of variables: " + cplex.getNcols());
        result.append(" Number of constraints: " + cplex.getNrows());
        
        return result.toString();
    }

}
