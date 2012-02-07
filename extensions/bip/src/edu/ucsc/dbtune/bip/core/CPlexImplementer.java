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
    protected IloLPMatrix matrix;
    protected IloNumVar [] vars;
    protected IloCplex cplex;
    protected boolean isSolvable;
    public static double OBJ_VALUE_UNKNOWN = -999;
    
    public CPlexImplementer()
    {
        isSolvable = false;
    }
    
    @Override
    public Map<String, Integer> solve(String inputFile) 
    {
        Map<String, Integer> mapVariableValue = null;
        isSolvable = false;
        try {
            // if cplex is NOT null, clear model
            if (cplex != null)
                cplex.clearModel();
            else 
                cplex = new IloCplex();
            
            // allow the solution differed 5% from the actual optimal value
            cplex.setParam(IloCplex.DoubleParam.EpGap, 0.05);
            // not output the log of CPLEX
            cplex.setOut(null);
            // not output the warning
            cplex.setWarning(null);
            // set the mode to be parallel barrier (determistic)
            //cplex.setParam(IloCplex.IntParam.RootAlg, IloCplex.Algorithm.Barrier);   
            cplex.setParam(IloCplex.IntParam.ParallelMode, IloCplex.ParallelMode.Deterministic);
            // Read model from file into cplex optimizer object
            cplex.importModel(inputFile);
            
            if (cplex.solve()) {
                mapVariableValue = getMapVariableValue();
                isSolvable = true;
            }
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
            matrix = getMatrix(cplex);
            vars = matrix.getNumVars();
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

    @Override
    public int getNumberOfConstraints() 
    {
        return cplex.getNrows();
    }

    @Override
    public int getNumberOfVariables() 
    {
        return cplex.getNcols();
    }

    @Override
    public double getObjectiveValue() 
    {
        if (isSolvable) {
            try {
                return cplex.getObjValue();
            } catch (IloException e) {
                System.err.append("Error with CPlex object" + e.getMessage());
            }
        }
        return CPlexImplementer.OBJ_VALUE_UNKNOWN;
    }
}
