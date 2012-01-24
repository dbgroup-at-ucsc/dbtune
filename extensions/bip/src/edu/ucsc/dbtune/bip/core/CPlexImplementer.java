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
    
    @Override
    public Map<String, Integer> solve(String inputFile) 
    {
        Map<String, Integer> mapVariableValue = null;
        try {               
            cplex = new IloCplex(); 
                      
            // Read model from file into cplex optimizer object
            cplex.importModel(inputFile);
            
            if (cplex.solve() == true){
                mapVariableValue = this.getMapVariableValue();
            }
        }
        catch (IloException e) {
            System.err.println("Concert exception caught: " + e);
        }
        
        return mapVariableValue;
    }
    
    protected Map<String, Integer> getMapVariableValue()
    {
        Map<String, Integer> mapVariableValue = new HashMap<String, Integer>();
        try {
            matrix = getMatrix(cplex);
            vars = matrix.getNumVars();
            
            for (int i = 0; i < vars.length; i++) {
                IloNumVar var = vars[i];
                double val = cplex.getValue(var);
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
}
