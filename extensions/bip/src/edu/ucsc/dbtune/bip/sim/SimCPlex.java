package edu.ucsc.dbtune.bip.sim;

import java.io.*;
import java.util.*;

import ilog.concert.*;
import ilog.cplex.*;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.bip.interactions.IIPLinGenerator;
import edu.ucsc.dbtune.bip.util.CPlexBuffer;
import edu.ucsc.dbtune.bip.util.QueryPlanDesc;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.bip.util.QueryPlanDesc;

public class SimCPlex {
	private IloCplex cplex;	
	private Environment environment = Environment.getInstance();
	private SimLinGenerator genSim;
	private IloLPMatrix matrix;
	private IloNumVar [] vars;

	
	/**
	 * Find pairs of indexes from the pool of candidate indexes that interact with each other
	 * 
	 * @param desc
	 *     Query plan description including (internal plan, access costs) derived from INUM	 
	 * @param W
	 * 		The number of window time
	 * @param B
	 * 		The maximum space budget at each window time 	   
	 * 
	 * @return
	 * 		The set of materialized indexes with marking the time window when this index is created/dropped
	 */
	public List<MatIndex> run(List<QueryPlanDesc> listQueryPlan, int W, List<Double> B)  {
		
		LogListener listener = new LogListener() {
            public void onLogEvent(String component, String logEvent) {
                //To change body of implemented methods use File | Settings | File Templates.
            }
        };
        List<MatIndex> listIndex = new ArrayList<MatIndex>();
        String cplexFile = "", binFile = "", consFile = "", objFile = "";	
        String workloadName = environment.getTempDir() + "/testwl";
        
        try {														
			genSim = new SimLinGenerator(workloadName, listQueryPlan, W, B);
			
			// Build BIP for a particular (c,d, @desc)
			genSim.build(listener); 
			
			cplexFile = workloadName + ".lp";
			binFile = workloadName + ".bin";
			consFile = workloadName + ".cons";
			objFile = workloadName + ".obj";
			
			CPlexBuffer.concat(cplexFile, objFile, consFile, binFile);							
        }
		catch(IOException e){
        	System.out.println("Error " + e);
        }	
        
		
	//  Load the corresponding CPLEX problem from the corresponding text file
        try {		        
            cplex = new IloCplex(); 
                      
            // Read model from file with name @cplexFile into cplex optimizer object
            cplex.importModel(cplexFile); 
            
            
            // Solve the model and record the solution into @listIndex 
            // if one was found
            if (cplex.solve()) 
            {				               
               listIndex = getMaterializeSchedule();
               System.out.println(" In CPlex, objective function value: " + cplex.getObjValue());
            } 
            else 
            {
            	System.out.println(" INFEASIBLE soltuion ");
            }
        }
        catch (IloException e) {
            System.err.println("Concert exception caught: " + e);
         }
        
        return listIndex;        
	}
	
	/**
	 * Return materialized index scheduling: which index is created/dropped at each window time
	 * 		Find out variable create_{w,i} = 1 and drop_{w,i} = 1
	 * 
	 * @return
	 * 		List of materialized indexes
	 */
	private List<MatIndex> getMaterializeSchedule()
	{ 
		List<MatIndex> listIndex = new ArrayList<MatIndex>();
		// Iterate over variables create_{i,w} and drop_{i,w}
		try {
			matrix = getMatrix(cplex);
			vars = matrix.getNumVars();
			
			for (int i = 0; i < vars.length; i++) 
			{
				IloNumVar var = vars[i];
				if (cplex.getValue(var) == 1)
				{
					MatIndex index = SimLinGenerator.deriveMatIndex(var.getName());
					if (index != null)
					{
						listIndex.add(index);
					}
				}
				
			}
			
		}
		catch (IloException e) {
			System.err.println("Concert exception caught: " + e);
	    }
		
		return listIndex;
	}
	
	/**
	 * Determine the matrix used in the BIP problem
	 * 
	 * @param cplex
	 * 		The model of the BIP problem
	 * @return
	 * 	    The matrix of @cplex	  
	 */	
	public IloLPMatrix getMatrix(IloCplex cplex) throws IloException {
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
