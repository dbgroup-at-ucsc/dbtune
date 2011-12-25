package edu.ucsc.dbtune.bip.sim;

import ilog.concert.IloException;
import ilog.concert.IloLPMatrix;
import ilog.concert.IloNumVar;
import ilog.cplex.IloCplex;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.ucsc.dbtune.bip.util.BIPPreparatorSchema;
import edu.ucsc.dbtune.bip.util.CPlexBuffer;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.bip.util.QueryPlanDesc;
import edu.ucsc.dbtune.bip.util.WorkloadPerSchema;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Environment;
import edu.ucsc.dbtune.workload.SQLStatement;
import edu.ucsc.dbtune.bip.sim.ScheduleIndexPool;

public class SimBIP 
{	
	private IloCplex cplex;	
	private Environment environment = Environment.getInstance();
	private SimLinGenerator genSim;
	private IloLPMatrix matrix;
	private IloNumVar [] vars;
	private ScheduleIndexPool poolIndexes;
	private int W;
	
	/**	
	 * Scheduling materialization index
	 * 
	 * @param Sinit
	 * 		The initial configuration
	 * @param Smat
	 * 		The configuration to be materialized	
	 * @param listWorkload
     *      Each entry of this list is a list of SQL Statements that belong to a same schema
	 * @param W
	 * 		The number of window time
	 * @param B
	 * 		The maximum space to be constrained all the materialized index at each the window time
	 * 
	 * 
	 * @return
	 * 		A sequence of index to be materialized (created/dropped) in the corresponding window time
	 * @throws SQLException 
	 * 
	 * {\b Note}: {@code listAgent} will be removed when this class is fully implemented
	 */
    public MaterializationSchedule schedule(List<Index> Sinit, List<Index> Smat, 
                                   List<WorkloadPerSchema> listWorkload, List<BIPPreparatorSchema> listPreparators,
                                   int W, double B) throws SQLException
	{  
        this.W = W;
        poolIndexes = new ScheduleIndexPool();
      
		// 1. Derive Sin, Sout, Sremain and store in {@code poolIndexes}
		insertIndexesToPool(Sinit, Smat);
		
		// 2. Derive the query plan description including internal cost, index access cost,
		// index at each slot ... 
		int iPreparator = 0;
		List<QueryPlanDesc> listQueryPlans = new ArrayList<QueryPlanDesc>();
		
		for (WorkloadPerSchema wl : listWorkload) {
		    //BIPPreparatorSchema agent = new BIPPreparatorSchema(wl.getSchema());
		    // TODO: Change this line when the implementation of BIPAgentPerSchema is done
		    BIPPreparatorSchema preparator = listPreparators.get(iPreparator++);
            
		    for (Iterator<SQLStatement> iterStmt = wl.getWorkload().iterator(); iterStmt.hasNext(); ) {
		        QueryPlanDesc desc =  new QueryPlanDesc();                    
                // Populate the INUM space for each statement
                // We do not add full table scans before populate from @desc
                // so that the full table scan is placed at the end of each slot 
                desc.generateQueryPlanDesc(preparator, iterStmt.next(), poolIndexes);
	            listQueryPlans.add(desc);
		    }
		    // Add full table scans into the pool
            for (Index index : preparator.getListFullTableScanIndexes()) {
                poolIndexes.addIndex(index);
            }
            
            // Map index in each slot to its pool ID
            for (QueryPlanDesc desc : listQueryPlans) {
                desc.mapIndexInSlotToPoolID(poolIndexes);
            }
		}
		
		// Formulate BIP and run the BIP to derive the final result 
		// which is a list of materialized indexes		
		return findIndexSchedule(listQueryPlans, W, B);
	}
	
	
	/**
	 * Classify the index into one of the three types: INDEX_TYPE_CREATE, INDEX_TYPE_DROP, INDEX_TYPE_REMAIN
	 * 
	 * @param Sinit
	 *     The initial configuration
	 * @param Smat
	 *     The materialized configuration
	 * @param listPreparators
	 *     The schemas relevant to statements in the workload    
	 * 
	 */
    private void insertIndexesToPool(List<Index> Sinit, List<Index> Smat)
	{
		List<Index> Sin = new ArrayList<Index>();
		List<Index> Sout = new ArrayList<Index>();
		List<Index> Sremain = new ArrayList<Index>();
		
		// create a hash map to speed up the performance
		Map<Index, Integer> mapIndexInit = new HashMap<Index, Integer>();
		Map<Index, Integer> mapIndexMat = new HashMap<Index, Integer>();
		for (Index index : Sinit){
			mapIndexInit.put(index, 1);
		}
		
		for (Index index : Smat){ 
			mapIndexMat.put(index, 1);
		}		
		
		// 1. Computer Sremain and Sout
		for (Index idxInit : Sinit) {	
			if (mapIndexMat.containsKey(idxInit) == true) {
				Sremain.add(idxInit);
			}
			else {
				Sout.add(idxInit);
			}
		}
		
		for (Index idxMat : Smat) {
			if (mapIndexInit.containsKey(idxMat) == false) {
				Sin.add(idxMat); 
			}
		}
		
		// 2. Store into@MatIndexPool
		// in the order of @Sin, @Sout, and @Sremain
		poolIndexes.setStartPosCreateIndex(poolIndexes.getTotalIndex());
		for (Index idx : Sin) {
		    poolIndexes.addIndex(idx);
		}
		poolIndexes.setStartPosDropIndex(poolIndexes.getTotalIndex());
        for (Index idx : Sout) {
            poolIndexes.addIndex(idx);
        }
        poolIndexes.setStartPosRemainIndex(poolIndexes.getTotalIndex());
        for (Index idx : Sremain) {
            poolIndexes.addIndex(idx);
        }
	}
	
	/**
	 * Find an optimal materialization schedule plan
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
	public MaterializationSchedule findIndexSchedule(List<QueryPlanDesc> listQueryPlans, int W, double B)  
	{	
		LogListener listener = new LogListener() {
            public void onLogEvent(String component, String logEvent) {
                //To change body of implemented methods use File | Settings | File Templates.
            }
        };
       
        String cplexFile = "", binFile = "", consFile = "", objFile = "";	
        String workloadName = environment.getTempDir() + "/testwl";
        
        try {														
			genSim = new SimLinGenerator(workloadName, poolIndexes, listQueryPlans, W, B);
			
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
        
		
	    //Load the corresponding CPLEX problem from the corresponding text file
        try {		        
            cplex = new IloCplex(); 
                      
            // Read model from file with name @cplexFile into cplex optimizer object
            cplex.importModel(cplexFile); 
            
            // Solve the model and record the solution into @listIndex 
            // if one was found
            if (cplex.solve()) {	
                System.out.println("The objective function value: " + cplex.getObjValue());
               return getMaterializeSchedule();
            } 
            else {
            	System.out.println(" INFEASIBLE soltuion ");
            }
        }
        catch (IloException e) {
            System.err.println("Concert exception caught: " + e);
        }
        
        return null;
	}
	
	/**
	 * Return materialized index scheduling: which index is created/dropped at each window time
	 * 		Find out variable create_{w,i} = 1 and drop_{w,i} = 1
	 * 
	 * @return
	 * 		List of materialized indexes
	 */
	private MaterializationSchedule getMaterializeSchedule()
	{ 
	    MaterializationSchedule schedule = new MaterializationSchedule(this.W);
		
		// Iterate over variables create_{i,w} and drop_{i,w}
		try {
			matrix = getMatrix(cplex);
			vars = matrix.getNumVars();
			
			for (int i = 0; i < vars.length; i++) 
			{
				IloNumVar var = vars[i];
				if (cplex.getValue(var) == 1) {
				    SimVariable simVar = this.genSim.getVariable(var.getName());
				    if (simVar.getType() == SimVariablePool.VAR_CREATE 
				        || simVar.getType() == SimVariablePool.VAR_DROP ) {
				        schedule.addIndexWindow(genSim.getIndexOfVarCreateDrop(var.getName()), 
				                                simVar.getWindow(), simVar.getType());
				    }
				}
			}
		}
		catch (IloException e) {
			System.err.println("Concert exception caught: " + e);
	    }
		
		return schedule;
	}
	
	/**
	 * Determine the matrix used in the BIP problem
	 * 
	 * @param cplex
	 * 		The model of the BIP problem
	 * @return
	 * 	    The matrix of @cplex	  
	 */	
    public IloLPMatrix getMatrix(IloCplex cplex) throws IloException 
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
