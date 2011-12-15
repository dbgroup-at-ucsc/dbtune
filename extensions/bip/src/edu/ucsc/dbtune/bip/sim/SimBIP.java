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

import edu.ucsc.dbtune.bip.util.BIPAgent;
import edu.ucsc.dbtune.bip.util.CPlexBuffer;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.inum.InumSpace;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Environment;

public class SimBIP 
{	
	private IloCplex cplex;	
	private Environment environment = Environment.getInstance();
	private SimLinGenerator genSim;
	private IloLPMatrix matrix;
	private IloNumVar [] vars;
	
	/**	
	 * Scheduling materialization index
	 * 
	 * @param Sinit
	 * 		The initial configuration
	 * @param Smat
	 * 		The configuration to be materialized	
	 * @param listInumSpace
	 * 		Each Inum space corresponds to one query in the workload
	 * @param W
	 * 		The number of window time
	 * @param B
	 * 		The maximum space to be constrained all the materialized index at each the window time
	 * 
	 * 
	 * @return
	 * 		A sequence of index to be materialized (created/dropped) in the corresponding window time
	 * @throws SQLException 
	 */
	public List<MatIndex> schedule(List<Index> Sinit, List<Index> Smat, BIPAgent agent, int W, 
								   List<Double> B) throws SQLException
	{
		List<Index> candidateIndexes = new ArrayList<Index>();		
		List<InumSpace> listInumSpace = agent.populateInumSpace();
		
		// 1. Derive Sin, Sout, Sremain
		classifyTypeIndex(Sinit, Smat);
		
		// 2.Derive candidate indexes
		for (int idx = 0; idx < MatIndexPool.getTotalIndex(); idx++) {
			Index index = MatIndexPool.getMatIndex(idx).getIndex();
			candidateIndexes.add(index);
		}
		
		// 3. Derive the query plan description including internal cost, index access cost,
		// index at each slot ... 
		List<SimQueryPlanDesc> listQueryPlan = new ArrayList<SimQueryPlanDesc>();
		for (InumSpace inum : listInumSpace){
			SimQueryPlanDesc desc = new SimQueryPlanDesc(); 
			desc.generateQueryPlanDesc(inum, candidateIndexes);
			// Update @MatIndexPool and materialized size indexes
			desc.simGenerateQueryPlanDesc(inum, candidateIndexes);
			listQueryPlan.add(desc);
		}
		
		// 5. Formulate BIP and run the BIP to derive the final result 
		// which is a list of materialized indexes		
		return findIndexSchedule(listQueryPlan, W, B);
	}
	
	
	/**
	 * Classify the index into one of the three types: INDEX_TYPE_CREATE, INDEX_TYPE_DROP, INDEX_TYPE_REMAIN
	 * 
	 * @param Sinit
	 * 		The initial configuration
	 * @param Smat
	 * 		The materialized configuration
	 * 
	 */
	private void classifyTypeIndex(List<Index> Sinit, List<Index> Smat)
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
		int globalId = 0;
		MatIndexPool.setStartPosCreateIndexType(globalId);
		for (Index idx : Sin) {
			MatIndex matIndex = new MatIndex(idx, globalId, MatIndex.INDEX_TYPE_CREATE);
			MatIndexPool.addMatIndex(matIndex);			
			MatIndexPool.mapIndexGlobalId(idx, globalId);			
			globalId++; 
		}
		
		MatIndexPool.setStartPosDropIndexType(globalId);
		for (Index idx : Sout) {
			MatIndex matIndex = new MatIndex(idx, globalId, MatIndex.INDEX_TYPE_DROP);
			MatIndexPool.addMatIndex(matIndex);
			MatIndexPool.mapIndexGlobalId(idx, globalId);			
			globalId++;
		}
		
		MatIndexPool.setStartPosRemainIndexType(globalId);
		for (Index idx: Sremain) {
			MatIndex matIndex = new MatIndex(idx, globalId, MatIndex.INDEX_TYPE_REMAIN);
			MatIndexPool.addMatIndex(matIndex);
			MatIndexPool.mapIndexGlobalId(idx, globalId);			
			globalId++;
		}
		MatIndexPool.setTotalIndex(globalId);
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
	public List<MatIndex> findIndexSchedule(List<SimQueryPlanDesc> listQueryPlan, int W, List<Double> B)  
	{	
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
        
		
	    //Load the corresponding CPLEX problem from the corresponding text file
        try {		        
            cplex = new IloCplex(); 
                      
            // Read model from file with name @cplexFile into cplex optimizer object
            cplex.importModel(cplexFile); 
            
            
            // Solve the model and record the solution into @listIndex 
            // if one was found
            if (cplex.solve()) {				               
               listIndex = getMaterializeSchedule();
               System.out.println(" In CPlex, objective function value: " + cplex.getObjValue());
            } 
            else {
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
				if (cplex.getValue(var) == 1) {
					MatIndex index = SimLinGenerator.deriveMatIndex(var.getName());
					if (index != null) {
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
	
	/**
	 * Print the detail of the schedule
	 * 
	 * @param listIndex
	 * 		The list of materialized index
	 * @param W
	 * 		Number of window times
	 * 
	 * @return
	 * 		The string contains a sequence of command to implement the scheduling index materialization
	 * 
	 */
	public String printSchedule(List<MatIndex> listIndex, int W)
	{
		String strSchedule = "";
		for (int w = 0; w < W; w++) {
			strSchedule += "=== At window " + w + "-th:============\n";
			for (MatIndex idx : listIndex) {
				if (idx.getMatWindow() == w) {
					if (idx.getTypeMatIndex() == MatIndex.INDEX_TYPE_CREATE) {
						strSchedule += " CREATE: ";
					}
					else if (idx.getTypeMatIndex() == MatIndex.INDEX_TYPE_DROP)
					{
						strSchedule += " DROP ";
					}
					strSchedule += idx.getIndex().getName();
					strSchedule += "\n";
				}
			}
		}
		
		return strSchedule;
	}
}
