package edu.ucsc.dbtune.bip.sim;

import ilog.concert.IloException;
import ilog.concert.IloNumVar;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.ucsc.dbtune.bip.util.CPlexBuffer;
import edu.ucsc.dbtune.bip.util.StringConcatenator;
import edu.ucsc.dbtune.bip.util.IndexInSlot;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.bip.core.AbstractBIPSolver;
import edu.ucsc.dbtune.bip.core.BIPIndexPool;
import edu.ucsc.dbtune.bip.core.BIPOutput;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.bip.sim.SchedulePoolLocator;
import edu.ucsc.dbtune.metadata.Index;


/**
 * The class serves as the main entry to solve the scheduling index problem using
 * Binary Integer Program framework.
 * 
 * @author tqtrung@soe.ucsc.edu
 *
 */
public class SimBIP extends AbstractBIPSolver 
{   
	private List<String> listCwq;
	private int W;
	private double timeLimit;
	private List<Index> Sinit, Smat;
	private SimVariablePool poolVariables;
	private SchedulePoolLocator poolLocator;
    // Map variable of type CREATE or DROP to the indexes
    private Map<String,Index> mapVarCreateDropToIndex;
    
    /**
     * The constructor of the object to find the optimal index materialization schedule
     * 
     * @param Sinit
     *      The set of indexes that are currently materialized in the system
     * @param Smat
     *      The set of indexes that are going to be materialized
     * @param listPreparators
     * @param W
     *      The number of maintenance windows
     * @param timeLimit
     *      The time budget at each maintenance window
     */
	public SimBIP(List<Index> Sinit, List<Index> Smat,	             
	              final int W, final double timeLimit)
	{	
		this.W = W;
		this.timeLimit = timeLimit;
		this.Sinit = Sinit;
		this.Smat = Smat;
	}
	
	
    /**
     * Classify the index into one of the three types: 
     * INDEX_TYPE_CREATE, INDEX_TYPE_DROP, INDEX_TYPE_REMAIN
     * and store into the pool in the order of:
     * indexes of type CREATE, and then DROP, and finally REMAIN    
     * 
     */
    @Override
    protected void insertIndexesToPool()
    {   
        poolLocator = new SchedulePoolLocator();
        poolIndexes = new BIPIndexPool();
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
        poolLocator.setStartPosCreateIndex(poolIndexes.getTotalIndex());
        for (Index idx : Sin) {
            poolIndexes.addIndex(idx);
        }
        poolLocator.setStartPosDropIndex(poolIndexes.getTotalIndex());
        for (Index idx : Sout) {
            poolIndexes.addIndex(idx);
        }
        poolLocator.setStartPosRemainIndex(poolIndexes.getTotalIndex());
        for (Index idx : Sremain) {
            poolIndexes.addIndex(idx);
        }
    }
    
    @Override
    protected final void buildBIP(final LogListener listener) throws IOException 
    {
        listener.onLogEvent(LogListener.BIP, "Building BIP for SIM...");
        super.numConstraints = 0;
        
        // 1. Add variables into {@code poolVariables}
        constructVariables();
        
        // 2. Each index is created/dropped one time
        buildWellBehavedScheduleConstraints();
        
        // 3. Index present
        buildIndexPresentConstraints();
        
        // 4. Construct the query cost at each window time
        buildQueryCostWindow();
        
        // 5. Atomic constraints
        buildAtomicConstraints();       
        
        // 6. Space constraints
        buildTimeConstraints();
        
        // 7. Optimal constraint
        buildObjectiveFunction();
        
        // 8. binary variables
        binaryVariableConstraints();
        
        buf.close();
        
        CPlexBuffer.concat(this.buf.getLpFileName(), buf.getObjFileName(), buf.getConsFileName(), buf.getBinFileName());        
        listener.onLogEvent(LogListener.BIP, "Built SIM program.");
    }
    
    
    @Override
    protected BIPOutput getOutput()
    { 
        MaterializationSchedule schedule = new MaterializationSchedule(this.W);
        
        // Iterate over variables create_{i,w} and drop_{i,w}
        try {
            matrix = getMatrix(cplex);
            vars = matrix.getNumVars();
            
            for (int i = 0; i < vars.length; i++) {
                IloNumVar var = vars[i];
                if (cplex.getValue(var) == 1) {
                    SimVariable simVar = (SimVariable) poolVariables.getVariable(var.getName());
                    if (simVar.getType() == SimVariablePool.VAR_CREATE 
                        || simVar.getType() == SimVariablePool.VAR_DROP ) {
                        Index index = this.mapVarCreateDropToIndex.get(var.getName());
                        schedule.addIndexWindow(index, simVar.getWindow(), simVar.getType());
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
     * Construct and add all variables used by the BIP 
     * into the pool of variables
     *  
     */
    private void constructVariables()
    {  
        this.poolVariables = new SimVariablePool();
        mapVarCreateDropToIndex = new HashMap<String,Index>();
        // for TYPE_CREATE index
        for (int idx = poolLocator.getStartPosCreateIndex(); idx < poolLocator.getStartPosDropIndex(); idx++) {
            for (int w = 0; w < W; w++) {
                SimVariable var = poolVariables.createAndStoreBIPVariable(SimVariablePool.VAR_CREATE, w, idx, 0, 0, 0);
                mapVarCreateDropToIndex.put(var.getName(), poolIndexes.indexes().get(idx));
            }          
        }
        
        // for TYPE_DROP index
        for (int idx = poolLocator.getStartPosDropIndex(); idx < poolLocator.getStartPosRemainIndex(); idx++) {
            for (int w = 0; w < W; w++) {
                SimVariable var = poolVariables.createAndStoreBIPVariable(SimVariablePool.VAR_DROP, w, idx, 0, 0, 0);
                mapVarCreateDropToIndex.put(var.getName(), poolIndexes.indexes().get(idx));
            }          
        }
        
        // for TYPE_PRESENT
        for (int idx = 0; idx < poolIndexes.getTotalIndex(); idx++) {
            for (int w = 0; w < W; w++) {
                poolVariables.createAndStoreBIPVariable(SimVariablePool.VAR_PRESENT, w, idx, 0, 0, 0);
            }          
        }
        
        // for TYPE_Y, TYPE_X
        for (QueryPlanDesc desc : listQueryPlanDescs){
            int q = desc.getStatementID();
            for (int w = 0; w < W; w++) {               
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
                    poolVariables.createAndStoreBIPVariable(SimVariablePool.VAR_Y, w, q, k, 0, 0);
                }    
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {              
                    for (int i = 0; i < desc.getNumberOfSlots(); i++) {  
                        for (int a = 0; a < desc.getNumberOfIndexesEachSlot(i); a++) {
                            poolVariables.createAndStoreBIPVariable(SimVariablePool.VAR_X, w, q, k, i, a);
                        }
                    }
                }       
            }
        }
        
        // for TYPE_S
        for (int ga = 0; ga < poolIndexes.getTotalIndex(); ga++) {
            for (int w = 0; w < W; w++) {
                poolVariables.createAndStoreBIPVariable(SimVariablePool.VAR_S, w, ga, 0, 0, 0);
            }
        }
    }
      
    
	/**
     * A well-behaved schedule satisfies the following three conditions:
     * <p>
     * <ol> 
     *  <li> Indexes in Sremain remain in the DBMS </li>
     *  <li> Indexes in Sin are created one time </li>
     *  <li> Indexes in Sout are dropped one time </li>
     * </ol>
     * </p>
     *  
     */
    private void buildWellBehavedScheduleConstraints()
    {   
        // for TYPE_CREATE index
        for (int idx = poolLocator.getStartPosCreateIndex(); idx < poolLocator.getStartPosDropIndex(); idx++) {
            List<String> linList = new ArrayList<String>();
            for (int w = 0; w < W; w++) {
                String var = poolVariables.getSimVariable(SimVariablePool.VAR_CREATE, w, idx, 0, 0, 0).getName();
                linList.add(var);
            }
           
            buf.getCons().println("well_behaved_11a_" + numConstraints  
                    + ": " + StringConcatenator.concatenate(" + ", linList)                     
                    + " = 1");
            numConstraints++;           
        }
        
        // for TYPE_DROP index
        for (int idx = poolLocator.getStartPosDropIndex(); idx < poolLocator.getStartPosRemainIndex(); idx++) {
            List<String> linList = new ArrayList<String>();
            for (int w = 0; w < W; w++) {
                String var = poolVariables.getSimVariable(SimVariablePool.VAR_DROP, w, idx, 0, 0, 0).getName();
                linList.add(var);
            }
            buf.getCons().println("well_behaved_11b_" + numConstraints  
                    + ": " + StringConcatenator.concatenate(" + ", linList)                     
                    + " = 1");
            numConstraints++;           
        }
        // for TYPE_REMAIN: we do not create variables of this types
    }
    
    /**
     * The present of an index I at a particular window w is determined as follows:
     * <p>
     * <ol> 
     *  <li> If I is a created index, then I is present at w if and only I has been 
     * created at some window point between 0 and w </li>
     * 
     *  <li> If I is a dropped index, then I is present at w if and only I has not been 
     * dropped at all windows between 0 and w </li>    
     * </ol>
     * </p>
     *  
     */
    private void buildIndexPresentConstraints()
    {   
        // for TYPE_CREATE index
        for (int idx = poolLocator.getStartPosCreateIndex(); idx < poolLocator.getStartPosDropIndex(); idx++) {
            for (int w = 0; w < W; w++) {
                String var_present = poolVariables.getSimVariable(SimVariablePool.VAR_PRESENT, w, idx, 0, 0, 0).getName();
                List<String> linList = new ArrayList<String>();
                for (int j = 0; j <= w; j++) {
                    String var_create = poolVariables.getSimVariable(SimVariablePool.VAR_CREATE, j, idx, 0, 0, 0).getName();
                    linList.add(var_create);
                }
                
                buf.getCons().println("index_present_12a_" + numConstraints  
                        + ": " + StringConcatenator.concatenate(" + ", linList)                     
                        + " - " + var_present + " = 0 ");
                numConstraints++;   
            }       
        }
        
        // for TYPE_DROP index
        for (int idx = poolLocator.getStartPosDropIndex(); idx < poolLocator.getStartPosRemainIndex(); idx++) {
            for (int w = 0; w < W; w++) {
                String var_present = poolVariables.getSimVariable(SimVariablePool.VAR_PRESENT, w, idx, 0, 0, 0).getName();
                List<String> linList = new ArrayList<String>();
                for (int j = 0; j <= w; j++) {
                    String var_create = poolVariables.getSimVariable(SimVariablePool.VAR_DROP, j, idx, 0, 0, 0).getName();
                    linList.add(var_create);
                }
                buf.getCons().println("index_present_12b_" + numConstraints  
                        + ": " + StringConcatenator.concatenate(" + ", linList)                     
                        + " + " + var_present + " = 1 ");
                numConstraints++;                   
            }       
        }
        
        // present_{a,w} = 1 for $a \in Sremain$, which we do not enforce here
    }
    
	/**
	 * Build cost function of each query in each window w
	 * 
	 * Cqw = \sum_{k \in [1, Kq]} \beta_{qk} y(w,q,k) + 
	 *      + \sum_{ k \in [1, Kq] \\ i \in [1, n] \\ a \in S_i \cup I_{\emptyset} x(w,q,k,i,a) \gamma_{q,k,i,a}     
	 *
	 */
	private void buildQueryCostWindow()
	{
	    listCwq = new ArrayList<String>(); 
        
	    for (QueryPlanDesc desc : listQueryPlanDescs){
		    int q = desc.getStatementID();
			for (int w = 0; w < W; w++) {
			    List<String> linList = new ArrayList<String>();
				for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
					String var = poolVariables.getSimVariable(SimVariablePool.VAR_Y, w, q, k, 0, 0).getName();
					linList.add(Double.toString(desc.getInternalPlanCost(k)) + var);		
				}
				String Cwq  = StringConcatenator.concatenate(" + ", linList);			
						
				// Index access cost
				linList.clear();			
				for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {				
					for (int i = 0; i < desc.getNumberOfSlots(); i++) {	
						for (int a = 0; a < desc.getNumberOfIndexesEachSlot(i); a++) {
							String var = poolVariables.getSimVariable(SimVariablePool.VAR_X, w, q, k, i, a).getName();
							linList.add(Double.toString(desc.getIndexAccessCost(k, i, a)) + var);		
						}
					}
				}		
				Cwq = Cwq + " + " + StringConcatenator.concatenate(" + ", linList);
				listCwq.add(Cwq);
			}
		}
	}
	
	/**
	 * The set of atomic constraints
	 * 
	 */
	private void buildAtomicConstraints()
	{	
		for (QueryPlanDesc desc : listQueryPlanDescs){
            int q = desc.getStatementID();
		
            for (int w = 0; w < W; w++) {
                List<String> linList = new ArrayList<String>();
				// (1) \sum_{k \in [1, Kq]}y^{theta}_k = 1
				for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
					linList.add(poolVariables.getSimVariable(SimVariablePool.VAR_Y, w, q, k, 0, 0).getName());
				}
				buf.getCons().println("atomic_13a_" + numConstraints + ": " + StringConcatenator.concatenate(" + ", linList) + " = 1");
				numConstraints++;
			
				// (2) \sum_{a \in S_i} x(theta, k, i, a) = y(theta, k)
				for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
					String var_y = poolVariables.getSimVariable(SimVariablePool.VAR_Y, w, q, k, 0, 0).getName();
					for (int i = 0; i < desc.getNumberOfSlots(); i++) {
					    if (desc.isSlotReferenced(i) == false) {
					        continue;
					    }
						linList.clear();
						for (int a = 0; a < desc.getNumberOfIndexesEachSlot(i); a++) {
							String var_x = poolVariables.getSimVariable(SimVariablePool.VAR_X, w, q, k, i, a).getName();
							linList.add(var_x);
							IndexInSlot iis = new IndexInSlot(q,i,a);
							int ga = poolIndexes.getPoolID(iis);
							String var_s = poolVariables.getSimVariable(SimVariablePool.VAR_S, w, ga, 0, 0, 0).getName();
							
							// (3) s_a^{theta} \geq x_{kia}^{theta}
							buf.getCons().println("atomic_14a_" + numConstraints + ":" 
												+ var_x + " - " 
												+ var_s
												+ " <= 0 ");
							numConstraints++;
						}
						buf.getCons().println("atomic_13b_" + numConstraints  
											+ ": " + StringConcatenator.concatenate(" + ", linList) 
											+ " - " + var_y
											+ " = 0");
						numConstraints++;
					}
				}		
			}
		}
		
		// s(w,ai) <= present(w,i)
        for (int idx = poolLocator.getStartPosCreateIndex(); idx < poolLocator.getStartPosDropIndex(); idx++) {
            for (int w = 0; w < W; w++) {
                String var_present = poolVariables.getSimVariable(SimVariablePool.VAR_PRESENT, w, idx, 0, 0, 0).getName();
                String var_s = poolVariables.getSimVariable(SimVariablePool.VAR_S, w, idx, 0, 0, 0).getName();
                buf.getCons().println("atomic_14b_" + numConstraints  
                                        + ": " + var_s + " - " + var_present + " <= 0 ");
                numConstraints++;   
            }
        }
        // s(w,a) <= present(w,a) for a \in Sremain is obvious
        // since present(w,a) = 1. Thus, we don't not impose these constraints
        // TODO: if we remove s(w,a), remember the special case of REMAINING indexes
	}
	
	/**
	 * Time constraint on the materialized indexes at all maintenance window
	 * 
	 */
	private void buildTimeConstraints()
	{
		for (int w = 0; w < W; w++) {
		    List<String> linList = new ArrayList<String>();
			for (int idx = poolLocator.getStartPosCreateIndex(); idx < poolLocator.getStartPosDropIndex(); idx++) {
				String var_create = poolVariables.getSimVariable(SimVariablePool.VAR_CREATE, w, idx, 0, 0, 0).getName();
				Index index = poolIndexes.indexes().get(idx);
				linList.add(Double.toString(index.getCreationCost()) + var_create);
			}
			buf.getCons().println("time_constraint" + numConstraints  
					+ " : " + StringConcatenator.concatenate(" + ", linList) 					
					+ " <= " + timeLimit);
			numConstraints++;				
		}
	}
	
	/**
	 * The accumulated total cost function
	 */
	private void buildObjectiveFunction()
	{
		buf.getObj().println(StringConcatenator.concatenate(" + ", listCwq));
	}
	
	/**
	 * Constraint all variables to be binary ones
	 * 
	 */
	private void binaryVariableConstraints()
	{
	    int NUM_VAR_PER_LINE = 10;
        String strListVars = poolVariables.enumerateListVariables(NUM_VAR_PER_LINE);
        buf.getBin().println(strListVars);	
	}
}
