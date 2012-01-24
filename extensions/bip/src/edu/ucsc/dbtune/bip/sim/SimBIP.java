package edu.ucsc.dbtune.bip.sim;

import ilog.concert.IloException;
import ilog.concert.IloNumVar;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.bip.util.CPlexBuffer;
import edu.ucsc.dbtune.bip.util.StringConcatenator;
import edu.ucsc.dbtune.bip.core.AbstractBIPSolver;
import edu.ucsc.dbtune.bip.core.BIPOutput;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
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
	private SimVariablePool poolVariables;
    // Map variable of type CREATE or DROP to the indexes
    private Map<String,Index> mapVarCreateDropToIndex;
    // Set of indexes that are created, dropped, and not touched
    private Set<Index> Screate, Sdrop, Sremain;
    
    /**
     * The constructor of the object to find the optimal index materialization schedule
     * 
     * @param Sinit
     *      The set of indexes that are currently materialized in the system
     * @param Smat
     *      The set of indexes that are going to be materialized
     * @param W
     *      The number of maintenance windows
     * @param timeLimit
     *      The time budget at each maintenance window
     */
	public SimBIP(Set<Index> Sinit, Set<Index> Smat, final int W, final double timeLimit)
	{	
		this.W = W;
		this.timeLimit = timeLimit;
	
		/**
	     * Classify the indexes into one of the three types: 
	     * INDEX_TYPE_CREATE, INDEX_TYPE_DROP, INDEX_TYPE_REMAIN
	    */
		candidateIndexes = Smat;
        candidateIndexes.addAll(Sinit);
        
        // Screate = Smat - Sinit
        Screate = Smat;
        Screate.removeAll(Sinit);
        // Sdrop = Sinit - Smat
        Sdrop = Sinit;
        Sdrop.removeAll(Smat);
        // Sremain = Sin \cap Smat
        Sremain = Sinit;
        Sremain.retainAll(Smat);
	}
	
    
	@Override
    protected final void buildBIP() 
    {   
        super.numConstraints = 0;
        
        // 1. Add variables into {@code poolVariables}
        constructVariables();
        
        // 2. Index present
        buildIndexPresentConstraints();
        
        // 3. Construct the query cost at each window time
        buildQueryCostWindow();
        
        // 4. Atomic constraints
        buildAtomicConstraints();       
        
        // 5. Space constraints
        buildTimeConstraints();
        
        // 6. Optimal constraint
        buildObjectiveFunction();
        
        // 7. binary variables
        binaryVariableConstraints();
        
        buf.close();
        
        try {
            CPlexBuffer.concat(this.buf.getLpFileName(), buf.getObjFileName(), buf.getConsFileName(), buf.getBinFileName());
        } catch (IOException e) {
            throw new RuntimeException("Cannot concantenate text files that store BIP.");
        }
    }
    
    
    @Override
    protected BIPOutput getOutput()
    { 
        MaterializationSchedule schedule = new MaterializationSchedule(this.W);
        
        // Iterate over variables create_{i,w} and drop_{i,w}
        try {
            System.out.println("L147 (SimBIP), objective value: " + cplex.getObjValue());
            matrix = getMatrix(cplex);
            vars = matrix.getNumVars();
            
            for (int i = 0; i < vars.length; i++) {
                IloNumVar var = vars[i];
                if (cplex.getValue(var) == 1) {
                    SimVariable simVar = (SimVariable) poolVariables.get(var.getName());
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
        for (Index index : this.Screate) {
            for (int w = 0; w < W; w++) {
                SimVariable var = poolVariables.createAndStore(SimVariablePool.VAR_CREATE, w, 0, 0, 0, index.getId());
                mapVarCreateDropToIndex.put(var.getName(), index);
            }          
        }
        
        // for TYPE_DROP index
        for (Index index : this.Sdrop) {
            for (int w = 0; w < W; w++) {
                SimVariable var = poolVariables.createAndStore(SimVariablePool.VAR_DROP, w, 0, 0, 0, index.getId());
                mapVarCreateDropToIndex.put(var.getName(), index);
            }          
        }
        
        // for TYPE_PRESENT
        for (Index index : candidateIndexes) {
            for (int w = 0; w < W; w++) {
                poolVariables.createAndStore(SimVariablePool.VAR_PRESENT, w, 0, 0, 0, index.getId());
            }          
        }
        
        // for TYPE_Y, TYPE_X
        for (QueryPlanDesc desc : listQueryPlanDescs){
            int q = desc.getStatementID();
            for (int w = 0; w < W; w++) {               
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
                    poolVariables.createAndStore(SimVariablePool.VAR_Y, w, q, k, 0, 0);
                }    
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {              
                    for (int i = 0; i < desc.getNumberOfSlots(); i++) {  
                        for (Index index : desc.getListIndexesAtSlot(i)) {
                            poolVariables.createAndStore(SimVariablePool.VAR_X, w, q, k, i, index.getId());
                        }
                    }
                }       
            }
        }
    }
      
    
	/**
     * A well-behaved schedule satisfies the following three conditions:
     * <p>
     * <ol>
     *  <li> Index I in Screate is created one time. In addition, I is present at window w if and only I has been 
     *  created at some window point between 0 and w </li>
     *  <li> Index I in Sdrop is dropped one time. Furthermore, I is present at w if and only I has not been 
     *  dropped at all windows between 0 and w </li>
     *  <li> Indexes in Sremain remain in the DBMS at all windows</li>
     * </ol>
     * </p>
     *  
     */
    private void buildIndexPresentConstraints()
    {   
        // for TYPE_CREATE index
        for (Index index : this.Screate) {
            List<String> linList = new ArrayList<String>();
            for (int w = 0; w < W; w++) {
                String var = poolVariables.get(SimVariablePool.VAR_CREATE, w, 0, 0, 0, index.getId()).getName();
                linList.add(var);
            }
            buf.getCons().println("well_behaved_11a_" + numConstraints  
                    + ": " + StringConcatenator.concatenate(" + ", linList)                     
                    + " = 1");
            numConstraints++;
            
            for (int w = 0; w < W; w++) {
                String var_present = poolVariables.get(SimVariablePool.VAR_PRESENT, w, 0, 0, 0, index.getId()).getName();
                linList = new ArrayList<String>();
                for (int j = 0; j <= w; j++) {
                    String var_create = poolVariables.get(SimVariablePool.VAR_CREATE, j, 0, 0, 0, index.getId()).getName();
                    linList.add(var_create);
                }
                
                buf.getCons().println("index_present_12a_" + numConstraints  
                        + ": " + StringConcatenator.concatenate(" + ", linList)                     
                        + " - " + var_present + " = 0 ");
                numConstraints++;   
            }
        }
        
        // for TYPE_DROP index
        for (Index index : this.Sdrop) {
            List<String> linList = new ArrayList<String>();
            for (int w = 0; w < W; w++) {
                String var = poolVariables.get(SimVariablePool.VAR_DROP, w, 0, 0, 0, index.getId()).getName();
                linList.add(var);
            }
            buf.getCons().println("well_behaved_11b_" + numConstraints  
                    + ": " + StringConcatenator.concatenate(" + ", linList)                     
                    + " = 1");
            numConstraints++;
            
            for (int w = 0; w < W; w++) {
                String var_present = poolVariables.get(SimVariablePool.VAR_PRESENT, w, 0, 0, 0, index.getId()).getName();
                linList = new ArrayList<String>();
                for (int j = 0; j <= w; j++) {
                    String var_drop = poolVariables.get(SimVariablePool.VAR_DROP, j, 0, 0, 0, index.getId()).getName();
                    linList.add(var_drop);
                }
                buf.getCons().println("index_present_12b_" + numConstraints  
                        + ": " + StringConcatenator.concatenate(" + ", linList)                     
                        + " + " + var_present + " = 1 ");
                numConstraints++;                   
            }
        }
        
        // for TYPE_PRESENT index
        for (Index index : this.Sremain) {
            for (int w = 0; w < W; w++) {
                String var = poolVariables.get(SimVariablePool.VAR_PRESENT, w, 0, 0, 0, index.getId()).getName();
                buf.getCons().println("well_behaved_11b_" + numConstraints  
                        + ": " + var      
                        + " = 1");
                numConstraints++;
            }          
        }
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
					String var = poolVariables.get(SimVariablePool.VAR_Y, w, q, k, 0, 0).getName();
					linList.add(Double.toString(desc.getInternalPlanCost(k)) + var);		
				}
				String Cwq  = StringConcatenator.concatenate(" + ", linList);			
						
				// Index access cost
				linList.clear();			
				for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {				
					for (int i = 0; i < desc.getNumberOfSlots(); i++) {	
					    for (Index index : desc.getListIndexesAtSlot(i)) {
							String var = poolVariables.get(SimVariablePool.VAR_X, w, q, k, i, index.getId()).getName();
							linList.add(Double.toString(desc.getAccessCost(k, index)) + var);		
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
					linList.add(poolVariables.get(SimVariablePool.VAR_Y, w, q, k, 0, 0).getName());
				}
				buf.getCons().println("atomic_13a_" + numConstraints + ": " + StringConcatenator.concatenate(" + ", linList) + " = 1");
				numConstraints++;
			
				// (2) \sum_{a \in S_i} x(w, k, i, a) = y(w, k)
				for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
					String var_y = poolVariables.get(SimVariablePool.VAR_Y, w, q, k, 0, 0).getName();
					for (int i = 0; i < desc.getNumberOfSlots(); i++) {
						linList.clear();
						for (Index index : desc.getListIndexesAtSlot(i)) {
							String var_x = poolVariables.get(SimVariablePool.VAR_X, w, q, k, i, index.getId()).getName();
							linList.add(var_x);
							String var_present = poolVariables.get(SimVariablePool.VAR_PRESENT, w, 0, 0, 0, index.getId()).getName();
							
							// (3) s_a^{w} \geq x_{kia}^{w}
							buf.getCons().println("atomic_14a_" + numConstraints + ":" 
												+ var_x + " - " 
												+ var_present
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
	}
	
	/**
	 * Time constraint on the materialized indexes at all maintenance window
	 * 
	 */
	private void buildTimeConstraints()
	{
		for (int w = 0; w < W; w++) {
		    List<String> linList = new ArrayList<String>();
			for (Index index : this.Screate){
				String var_create = poolVariables.get(SimVariablePool.VAR_CREATE, w, 0, 0, 0, index.getId()).getName();				
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
        String strListVars = poolVariables.enumerateList(NUM_VAR_PER_LINE);
        buf.getBin().println(strListVars);	
	}
}
