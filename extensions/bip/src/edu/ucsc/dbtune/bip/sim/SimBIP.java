package edu.ucsc.dbtune.bip.sim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import edu.ucsc.dbtune.bip.util.CPlexBuffer;
import edu.ucsc.dbtune.bip.core.AbstractBIPSolver;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Strings;


/**
 * The class serves as the main entry to solve the scheduling index problem using
 * Binary Integer Program framework.
 * 
 * @author Quoc Trung Tran
 *
 */
public class SimBIP extends AbstractBIPSolver implements ScheduleBIPSolver
{   
    /** Variables used in constructing BIP */
	private List<String> listCwq;
	private SimVariablePool poolVariables;
    /** Map variable of type CREATE or DROP to the indexes */
	private Map<String,Index> mapVarCreateDropToIndex;
	
	/** Variables for the constraints on windows*/
	protected int W;	
    protected int maxNumberIndexesWindow;
    protected double maxCreationCostWindow;
    protected boolean isConstraintNumberIndexesWindow;
    protected boolean isConstraintCreationCostWindow;
    
    /** Set of indexes that are created, dropped, and not touched */
    protected Set<Index> Sinit;
    protected Set<Index> Screate;
    protected Set<Index> Sdrop;
    protected Set<Index> Sremain;
    
    
    @Override
    public void setConfigurations(Set<Index> Sinit, Set<Index> Smat) 
    {
        this.Sinit = Sinit;
        /**
         * Classify the indexes into one of the three types: 
         * INDEX_TYPE_CREATE, INDEX_TYPE_DROP, INDEX_TYPE_REMAIN
         */
        candidateIndexes = new HashSet<Index> (Smat);
        candidateIndexes.addAll(Sinit);
        
        // Screate = Smat - Sinit
        Screate = new HashSet<Index>(Smat);
        Screate.removeAll(Sinit);
        // Sdrop = Sinit - Smat
        Sdrop = new HashSet<Index>(Sinit);
        Sdrop.removeAll(Smat);
        // Sremain = Sin \cap Smat
        Sremain = new HashSet<Index>(Sinit); 
        Sremain.retainAll(Smat);
    }
    

    @Override
    public void setNumberWindows(int W) 
    {
        this.W  = W;
    }

    @Override
    public void setNumberofIndexesEachWindow(int n) 
    {
        maxNumberIndexesWindow = n; 
        isConstraintNumberIndexesWindow = true;
    }

    @Override
    public void setCreationCostWindow(int C) 
    {
        this.maxCreationCostWindow = C;
        isConstraintCreationCostWindow = true;
    }
    
    
	public SimBIP()
	{	
	    isConstraintNumberIndexesWindow = false;
	    isConstraintCreationCostWindow = false;	
	}
	
	/**
	 * Since the set of indexes are derived from the initial configuration ({@code Sinit}) and
	 * the configuration to be materialized ({@code Smat}), this method is left to be empty. 
	 */
	@Override
	public void setCandidateIndexes(Set<Index> candidateIndexes)
	{   
	}
    
	@Override
    protected final void buildBIP() 
    {   
        super.numConstraints = 0;
        
        // 1. Add variables into {@code poolVariables}
        constructVariables();
        
        // 2. Index present
        indexPresentConstraints();
        
        // 3. Construct the query cost at each window time
        queryCostWindow();
        
        // 4. Atomic constraints
        atomicConstraints();        
        usedIndexAtWindow();
        
        // 5. Space constraints
        if (isConstraintCreationCostWindow)
            timeConstraints();
        
        if (isConstraintNumberIndexesWindow)
            numberIndexesAtEachWindowConstraint();
        
        // 6. Optimal constraint
        objectiveFunction();
        
        // 7. binary variables
        binaryVariableConstraints();
        
        buf.close();
        
        try {
            CPlexBuffer.concat(buf.getLpFileName(), buf.getObjFileName(), 
                               buf.getConsFileName(), buf.getBinFileName());
        } catch (IOException e) {
            throw new RuntimeException("Cannot concantenate text files that store BIP.");
        }
    }
    
    
    @Override
    protected IndexTuningOutput getOutput()
    { 
        MaterializationSchedule schedule = new MaterializationSchedule(W, Sinit);
        
        // Iterate over variables create_{i,w} and drop_{i,w}
        for (Entry<String, Integer> pairVarVal : mapVariableValue.entrySet()) {
            
            if (pairVarVal.getValue() == 1) {
                SimVariable simVar = (SimVariable) poolVariables.get(pairVarVal.getKey());
                
                if (simVar.getType() == SimVariablePool.VAR_CREATE 
                        || simVar.getType() == SimVariablePool.VAR_DROP) {
                    Index index = mapVarCreateDropToIndex.get(pairVarVal.getKey());
                    schedule.addIndexWindow(index, simVar.getWindow(), simVar.getType());
                }
            }
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
        poolVariables = new SimVariablePool();
        mapVarCreateDropToIndex = new HashMap<String,Index>();
        // for TYPE_CREATE index 
        for (Index index : Screate) {
            
            for (int w = 0; w < W; w++) {
                SimVariable var = poolVariables.createAndStore(SimVariablePool.VAR_CREATE, w, 
                                                               0, 0, index.getId());
                mapVarCreateDropToIndex.put(var.getName(), index);
            }          
        }
        
        // for TYPE_DROP index
        for (Index index : Sdrop) {
            
            for (int w = 0; w < W; w++) {
                SimVariable var = poolVariables.createAndStore(SimVariablePool.VAR_DROP, w, 
                                                               0, 0, index.getId());
                mapVarCreateDropToIndex.put(var.getName(), index);
            }          
        }
        
        // for TYPE_PRESENT
        for (Index index : candidateIndexes) {            
            for (int w = 0; w < W; w++)
                poolVariables.createAndStore(SimVariablePool.VAR_PRESENT, w, 0, 0, index.getId());                      
        }
        
        // for TYPE_Y, TYPE_X
        for (QueryPlanDesc desc : listQueryPlanDescs){
            
            int q = desc.getStatementID();
            for (int w = 0; w < W; w++) {
                
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) 
                    poolVariables.createAndStore(SimVariablePool.VAR_Y, w, q, k, 0);
                    
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {    
                    
                    for (int i = 0; i < desc.getNumberOfSlots(); i++) {
                        
                        for (Index index : desc.getListIndexesAtSlot(i)) 
                            poolVariables.createAndStore(SimVariablePool.VAR_X, w, q, k, 
                                                         index.getId());
                        
                    }
                }       
            }
        }
    }
      
    
	/**
     * A well-behaved schedule satisfies the following three conditions:
     * <p>
     * <ol>
     *  <li> Index I in {@code Screate} is created one time. In addition, I is present at window w 
     *  if and only I has been created at some window point between 0 and w </li>
     *  <li> Index I in {@code Sdrop} is dropped one time. Furthermore, I is present at window w 
     *  if and only I has not been dropped at all windows between 0 and w </li>
     *  <li> Indexes in {@code Sremain} remain in the DBMS at all windows</li>
     * </ol>
     * </p>
     *  
     */
    private void indexPresentConstraints()
    {   
        // for TYPE_CREATE index
        for (Index index : Screate) {
            
            List<String> linList = new ArrayList<String>();
            
            for (int w = 0; w < W; w++) {
                
                String var = poolVariables.get(SimVariablePool.VAR_CREATE, w, 
                                               0, 0, index.getId()).getName();
                linList.add(var);
            }
            
            buf.getCons().println("well_behaved_11a_" + numConstraints  
                    + ": " + Strings.concatenate(" + ", linList)                     
                    + " = 1");
            numConstraints++;
            
            for (int w = 0; w < W; w++) {
                
                String var_present = poolVariables.get(SimVariablePool.VAR_PRESENT, w, 
                                                       0, 0, index.getId()).getName();
                linList = new ArrayList<String>();
                
                for (int j = 0; j <= w; j++) {
                    
                    String var_create = poolVariables.get(SimVariablePool.VAR_CREATE, j, 
                                                          0, 0, index.getId()).getName();
                    linList.add(var_create);
                }
                
                buf.getCons().println("index_present_12a_" + numConstraints  
                        + ": " + Strings.concatenate(" + ", linList)                     
                        + " - " + var_present + " = 0 ");
                numConstraints++;   
            }
        }
        
        // for TYPE_DROP index
        for (Index index : Sdrop) {
            
            List<String> linList = new ArrayList<String>();
            
            for (int w = 0; w < W; w++) {
                String var = poolVariables.get(SimVariablePool.VAR_DROP, w, 0, 
                                               0, index.getId()).getName();
                linList.add(var);
            }
            buf.getCons().println("well_behaved_11b_" + numConstraints  
                    + ": " + Strings.concatenate(" + ", linList)                     
                    + " = 1");
            numConstraints++;
            
            for (int w = 0; w < W; w++) {
                
                String var_present = poolVariables.get(SimVariablePool.VAR_PRESENT, w, 
                                                       0, 0, index.getId()).getName();
                linList = new ArrayList<String>();
                
                for (int j = 0; j <= w; j++) {
                    String var_drop = poolVariables.get(SimVariablePool.VAR_DROP, j, 
                                                        0, 0, index.getId()).getName();
                    linList.add(var_drop);
                }
                
                buf.getCons().println("index_present_12b_" + numConstraints  
                        + ": " + Strings.concatenate(" + ", linList)                     
                        + " + " + var_present + " = 1 ");
                numConstraints++;                   
            }
        }
        
        // for TYPE_PRESENT index
        for (Index index : Sremain) {
            
            for (int w = 0; w < W; w++) {
                
                String var = poolVariables.get(SimVariablePool.VAR_PRESENT, w, 
                                                0, 0, index.getId()).getName();
                buf.getCons().println("well_behaved_11b_" + numConstraints  
                        + ": " + var      
                        + " = 1");
                numConstraints++;
            }          
        }
    }
    
   	/**
	 * Build the execution cost of each query at each window
	 * 
	 * {@code cost(q, w) = \sum_{k \in [1, Kq]} \beta_{qk} y(w,q,k) }
	 * {@code            +  \sum_{ k \in [1, Kq] \\ i \in [1, n] \\ a \in S_i \cup I_{\emptyset} }
	 * {@code                x(w,q,k,i,a) \gamma_{q,k,i,a} }     
	 *
	 */
	private void queryCostWindow()
	{
	    listCwq = new ArrayList<String>(); 
        
	    for (QueryPlanDesc desc : listQueryPlanDescs){
	        
		    int q = desc.getStatementID();
			for (int w = 0; w < W; w++) {
			    
			    List<String> linList = new ArrayList<String>();
				for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
				    
					String var = poolVariables.get(SimVariablePool.VAR_Y, w, q, k, 0).getName();
					linList.add(Double.toString(desc.getInternalPlanCost(k)) + var);		
				}
				String Cwq  = Strings.concatenate(" + ", linList);			
						
				// Index access cost
				linList.clear();			
				for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {	
				    
					for (int i = 0; i < desc.getNumberOfSlots(); i++) {
					    
					    for (Index index : desc.getListIndexesAtSlot(i)) {
					        
							String var = poolVariables.get(SimVariablePool.VAR_X, w, 
							                               q, k, index.getId()).getName();
							linList.add(desc.getAccessCost(k, index) + var);		
						}
					}
				}		
				Cwq = Cwq + " + " + Strings.concatenate(" + ", linList);
				listCwq.add(Cwq);
			}
		}
	}
	
	/**
     * Build a set of atomic constraints on variables {@code VAR_X} and {@code VAR_Y}
     * that are common for methods using INUM.
     * 
     * For example, the summation of all variables of type {@code VAR_Y} of a same {@code theta}
     * must be {@code 1} in order for the optimal execution cost corresponds to only one 
     * template plan.  
     * 
     */
	private void atomicConstraints()
	{	
		for (QueryPlanDesc desc : listQueryPlanDescs){
            int q = desc.getStatementID();
		
            for (int w = 0; w < W; w++) {
                List<String> linList = new ArrayList<String>();
                
				// (1) \sum_{k \in [1, Kq]}y^{theta}_k = 1
				for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) 
					linList.add(poolVariables.get(SimVariablePool.VAR_Y, w, q, k, 0).getName());
				
				buf.getCons().println("atomic_13a_" + numConstraints + ": " + 
						Strings.concatenate(" + ", linList) + " = 1");
				numConstraints++;
			
				// (2) \sum_{a \in S_i} x(w, k, i, a) = y(w, k)
				for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
				    
					String var_y = poolVariables.get(SimVariablePool.VAR_Y, w, q, k, 0).getName();
					for (int i = 0; i < desc.getNumberOfSlots(); i++) {
					    
						linList.clear();
						for (Index index : desc.getListIndexesAtSlot(i)) {
						    
							String var_x = poolVariables.get(SimVariablePool.VAR_X, w, 
							                                 q, k, index.getId()).getName();
							linList.add(var_x);
						}
						
						buf.getCons().println("atomic_13b_" + numConstraints  
											+ ": " + Strings.concatenate(" + ", linList) 
											+ " - " + var_y
											+ " = 0");
						numConstraints++;
					}
				}		
			}
		}
	}
	
	/**
	 * An index {@code a} is used to compute {@code cost(q, w)} if and only if {@code present_a = 1}
	 */
	private  void usedIndexAtWindow()
	{
	    for (QueryPlanDesc desc : listQueryPlanDescs){
            int q = desc.getStatementID();
        
            for (int w = 0; w < W; w++) {
                
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
                    
                    for (int i = 0; i < desc.getNumberOfSlots(); i++) {
                        
                        for (Index index : desc.getListIndexesAtSlot(i)) {
                            
                            String var_x = poolVariables.get(SimVariablePool.VAR_X, w, 
                                                             q, k, index.getId()).getName();
                        
                            String var_present = poolVariables.get(SimVariablePool.VAR_PRESENT, 
                                                             w, 0, 0, index.getId()).getName();
                            
                            // (3) present_a^{w} \geq x_{qkia}^{w}
                            buf.getCons().println("atomic_14a_" + numConstraints + ":" 
                                                + var_x + " - " 
                                                + var_present
                                                + " <= 0 ");
                            numConstraints++;
                        }
                    }
                }       
            }
        }
	}
	/**
	 * Time constraint on the materialized indexes at all maintenance window
	 * 
	 */
	private void timeConstraints()
	{
		for (int w = 0; w < W; w++) {
		    
		    List<String> linList = new ArrayList<String>();
		    
			for (Index index : Screate){
				String var_create = poolVariables.get(SimVariablePool.VAR_CREATE, w, 
				                                      0, 0, index.getId()).getName();				
				linList.add(Double.toString(index.getCreationCost()) + var_create);
			}
			
			buf.getCons().println("creation_cost_constraint" + numConstraints  
					+ " : " + Strings.concatenate(" + ", linList) 					
					+ " <= " + maxCreationCostWindow);
			numConstraints++;				
		}
	}
	
	private void numberIndexesAtEachWindowConstraint()
	{
	    for (int w = 0; w < W; w++) {
            List<String> linList = new ArrayList<String>();
            for (Index index : Screate){
                String var_create = poolVariables.get(SimVariablePool.VAR_CREATE, w, 
                                                      0, 0, index.getId()).getName();                
                linList.add(var_create);
            }
            
            buf.getCons().println("number_index_constraint" + numConstraints
                    + " : " + Strings.concatenate(" + ", linList)
                    + " <= " + maxNumberIndexesWindow);
            numConstraints++;
        }
	}
	
	/**
	 * The accumulated total cost function
	 */
	private void objectiveFunction()
	{
		buf.getObj().println(Strings.concatenate(" + ", listCwq));
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
