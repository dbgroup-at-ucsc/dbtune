package edu.ucsc.dbtune.bip.sim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.ucsc.dbtune.bip.util.CPlexBuffer;
import edu.ucsc.dbtune.bip.util.StringConcatenator;
import edu.ucsc.dbtune.bip.util.IndexInSlot;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.bip.util.QueryPlanDesc;
import edu.ucsc.dbtune.bip.sim.ScheduleIndexPool;
import edu.ucsc.dbtune.metadata.Index;


public class SimLinGenerator 
{
    private CPlexBuffer buf;
	private List<String> listCwq;
	private List<QueryPlanDesc> listQueryPlanDescs;
	private int W;
	private double timeLimit;
	private int numConstraints;
	private SimVariablePool poolVariables;
    private ScheduleIndexPool poolIndexes;
    // Map variable of type CREATE or DROP to the indexes
    private Map<String,Index> mapVarCreateDropToIndex;
    
	
	SimLinGenerator(final String prefix, final ScheduleIndexPool poolIndexes, final List<QueryPlanDesc> listQueryPlanDecs, 
	                final int W, final double timeLimit)
	{		
	    this.poolIndexes = poolIndexes;
		this.listQueryPlanDescs = listQueryPlanDecs;
		this.W = W;
		this.timeLimit = timeLimit;
		listCwq = new ArrayList<String>();	
		this.poolVariables = new SimVariablePool();
		mapVarCreateDropToIndex = new HashMap<String,Index>();
		
		try {
			this.buf = new CPlexBuffer(prefix);
		}
		catch (IOException e) {
			System.out.println(" Error in opening files " + e.toString());			
		}
	}
	
	/**
	 * The function builds the BIP for the SIM problem, includes four sets of constraints:
	 * <p>
	 * <ol>
	 *  <li> Well-behaved schedule constraints </li>
     *  <li> Index present constraints </li>
	 * 	<li> Atomic constraints </li> 
	 *  <li> Window time constraints </li>
	 * </ol>
	 * </p>	  
	 * 
	 * @param listener
	 * 		Log the building process
	 * 
	 * @throws IOException
	 */
	public final void build(final LogListener listener) throws IOException 
	{
    	listener.onLogEvent(LogListener.BIP, "Building IIP program...");
    	numConstraints = 0;
    	
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
    	    	
        listener.onLogEvent(LogListener.BIP, "Built SIM program.");
    }
	
	/**
     * Add all variables into the pool of variables of this BIP formulation
     *  
     */
    private void constructVariables()
    {
        // for TYPE_CREATE index
        for (int idx = poolIndexes.getStartPosCreateIndex(); idx < poolIndexes.getStartPosDropIndex(); idx++) {
            for (int w = 0; w < W; w++) {
                SimVariable var = poolVariables.createAndStoreBIPVariable(SimVariablePool.VAR_CREATE, w, idx, 0, 0, 0);
                mapVarCreateDropToIndex.put(var.getName(), poolIndexes.indexes().get(idx));
            }          
        }
        
        // for TYPE_DROP index
        for (int idx = poolIndexes.getStartPosDropIndex(); idx < poolIndexes.getStartPosRemainIndex(); idx++) {
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
            int q = desc.getId();
            for (int w = 0; w < W; w++) {
                for (int k = 0; k < desc.getNumPlans(); k++) {
                    poolVariables.createAndStoreBIPVariable(SimVariablePool.VAR_Y, w, q, k, 0, 0);
                }    
                for (int k = 0; k < desc.getNumPlans(); k++) {              
                    for (int i = 0; i < desc.getNumSlots(); i++) {  
                        for (int a = 0; a < desc.getNumIndexesEachSlot(i); a++) {
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
     * 
     * </b> Note </b> Add variables of type CREATE and DROP into the list of variables
     * </p>
     *  
     */
    private void buildWellBehavedScheduleConstraints()
    {   
        // for TYPE_CREATE index
        for (int idx = poolIndexes.getStartPosCreateIndex(); idx < poolIndexes.getStartPosDropIndex(); idx++) {
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
        for (int idx = poolIndexes.getStartPosDropIndex(); idx < poolIndexes.getStartPosRemainIndex(); idx++) {
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
     * The present of an index I at a particular window w is defined as follows:
     * <p>
     * <ol> 
     *  <li> If I is a created index, then I is present at w if and only w i HAS been 
     * created at SOME window point between 0 and w </li>
     * 
     *  <li> If I is a dropped index, then I is present at w if and only w i has NOT been 
     * dropped at ALL window points between 0 and w </li>    
     * </ol>
     * </p>
     * 
     *  
     */
    private void buildIndexPresentConstraints()
    {   
        // for TYPE_CREATE index
        for (int idx = poolIndexes.getStartPosCreateIndex(); idx < poolIndexes.getStartPosDropIndex(); idx++) {
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
        for (int idx = poolIndexes.getStartPosDropIndex(); idx < poolIndexes.getStartPosRemainIndex(); idx++) {
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
	 * Cqw = \sum_{k \in [1, Kq]} \beta_{qk} y(w,q,k) + 
	 *      + \sum_{ k \in [1, Kq] \\ i \in [1, n] \\ a \in S_i \cup I_{\emptyset} x(w,q,k,i,a) \gamma_{q,k,i,a}     
	 *
	 */
	private void buildQueryCostWindow()
	{
	    for (QueryPlanDesc desc : listQueryPlanDescs){
		    int q = desc.getId();
			for (int w = 0; w < W; w++) {
			    List<String> linList = new ArrayList<String>();
				for (int k = 0; k < desc.getNumPlans(); k++) {
					String var = poolVariables.getSimVariable(SimVariablePool.VAR_Y, w, q, k, 0, 0).getName();
					linList.add(Double.toString(desc.getInternalPlanCost(k)) + var);		
				}
				String Cwq  = StringConcatenator.concatenate(" + ", linList);			
						
				// Index access cost
				linList.clear();			
				for (int k = 0; k < desc.getNumPlans(); k++) {				
					for (int i = 0; i < desc.getNumSlots(); i++) {	
						for (int a = 0; a < desc.getNumIndexesEachSlot(i); a++) {
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
	 * Standard set of atomic constraint of INUM
	 * 
	 * 
	 */
	private void buildAtomicConstraints()
	{	
		for (QueryPlanDesc desc : listQueryPlanDescs){
            int q = desc.getId();
		
            for (int w = 0; w < W; w++) {
                List<String> linList = new ArrayList<String>();
				// (1) \sum_{k \in [1, Kq]}y^{theta}_k = 1
				for (int k = 0; k < desc.getNumPlans(); k++) {
					linList.add(poolVariables.getSimVariable(SimVariablePool.VAR_Y, w, q, k, 0, 0).getName());
				}
				buf.getCons().println("atomic_13a_" + numConstraints + ": " + StringConcatenator.concatenate(" + ", linList) + " = 1");
				numConstraints++;
			
				// (2) \sum_{a \in S_i} x(theta, k, i, a) = y(theta, k)
				for (int k = 0; k < desc.getNumPlans(); k++) {
					String var_y = poolVariables.getSimVariable(SimVariablePool.VAR_Y, w, q, k, 0, 0).getName();
					for (int i = 0; i < desc.getNumSlots(); i++) {
					    if (desc.isReferenced(i) == false) {
					        continue;
					    }
						linList.clear();
						for (int a = 0; a < desc.getNumIndexesEachSlot(i); a++) {
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
        for (int idx = poolIndexes.getStartPosCreateIndex(); idx < poolIndexes.getStartPosDropIndex(); idx++) {
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
	}
	
	
	
	/**
	 * Impose space constraint on the materialized indexes at all window times
	 * 
	 */
	private void buildTimeConstraints()
	{
		for (int w = 0; w < W; w++) {
		    List<String> linList = new ArrayList<String>();
			for (int idx = poolIndexes.getStartPosCreateIndex(); idx < poolIndexes.getStartPosDropIndex(); idx++) {
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
	 * Constraints all variables to be binary ones
	 * 
	 */
	private void binaryVariableConstraints()
	{
	    int NUM_VAR_PER_LINE = 10;
        String strListVars = poolVariables.enumerateListVariables(NUM_VAR_PER_LINE);
        buf.getBin().println(strListVars);	
	}
	
	public SimVariable getVariable(String name)
	{
	    return (SimVariable) this.poolVariables.getVariable(name);
	}
	
	public Index getIndexOfVarCreateDrop(String name)
	{
	    return this.mapVarCreateDropToIndex.get(name);
	}
}
