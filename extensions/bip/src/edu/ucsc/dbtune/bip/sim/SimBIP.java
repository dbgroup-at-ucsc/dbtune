package edu.ucsc.dbtune.bip.sim;

import ilog.concert.IloException;
import ilog.concert.IloNumVar;
import ilog.cplex.IloCplex;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.ucsc.dbtune.bip.util.AbstractBIPSolver;
import edu.ucsc.dbtune.bip.util.BIPOutput;
import edu.ucsc.dbtune.bip.util.BIPPreparatorSchema;
import edu.ucsc.dbtune.bip.util.CPlexBuffer;
import edu.ucsc.dbtune.bip.util.StringConcatenator;
import edu.ucsc.dbtune.bip.util.IndexInSlot;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.bip.util.QueryPlanDesc;
import edu.ucsc.dbtune.bip.sim.ScheduleIndexPool;
import edu.ucsc.dbtune.metadata.Index;


/**
 * The class that generates the set of linear expressions (constraints and objective functions)
 * for the problem of Scheduling Index Materialization
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
	private ScheduleIndexPool poolIndexes;
	private SimVariablePool poolVariables;
    // Map variable of type CREATE or DROP to the indexes
    private Map<String,Index> mapVarCreateDropToIndex;
           
    
    /**
     * The constructor of the object to find the optimal index materialization
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
	              List<BIPPreparatorSchema> listPreparators,
	              final int W, final double timeLimit)
	{	
		this.W = W;
		this.timeLimit = timeLimit;
		listCwq = new ArrayList<String>();	
		this.poolVariables = new SimVariablePool();
		mapVarCreateDropToIndex = new HashMap<String,Index>();
		
		this.Sinit = Sinit;
		this.Smat = Smat;
		this.listPreparators = listPreparators;
	}
	
	/**    
     * Derive an optimal schedule of materializing selected indexes
     * 
     * 
     * @return
     *      A sequence of indexes to be materialized (created/dropped) in each maintenance window
	 * @throws SQLException 
     * 
     */
    public BIPOutput solve() throws SQLException
    {   
        poolIndexes = new ScheduleIndexPool();
      
        // 1. Store indexes into the {@code poolIndexes} in the order of
        // indexes of type CREATE, then indexes of type DROP, and finally indexes of type REMAIN
        insertIndexesToPool();
        
        // 2. Derive the query plan description including internal cost, index access cost,
        // index at each slot  
        this.populatePlanDescriptionForStatement(this.poolIndexes);
        
        // Formulate BIP and run the BIP to derive the final result 
        // which is a list of materialized indexes      
        LogListener listener = new LogListener() {
            public void onLogEvent(String component, String logEvent) {
                //To change body of implemented methods use File | Settings | File Templates.
            }
        };
           
        try {
            build(listener);            
            CPlexBuffer.concat(this.buf.getLpFileName(), buf.getObjFileName(), buf.getConsFileName(), buf.getBinFileName());                          
        }
        catch(IOException e){
            System.out.println("Error " + e);
        }   
        
        //Load the corresponding CPLEX problem from the corresponding text file
        try {               
            cplex = new IloCplex(); 
                      
            // Read model from file with name @cplexFile into cplex optimizer object
            cplex.importModel(this.buf.getLpFileName()); 
            
            // Solve the model and record the solution into @listIndex 
            // if one was found
            if (cplex.solve()) {    
                System.out.println("The objective function value: " + cplex.getObjValue());
               return getMaterializationSchedule();
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
     * Classify the index into one of the three types: 
     * INDEX_TYPE_CREATE, INDEX_TYPE_DROP, INDEX_TYPE_REMAIN
     * 
     *     
     *  {\bf Note}: After the indexes are classified into their categories,
     *  we will store the indexes into {@code poolIndexes} in the order of:
     *  indexes of type CREATE, and then DROP, and finally REMAIN    
     * 
     */
    @Override
    protected void insertIndexesToPool()
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
     * Construct the BIP and store in a file
     * 
     * @param listener
     *      The logger
     * @throws IOException
     */
    private final void build(final LogListener listener) throws IOException 
    {
        listener.onLogEvent(LogListener.BIP, "Building IIP program...");
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
                
        listener.onLogEvent(LogListener.BIP, "Built SIM program.");
    }
    
    
    
	/**
     * Add all variables into the pool of variables of the BIP formulation
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
        System.out.println(" ======== num query descs: " + listQueryPlanDescs.size());
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
	 * 
	 * Cqw = \sum_{k \in [1, Kq]} \beta_{qk} y(w,q,k) + 
	 *      + \sum_{ k \in [1, Kq] \\ i \in [1, n] \\ a \in S_i \cup I_{\emptyset} x(w,q,k,i,a) \gamma_{q,k,i,a}     
	 *
	 */
	private void buildQueryCostWindow()
	{
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
					    if (desc.isReferenced(i) == false) {
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
	 * Constraint all variables to be binary ones
	 * 
	 */
	private void binaryVariableConstraints()
	{
	    int NUM_VAR_PER_LINE = 10;
        String strListVars = poolVariables.enumerateListVariables(NUM_VAR_PER_LINE);
        buf.getBin().println(strListVars);	
	}
	
	/**
     * Retrieve a materialized schedule from the result of the formulated BIP:
     *     Use the value of variables {@code create_{w,i}} and {@code drop_{w,i}}
     * 
     * @return
     *      The corresponding materialization schedule
     */
    private MaterializationSchedule getMaterializationSchedule()
    { 
        MaterializationSchedule schedule = new MaterializationSchedule(this.W);
        
        // Iterate over variables create_{i,w} and drop_{i,w}
        try {
            matrix = getMatrix(cplex);
            vars = matrix.getNumVars();
            
            for (int i = 0; i < vars.length; i++) {
                IloNumVar var = vars[i];
                if (cplex.getValue(var) == 1) {
                    System.out.println(" var: " + var.getName());
                    SimVariable simVar = (SimVariable) getVariable(var.getName());
                    if (simVar.getType() == SimVariablePool.VAR_CREATE 
                        || simVar.getType() == SimVariablePool.VAR_DROP ) {
                        schedule.addIndexWindow(getIndexOfVarCreateDrop(var.getName()), 
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
    
    
	/* (non-Javadoc)
     * @see edu.ucsc.dbtune.bip.sim.LinGenerator#getVariable(java.lang.String)
     */
	@Override
	public SimVariable getVariable(String name)
	{
	    System.out.println(" List vars: " + poolVariables.enumerateListVariables(10));
	    if (this.poolVariables.getVariable(name) == null) {
	        System.out.println(" NULL ");
	    }
	    System.out.println(" Name: " + name);
	    return (SimVariable) this.poolVariables.getVariable(name);
	}
	
	/**
     * Retrieve the corresponding index that a variable of type create, drop is defined on
     * @param name
     *     The given of a BIP variable     
     * @return
     *     An index object or NULL
     */
	private Index getIndexOfVarCreateDrop(String name)
	{
	    return this.mapVarCreateDropToIndex.get(name);
	}    
}
