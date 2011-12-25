package edu.ucsc.dbtune.bip.div;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import edu.ucsc.dbtune.bip.sim.SimVariablePool;
import edu.ucsc.dbtune.bip.util.BIPIndexPool;
import edu.ucsc.dbtune.bip.util.QueryPlanDesc;
import edu.ucsc.dbtune.bip.util.CPlexBuffer;
import edu.ucsc.dbtune.bip.util.StringConcatenator;
import edu.ucsc.dbtune.bip.util.IndexInSlot;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.metadata.Index;

public class DivLinGenerator 
{
    protected CPlexBuffer buf;
    protected List<String> listCrq;
    protected List<QueryPlanDesc> listQueryPlanDescs;
    protected int Nreplicas;
    protected int loadfactor;
    protected double B;
    protected int numConstraints;
    private DivVariablePool poolVariables;
    private BIPIndexPool poolIndexes;
 // Map variable of type CREATE or DROP to the indexes
    private Map<String,Index> mapVarSToIndex;
    
    
    DivLinGenerator(final String prefix, final BIPIndexPool poolIndexes, final List<QueryPlanDesc> listQueryPlanDecs, 
                    final int Nreplicas, final int loadfactor, final double B)
    {       
        this.poolIndexes = poolIndexes;
        this.listQueryPlanDescs = listQueryPlanDecs;
        this.Nreplicas = Nreplicas;
        this.B = B;
        this.loadfactor = loadfactor;
        this.listCrq = new ArrayList<String>();  
        this.poolVariables = new DivVariablePool();
        this.mapVarSToIndex = new HashMap<String, Index>();
        
        try {
            this.buf = new CPlexBuffer(prefix);
        }
        catch (IOException e) {
            System.out.println(" Error in opening files " + e.toString());          
        }
    }
    
    /**
     * The function builds the BIP Divergent Index Tuning Problem:
     * <p>
     * <ol>
     *  <li> Atomic constraints </li>
     *  <li> Top-m best cost constraints </li>
     *  <li> Space constraints </li>
     * </ol>
     * </p>   
     * 
     * @param listener
     *      Log the building process
     * 
     * @throws IOException
     */
    public final void build(final LogListener listener) throws IOException 
    {
        listener.onLogEvent(LogListener.BIP, "Building IIP program...");
        numConstraints = 0;
        
        // 1. Add variables into list
        constructVariables();
        
        // 2. Construct the query cost at each replica
        buildQueryCostReplica();
        
        // 3. Atomic constraints
        buildAtomicConstraints();       
        
        // 4. Top-m best cost 
        buildTopmBestCostConstraints();
        
        // 5. Space constraints
        buildSpaceConstraints();
        
        // 6. Optimal constraint
        buildObjectiveFunction();
        
        // 7. binary variables
        binaryVariableConstraints();
        
        buf.close();
                
        listener.onLogEvent(LogListener.BIP, "Built IIP program");
    }
    
    /**
     * Add all variables into the pool of variables of this BIP formulation
     *  
     */
    protected void constructVariables()
    {   
        // for TYPE_Y, TYPE_X
        for (QueryPlanDesc desc : listQueryPlanDescs){
            int q = desc.getId();
            for (int r = 0; r < Nreplicas; r++) {
                for (int k = 0; k < desc.getNumPlans(); k++) {
                    poolVariables.createAndStoreBIPVariable(SimVariablePool.VAR_Y, r, q, k, 0, 0);
                }    
                for (int k = 0; k < desc.getNumPlans(); k++) {              
                    for (int i = 0; i < desc.getNumSlots(); i++) {  
                        for (int a = 0; a < desc.getNumIndexesEachSlot(i); a++) {
                            poolVariables.createAndStoreBIPVariable(SimVariablePool.VAR_X, r, q, k, i, a);
                        }
                    }
                }       
            }
        }
        
        // for TYPE_S
        for (int ga = 0; ga < poolIndexes.getTotalIndex(); ga++) {
            for (int r = 0; r < Nreplicas; r++) {
                DivVariable var = poolVariables.createAndStoreBIPVariable(SimVariablePool.VAR_S, r, ga, 0, 0, 0);
                this.mapVarSToIndex.put(var.getName(), poolIndexes.indexes().get(ga));
            }
        }
    }
    
    /**
     * Build cost function of each query in each window w
     * Cqr = \sum_{k \in [1, Kq]} \beta_{qk} y(r,q,k) + 
     *      + \sum_{ k \in [1, Kq] \\ i \in [1, n] \\ a \in S_i \cup I_{\emptyset} x(r,q,k,i,a) \gamma_{q,k,i,a}
     * {\b Note}: Add variables of type Y, X, S into the list of variables     
     */
    protected void buildQueryCostReplica()
    {           
        for (QueryPlanDesc desc : listQueryPlanDescs){
            int q = desc.getId();
            
            for (int r = 0; r < Nreplicas; r++) {
                // Internal plan
                List<String> linList = new ArrayList<String>();            
                for (int k = 0; k < desc.getNumPlans(); k++) {
                    String var = poolVariables.getDivVariable(DivVariablePool.VAR_Y, r, q, k, 0, 0).getName();
                    String element = Double.toString(desc.getInternalPlanCost(k)) + var;
                    linList.add(element); 
                }
                String Cwq  = StringConcatenator.concatenate(" + ", linList);          
                        
                // Index access cost
                linList.clear();            
                for (int k = 0; k < desc.getNumPlans(); k++) {              
                    for (int i = 0; i < desc.getNumSlots(); i++) {  
                        for (int a = 0; a < desc.getNumIndexesEachSlot(i); a++) {
                            String var = poolVariables.getDivVariable(DivVariablePool.VAR_X, r, q, k, i, a).getName();
                            String element = Double.toString(desc.getIndexAccessCost(k, i, a)) + var;
                            linList.add(element);     
                        }
                    }
                }       
                Cwq = Cwq + " + " + StringConcatenator.concatenate(" + ", linList);
                listCrq.add(Cwq);
            }
        }
    }
    
    /**
     * Atomic configuration constraints:
     *     
     */
    protected void buildAtomicConstraints()
    {
        this.buildAtomicInternalPlanConstraints();
        this.buildAtomicIndexAcessCostConstraints();
    }
    
    /**
     * Constraints on internal plans: different from INUM
     */
    protected void buildAtomicInternalPlanConstraints()
    {   
        for (QueryPlanDesc desc : listQueryPlanDescs){
            int q = desc.getId();
        
            for (int r = 0; r < Nreplicas; r++) {
                List<String> linList = new ArrayList<String>();
                // \sum_{k \in [1, Kq]}y^{r}_{qk} <= 1
                for (int k = 0; k < desc.getNumPlans(); k++) {
                    linList.add(poolVariables.getDivVariable(DivVariablePool.VAR_Y, r, q, k, 0, 0).getName());
                }
                buf.getCons().println("atomic_2a_" + numConstraints + ": " + StringConcatenator.concatenate(" + ", linList) + " <= 1");
                numConstraints++;
            }
        }
    }
    
    /**
     * 
     * Index access cost and the presence of an index constraint
     * 
     */
    protected void buildAtomicIndexAcessCostConstraints()
    {   
        for (QueryPlanDesc desc : listQueryPlanDescs){
            int q = desc.getId();
        
            for (int r = 0; r < Nreplicas; r++) {
                List<String> linList = new ArrayList<String>();
                
                // \sum_{a \in S_i} x(r, q, k, i, a) = y(r, q, k)
                for (int k = 0; k < desc.getNumPlans(); k++) {
                    String var_y = poolVariables.getDivVariable(DivVariablePool.VAR_Y, r, q, k, 0, 0).getName();
                    
                    for (int i = 0; i < desc.getNumSlots(); i++) {
                        if (desc.isReferenced(i) == false) {
                            continue;
                        }                        
                        linList.clear();
                        for (int a = 0; a < desc.getNumIndexesEachSlot(i); a++) {
                            String var_x = poolVariables.getDivVariable(DivVariablePool.VAR_X, r, q, k, i, a).getName();
                            linList.add(var_x);
                            IndexInSlot iis = new IndexInSlot(q,i,a);
                            int ga = poolIndexes.getPoolID(iis);
                            String var_s = poolVariables.getDivVariable(DivVariablePool.VAR_S, r, ga, 0, 0, 0).getName();
                            
                            // (3) s_a^{r} \geq x_{qkia}^{r}
                            buf.getCons().println("atomic_2c_" + numConstraints + ":" 
                                                + var_x + " - " 
                                                + var_s
                                                + " <= 0 ");
                            numConstraints++;
                        }
                        buf.getCons().println("atomic_2b_" + numConstraints  
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
     * Top-m best cost constraints
     * 
     */
    protected void buildTopmBestCostConstraints()
    {   
        for (QueryPlanDesc desc : listQueryPlanDescs){
            int q = desc.getId();
            List<String> linList = new ArrayList<String>();
            for (int r = 0; r < Nreplicas; r++) {
                for (int k = 0; k < desc.getNumPlans(); k++) {
                    linList.add(poolVariables.getDivVariable(DivVariablePool.VAR_Y, r, q, k, 0, 0).getName());
                }
            }
            buf.getCons().println("topm_3a_" + numConstraints + ": " + StringConcatenator.concatenate(" + ", linList) + " = " + loadfactor);
            numConstraints++;
        }
    }
    
    /**
     * Impose space constraint on the materialized indexes at all window times
     * 
     */
    protected void buildSpaceConstraints()
    {   
        for (int r = 0; r < Nreplicas; r++) {
            List<String> linList = new ArrayList<String>();
            for (int idx = 0; idx < poolIndexes.getTotalIndex(); idx++) {
                String var_create = poolVariables.getDivVariable(DivVariablePool.VAR_S, r, idx, 0, 0, 0).getName();
                double sizeindx = poolIndexes.indexes().get(idx).getBytes();
                linList.add(Double.toString(sizeindx) + var_create);
            }
            buf.getCons().println("space_constraint_4" + numConstraints  
                    + " : " + StringConcatenator.concatenate(" + ", linList)                    
                    + " <= " + B);
            numConstraints++;               
        }
    }
    
    /**
     * The accumulated total cost function
     */
    protected void buildObjectiveFunction()
    {
        buf.getObj().println(StringConcatenator.concatenate(" + ", listCrq));
    }
    
    
    /**
     * Constraints all variables to be binary ones
     * 
     */
    protected void binaryVariableConstraints()
    {
        int NUM_VAR_PER_LINE = 10;
        String strListVars = poolVariables.enumerateListVariables(NUM_VAR_PER_LINE);
        buf.getBin().println(strListVars);     
    }
    
    public DivVariable getVariable(String name)
    {
        return (DivVariable) this.poolVariables.getVariable(name);
    }
    
    public Index getIndexOfVarS(String name)
    {
        return this.mapVarSToIndex.get(name);
    }
}
