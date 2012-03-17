package edu.ucsc.dbtune.bip.div;

import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;

import java.util.HashMap;
import java.util.Map;

import edu.ucsc.dbtune.bip.core.AbstractBIPSolver;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.inum.FullTableScanIndex;
import edu.ucsc.dbtune.metadata.Index;

import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_S;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_X;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_Y;

public class DivBIP extends AbstractBIPSolver implements Divergent
{  
    protected int    Nreplicas;
    protected int    loadfactor;    
    protected double B;
    protected double beta;
    
    protected DivVariablePool   poolVariables;    
    protected Map<String,Index> mapVarSToIndex;
    
    protected boolean isImbalanceReplica;
    
    /**
     * Initial the default value for input parameters
     */
    public DivBIP()
    {
        isImbalanceReplica = false;
    }
    
    @Override
    public void setLoadBalanceFactor(int m) 
    {
        this.loadfactor = m;
    }


    @Override
    public void setNumberReplicas(int n)
    {
        Nreplicas  = n;
    }


    @Override
    public void setReplicaImbalanceFactor(int beta) 
    {
        this.beta = beta;
        this.isImbalanceReplica = true;
    }

    @Override
    public void setSpaceBudget(double B) 
    {
        this.B = B;
    }
    
    /**
     * Reads the value of variables corresponding to the presence of indexes
     * at each replica and returns indexes to materialize at each replica.
     * 
     * @return
     *      List of indexes to be materialized at each replica.
     */
    @Override
    protected IndexTuningOutput getOutput() 
    {
        DivConfiguration conf = new DivConfiguration(Nreplicas);
        
        // Iterate over variables s^r_{i,w}
        for (int i = 0; i < poolVariables.variables().size(); i++)
            if (valVar[i] == 1) {
                DivVariable var = (DivVariable) poolVariables.variables().get(i);
                
                if (var.getType() == VAR_S) {
                    Index index = mapVarSToIndex.get(var.getName());
                    if (!(index instanceof FullTableScanIndex))
                        conf.addIndexReplica(var.getReplica(), index);
                }
            }
        
        return conf;
    }

   
    @Override
    protected void buildBIP() 
    {
        numConstraints = 0;
        
        try {
            // 1. Add variables into list
            constructVariables();
            
            // 2. Construct the query cost at each replica
            totalCost();
            
            // 3. Atomic constraints
            atomicInternalPlanConstraints();
            atomicAcessCostConstraints();      
            
            // 4. Top-m best cost 
            topMBestCostConstraints();
            
            // 5. Space constraints
            spaceConstraints();
            
            // 6. Imbalance replica constraints (if any)
            if (isImbalanceReplica)
                imbalanceReplicaConstraints();
            
        }     
        catch (IloException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * Add all variables into the pool of variables of this BIP formulation
     *  
     */
    protected void constructVariables() throws IloException
    {   
        poolVariables = new DivVariablePool();
        mapVarSToIndex = new HashMap<String, Index>();
        
        int q;
        
        // for TYPE_Y, TYPE_X
        for (QueryPlanDesc desc : queryPlanDescs){
            
            q = desc.getStatementID();
            
            for (int r = 0; r < Nreplicas; r++) {
                
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
                    poolVariables.createAndStore(VAR_Y, r, q, k, 0);
                    
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
                    for (int i = 0; i < desc.getNumberOfSlots(); i++)  
                        for (Index index : desc.getIndexesAtSlot(i)) 
                            poolVariables.createAndStore(VAR_X, r, q, k, index.getId());
                       
            }
        }
        
        // for TYPE_S
        for (Index index : candidateIndexes)
            for (int r = 0; r < Nreplicas; r++) {
                DivVariable var = poolVariables.createAndStore(VAR_S, r, 0, 0, index.getId());
                mapVarSToIndex.put(var.getName(), index);
            }
        
        super.createCplexVariable(poolVariables.variables());
    }
    
    
    /**
     * Build cost function of each query in each window w
     * cost(q,r) = \sum_{k \in [1, Kq]} \beta_{qk} y(r,q,k) + 
     *            + \sum_{ k \in [1, Kq] \\ i \in [1, n] \\ a \in S_i \cup I_{\emptyset} 
     *              x(r,q,k,i,a) \gamma_{q,k,i,a}
     *     
     * @throws IloException 
     */
    protected void totalCost() throws IloException
    {         
        IloLinearNumExpr expr = cplex.linearNumExpr(); 
        int id;
        int q;
        
        for (QueryPlanDesc desc : queryPlanDescs) {
            
            q = desc.getStatementID();
            
            for (int r = 0; r < Nreplicas; r++) {

                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {                    
                    id = poolVariables.get(VAR_Y, r, q, k, 0).getId();
                    expr.addTerm(desc.getInternalPlanCost(k), cplexVar.get(id));                    
                }                
                        
                // Index access cost                            
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
                    for (int i = 0; i < desc.getNumberOfSlots(); i++)
                        for (Index index : desc.getIndexesAtSlot(i)) {
                            id = poolVariables.get(VAR_X, r, q, k, index.getId()).getId();
                            expr.addTerm(desc.getAccessCost(k, index), cplexVar.get(id));
                        }
            }
        }
        
        cplex.addMinimize(expr);
    }
    
    
    
    /**
     * Atomic constraints: only one template plan is chosen to compute {@code cost(q,r)}.
     * 
     * @throws IloException
     *      If there is error in formulating the expression in CPLEX.
     */
    protected void atomicInternalPlanConstraints() throws IloException
    {   
        IloLinearNumExpr expr; 
        int id;
        int q;
        
        for (QueryPlanDesc desc : queryPlanDescs) {
            
            q = desc.getStatementID();
        
            for (int r = 0; r < Nreplicas; r++) {
                
                expr = cplex.linearNumExpr();
                
                // \sum_{k \in [1, Kq]}y^{r}_{qk} <= 1
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
                    id = poolVariables.get(VAR_Y, r, q, k, 0).getId();
                    expr.addTerm(1, cplexVar.get(id));
                }
                
                cplex.addLe(expr, 1);
                numConstraints++;
                
            }
        }
    }
    
    /**
     * 
     * Atomic constraint: at most one index is selected to plug into a slot. 
     * Present of an index: a is recommended if it is used to compute at least one cost(q,r)
     * 
     * @throws IloException
     *      If there is error in formulating the expression in CPLEX. 
     * 
     */
    protected void atomicAcessCostConstraints() throws IloException
    {   
        IloLinearNumExpr expr; 
        int idY;
        int idX;
        int idS;
        int q;
        
        // atomic constraint
        for (QueryPlanDesc desc : queryPlanDescs){
            
            q = desc.getStatementID();
        
            for (int r = 0; r < Nreplicas; r++) {
                
                // \sum_{a \in S_i} x(r, q, k, i, a) = y(r, q, k)
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
                    
                    idY = poolVariables.get(DivVariablePool.VAR_Y, r, q, k, 0).getId();
                    
                    for (int i = 0; i < desc.getNumberOfSlots(); i++) {            
                    
                        expr = cplex.linearNumExpr();
                        expr.addTerm(-1, cplexVar.get(idY));
                        
                        for (Index index : desc.getIndexesAtSlot(i)) { 
                            idX = poolVariables.get(VAR_X, r, q, k, index.getId()).getId();                            
                            expr.addTerm(1, cplexVar.get(idX));
                        }
                        
                        cplex.addEq(expr, 0, "atomic_constraint_" + numConstraints);
                        numConstraints++;
                        
                    }
                }       
            }
        }
        
        // used index constraint
        for (QueryPlanDesc desc : queryPlanDescs) {
            
            q = desc.getStatementID();
            
            // s(a,r ) >= x(r, q, k, a)
            for (int r = 0; r < Nreplicas; r++)
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
                    for (int i = 0; i < desc.getNumberOfSlots(); i++)   
                        for (Index index : desc.getIndexesAtSlot(i)) {
                            
                            idX = poolVariables.get(VAR_X, r, q, k, index.getId()).getId();
                            idS = poolVariables.get(VAR_S, r, 0, 0, index.getId()).getId();
                            
                            expr = cplex.linearNumExpr();
                            expr.addTerm(1, cplexVar.get(idX));
                            expr.addTerm(-1, cplexVar.get(idS));
                            cplex.addLe(expr, 0, "atomic_constraint_" + numConstraints);
                            numConstraints++;
                            
                        }
            
        }
    }
    
    /**
     * Top-m best cost constraints.
     * 
     * @throws IloException
     *      If there is error in formulating the expression in CPLEX. 
     * 
     */
    protected void topMBestCostConstraints() throws IloException
    {   
        IloLinearNumExpr expr; 
        int idY;        
        int q;
        
        for (QueryPlanDesc desc : queryPlanDescs) {
            
            q = desc.getStatementID();
            expr = cplex.linearNumExpr();
            
            for (int r = 0; r < Nreplicas; r++)
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
                    idY = poolVariables.get(VAR_Y, r, q, k, 0).getId();
                    expr.addTerm(1, cplexVar.get(idY));
                }
            
            
            cplex.addEq(expr, loadfactor);            
            numConstraints++;
        }
    }
    
    /**
     * Impose space constraint on the materialized indexes at all window times.
     * 
     * @throws IloException
     *      If there is error in formulating the expression in CPLEX. 
     * 
     */
    protected void spaceConstraints() throws IloException
    {   
        IloLinearNumExpr expr; 
        int idS;       
        
        for (int r = 0; r < Nreplicas; r++) {

            expr = cplex.linearNumExpr();
            
            for (Index index : candidateIndexes) {  
                idS = poolVariables.get(VAR_S, r, 0, 0 , index.getId()).getId();                
                expr.addTerm(index.getBytes(), cplexVar.get(idS));
            }
            
            cplex.addLe(expr, B);
            numConstraints++;               
        }
    }

    protected void imbalanceReplicaConstraints()
    {
        
    }
}
