package edu.ucsc.dbtune.bip.div;

import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;
import ilog.concert.IloLinearNumExprIterator;
import ilog.concert.IloNumVar;
import ilog.cplex.IloCplex.DoubleParam;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.bip.core.AbstractBIPSolver;
import edu.ucsc.dbtune.bip.core.BIPVariable;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.inum.FullTableScanIndex;
import edu.ucsc.dbtune.metadata.Index;

import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_S;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_X;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_Y;

/**
 * Reference {@link http://www-01.ibm.com/support/docview.wss?uid=swg21400034} for tuning 
 * CPLEX solver.
 * 
 * @author Quoc Trung Tran
 *
 */
public class DivBIP extends AbstractBIPSolver implements Divergent
{  
    protected int    nReplicas;
    protected int    loadfactor;    
    protected double B;
    
    protected DivVariablePool   poolVariables;    
    protected Map<String,Index> mapVarSToIndex;
    
    
    @Override
    public void setLoadBalanceFactor(int m) 
    {
        this.loadfactor = m;
    }


    @Override
    public void setNumberReplicas(int n)
    {
        nReplicas  = n;
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
        DivConfiguration conf = new DivConfiguration(nReplicas, loadfactor);
        
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
            // Dual Simplex = 2
            // Sifting is a simple form of column generation well suited for models 
            // where the number of variables dramatically exceeds the number of constraints. 
            // Concurrentopt works with multiple processors, running a different algorithm 
            // on each processor and stopping as soon as one of them finds an optimal solution
            //cplex.setParam(IntParam.NodeAlg, IloCplex.Algorithm.Dual);
            //cplex.setParam(IntParam.RootAlg, IloCplex.Algorithm.Sifting);            
            //cplex.setParam(IntParam.SiftAlg, 2);
            
            // epsilon in linerization
            cplex.setParam(DoubleParam.EpLin, 1e-1);
            
            // 1. Add variables into list
            constructVariables();
            super.createCplexVariable(poolVariables.variables());
            
            // 2. Construct the query cost at each replica
            totalCost();
            
            // 3. Atomic constraints
            atomicConstraints();      
            
            // 4. Top-m best cost 
            topMBestCostConstraints();
            
            // 5. Space constraints
            spaceConstraints();
            
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
        // reset variable counter
        BIPVariable.resetIdGenerator();
        
        DivVariable var;
        
        poolVariables = new DivVariablePool();
        mapVarSToIndex = new HashMap<String, Index>();
        
        // variable for each query descriptions
        for (int r = 0; r < nReplicas; r++)
            for (QueryPlanDesc desc : queryPlanDescs)
                constructVariables(r, desc.getStatementID(), desc);
        
        // for TYPE_S
        for (int r = 0; r < nReplicas; r++) 
            for (Index index : candidateIndexes) {
                var = poolVariables.createAndStore(VAR_S, r, 0, 0, index.getId());
                mapVarSToIndex.put(var.getName(), index);
            }
        
    }
    
    /**
     * Construct variables for the query expression of the given query
     * 
     * @param r
     *      The replica ID
     * @param q
     *      The query ID
     * @param desc
     *      The query plan description.
     *      
     */
    protected void constructVariables(int r, int q, QueryPlanDesc desc)
    {
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
            poolVariables.createAndStore(VAR_Y, r, q, k, 0);
            
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
            for (int i = 0; i < desc.getNumberOfSlots(); i++)  
                for (Index index : desc.getIndexesAtSlot(i)) 
                    poolVariables.createAndStore(VAR_X, r, q, k, index.getId());
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
        IloLinearNumExpr obj = cplex.linearNumExpr(); 
        IloLinearNumExpr expr; 
        IloNumVar        var;
        double           coef;
        
        IloLinearNumExprIterator iter;
        
        for (int r = 0; r < nReplicas; r++)
            for (QueryPlanDesc desc : queryPlanDescs) {
                expr = queryExpr(r, desc.getStatementID(), desc);
                iter = expr.linearIterator();
                
                while (iter.hasNext()) {
                    var = iter.nextNumVar();
                    coef = iter.getValue();
                    obj.addTerm(var, coef / loadfactor);
                }
            }
        
        cplex.addMinimize(obj);
    }
    
    /**
     * Formulate the expression of a query on a particular replica
     * 
     * @param r
     *      The replica ID
     * @param q
     *      The query ID
     * @param desc
     *      The query plan description.
     *      
     * @return
     *      The linear expression of the query
     *      
     * @throws IloException
     */
    protected IloLinearNumExpr queryExpr(int r, int q, QueryPlanDesc desc) 
              throws IloException
    {
        int id;
        IloLinearNumExpr expr = cplex.linearNumExpr();
        
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
        
        return expr;
    }
    
    /**
     * Construct the set of atomic constraints.
     * 
     * @throws IloException
     */
    protected void atomicConstraints() throws IloException
    {
        for (int r = 0; r < nReplicas; r++)
            for (QueryPlanDesc desc : queryPlanDescs) {
                internalAtomicConstraint(r, desc.getStatementID(), desc);
                slotAtomicConstraints(r, desc.getStatementID(), desc);
            }
    }
    
        
    /**
     * Internal atomic constraints: only one template plan is chosen to compute {@code cost(q,r)}.
     *  
     * @param r
     *     Replica ID
     * @param q
     *     Statement ID
     * @param desc
     *     The query plan description        
     *  
     * @throws IloException
     *      If there is error in formulating the expression in CPLEX.
     */
    protected void internalAtomicConstraint(int r, int q, QueryPlanDesc desc) throws IloException
    {
        IloLinearNumExpr expr;
        int idY;
       
        // \sum_{k \in [1, Kq]}y^{r}_{qk} <= 1
        expr = cplex.linearNumExpr();
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
            idY = poolVariables.get(VAR_Y, r, q, k, 0).getId();
            expr.addTerm(1, cplexVar.get(idY));
        }
        
        cplex.addLe(expr, 1, "atomic_internal_" + numConstraints);
        numConstraints++;
    }
    
    /**
     * Slot atomic constraints:
     *  At most one index is selected to plug into a slot. 
     *  An index a is recommended if it is used to compute at least one cost(q,r)
     *  
     * @param r
     *     Replica ID
     * @param q
     *     Statement ID
     * @param desc
     *     The query plan description        
     *  
     * @throws IloException
     *      If there is error in formulating the expression in CPLEX.
     */
    protected void slotAtomicConstraints(int r, int q, QueryPlanDesc desc) throws IloException
    {
        IloLinearNumExpr expr;
        int idY;
        int idX;
        int idS;
        
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
        
        // used index
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
            for (int i = 0; i < desc.getNumberOfSlots(); i++)   
                for (Index index : desc.getIndexesAtSlot(i)) {
                    
                    idX = poolVariables.get(VAR_X, r, q, k, index.getId()).getId();
                    idS = poolVariables.get(VAR_S, r, 0, 0, index.getId()).getId();
                    
                    expr = cplex.linearNumExpr();
                    expr.addTerm(1, cplexVar.get(idX));
                    expr.addTerm(-1, cplexVar.get(idS));
                    cplex.addLe(expr, 0, "index_present_" + numConstraints);
                    numConstraints++;
                    
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
            
            for (int r = 0; r < nReplicas; r++)
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
                    idY = poolVariables.get(VAR_Y, r, q, k, 0).getId();
                    expr.addTerm(1, cplexVar.get(idY));
                }
            
            
            cplex.addEq(expr, loadfactor, "top_m_" + numConstraints);            
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
        
        for (int r = 0; r < nReplicas; r++) {

            expr = cplex.linearNumExpr();
            
            for (Index index : candidateIndexes) {  
                idS = poolVariables.get(VAR_S, r, 0, 0 , index.getId()).getId();                
                expr.addTerm(index.getBytes(), cplexVar.get(idS));
            }
            
            cplex.addLe(expr, B, "space_" + numConstraints);
            numConstraints++;               
        }
    }

     /**
     * Recalculate the total cost returned by CPLEX
     *  
     */
    public void costFromCplex() throws Exception
    {   
        int id;
        int q;
        
        // get variables assignment
        getMapVariableValue();
        /*
        for (int i = 0; i < valVar.length; i++)
            if (valVar[i] == 1)
                System.out.println(poolVariables.variables().get(i).getName());
        */
        
        // run over each replica
        double cost;
        double costReplica;
        DivConfiguration output = (DivConfiguration) getOutput();
        
        for (int r = 0; r < nReplicas; r++) {
            
            costReplica = 0.0;
            System.out.println(" replica: " + r);
            List<Double> costs = new ArrayList<Double>();
            List<Double> costInums = new ArrayList<Double>();
            Set<Index>   conf = output.indexesAtReplica(r);
            
            for (QueryPlanDesc desc : queryPlanDescs) {
                cost = 0.0;
                q = desc.getStatementID();
                
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {                    
                    id = poolVariables.get(VAR_Y, r, q, k, 0).getId();
                    cost += (desc.getInternalPlanCost(k) * valVar[id]);                    
                }                
                        
                // Index access cost                            
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
                    for (int i = 0; i < desc.getNumberOfSlots(); i++)
                        for (Index index : desc.getIndexesAtSlot(i)) {
                            id = poolVariables.get(VAR_X, r, q, k, index.getId()).getId();
                            cost += (desc.getAccessCost(k, index) * valVar[id]);
                        }
                
                costs.add(cost);
                costReplica += cost;
            }
            
            for (int i = 0; i < workload.size(); i++) {
                cost = inumOptimizer.prepareExplain(workload.get(i)).explain(conf).getTotalCost();
                costInums.add(cost);
            }
          
            System.out.println("query cost (CPLEX): " + costs);
            System.out.println("query cost (INUM): " + costInums);
            System.out.println(" cost replica: " + costReplica);
            
        }
    }
}
