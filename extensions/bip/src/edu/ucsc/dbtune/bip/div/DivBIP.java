package edu.ucsc.dbtune.bip.div;

import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;

import java.util.ArrayList;
import java.util.Collections;
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
import static edu.ucsc.dbtune.workload.SQLCategory.NOT_SELECT;

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
     * Retrieve the (constant) base table update cost
     * 
     * @return
     *      The base table update cost.
     */
    public double getTotalBaseTableUpdateCost()
    {
        double totalBaseTableUpdateCost = 0.0;
        
        // base update cost on one replicas
        for (QueryPlanDesc desc : queryPlanDescs)
            if (desc.getSQLCategory().isSame(NOT_SELECT))
                totalBaseTableUpdateCost += desc.getBaseTableUpdateCost();
            
        // need to time the number of replica
        return totalBaseTableUpdateCost * nReplicas;
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
            //cplex.setParam(DoubleParam.EpLin, 1e-1);
            UtilConstraintBuilder.cplex = cplex;
            
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
     * Build the total cost formula, which is the summation of the costs of all replicas.      
     *     
     * @throws IloException 
     *      when there is error in calling {@code IloCplex} class
     */
    protected void totalCost() throws IloException
    {         
        IloLinearNumExpr obj = cplex.linearNumExpr();
        
        for (int r = 0; r < nReplicas; r++) 
            obj.add(replicaCost(r));
        
        cplex.addMinimize(obj);
    }
    
    /**
     * Build the cost at each replica.
     * 
     * CostReplica(r) = \sum_{q \in Q} cost(q,r) / loadfactor : query statement
     *                + \sum_{q \in Qshell} cost(q,r)         : query shell in update statement
     *                + \sum_{u \in Q} \sum_{a \in Cand} cost(u, a, r) \times s_a^r
     *                                                        : update cost
     */
    protected IloLinearNumExpr replicaCost(int r) throws IloException
    {
        IloLinearNumExpr expr = cplex.linearNumExpr();
        
        // query component
        for (QueryPlanDesc desc : queryPlanDescs) 
            expr.add(UtilConstraintBuilder.modifyCoef(queryExpr(r, desc.getStatementID(), desc), 
                               getFactorStatement(desc)
                               )
                    );
        
        // update component
        for (QueryPlanDesc desc : queryPlanDescs) 
            if (desc.getSQLCategory().isSame(NOT_SELECT)) 
                expr.add(updateCost(r, desc.getStatementID(), desc));
        
        return expr;
    }
    
    /**
     * Retrieve the balance factor of the given statement.
     * 
     * If the this is a query statement, the {@code factor = 1 / loadfactor}
     * else if this is a query shell from the update statement, then {@code factor = 1.0}
     * 
     * @param desc
     *      The given query plan description
     *      
     * @return
     *      The factor
     */
    protected double getFactorStatement(QueryPlanDesc desc)
    {
        return (desc.getSQLCategory().isSame(NOT_SELECT)) ? 
                1.0 :  (double) 1 / loadfactor;
    }
    
    /**
     * Build the update cost formula for the given query on the given replica 
     * 
     * @param r
     *      The replica ID
     * @param q
     *      The statement ID
     * @param desc
     *      The query plan description
     *      
     * @return
     *      The update cost formulas
     *      
     * @throws IloException
     */
    protected IloLinearNumExpr updateCost(int r, int q, QueryPlanDesc desc) throws IloException
    {
        IloLinearNumExpr expr = cplex.linearNumExpr();
        int idS;
        double  cost;
        
        for (Index index : candidateIndexes) {
            
            if (index instanceof FullTableScanIndex)
                continue;
            
            cost = desc.getUpdateCost(index);
            
            if (cost != 0.0) {
                idS = poolVariables.get(VAR_S, r, 0, 0, index.getId()).getId();
                expr.addTerm(cplexVar.get(idS), cost);
            }
        }
        
        return expr;
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
        int rhs;
        
        for (QueryPlanDesc desc : queryPlanDescs) {
            
            q = desc.getStatementID();
            expr = cplex.linearNumExpr();
            
            for (int r = 0; r < nReplicas; r++)
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
                    idY = poolVariables.get(VAR_Y, r, q, k, 0).getId();
                    expr.addTerm(1, cplexVar.get(idY));
                }
            
            rhs = (desc.getSQLCategory().isSame(NOT_SELECT)) ? nReplicas : loadfactor;
            cplex.addEq(expr, rhs, "top_m_" + numConstraints);            
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
                // not consider FTS index
                if (!(index instanceof FullTableScanIndex)) {
                    idS = poolVariables.get(VAR_S, r, 0, 0 , index.getId()).getId();                
                    expr.addTerm(index.getBytes(), cplexVar.get(idS));
                }
            }
            
            cplex.addLe(expr, B, "space_" + numConstraints);
            numConstraints++;               
        }
    }
    
    
    /**
     * Retrieve the update cost, computed by INUM including the cost for the query shell
     * and the update indexes (NOT including the cost to update base tables)
     * 
     * @return
     *      The update cost
     */
    public double getUpdateCost() throws Exception
    {
        // get variables assignment
        getMapVariableValue();
        
        // run over each replica
        double cost;
        int id;
        int q;
        int idS;
        
        cost = 0.0;
        
        for (QueryPlanDesc desc : queryPlanDescs) {
            
            if (!desc.getSQLCategory().isSame(NOT_SELECT))
                continue;
            
            q = desc.getStatementID();
        
            
            for (int r = 0; r < nReplicas; r++) {
                // query shell cost
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
                
                // update cost
                for (Index index : candidateIndexes) {
                    
                    if (index instanceof FullTableScanIndex)
                        continue;
                    
                    idS = poolVariables.get(VAR_S, r, 0, 0, index.getId()).getId();
                    cost += desc.getUpdateCost(index) * valVar[idS];
                    
                }
            }
        }
        
        return cost;
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
    
    /**
     * Compute the query cost from the result of CPLEX.
     * 
     * @param r
     *  todo
     * @param q
     * @param desc
     * @return
     */
    public double costQuery(int r, int q, QueryPlanDesc desc)
    {
        double cost;
        int id;
        
        cost = 0.0;
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
        
        return cost;
    }
    
    /**
     * Compute the cost to update affected indexes.  
     * @param r
     * @param q
     * @param desc
     */
    public double costUpdateIndexes(int r, QueryPlanDesc desc)
    {
        int idS;
        double cost = 0.0;
        
        for (Index index : candidateIndexes) {
            
            if (index instanceof FullTableScanIndex)
                continue;
            
            idS = poolVariables.get(VAR_S, r, 0, 0, index.getId()).getId();
            cost += valVar[idS] * desc.getUpdateCost(index);
            
        }
        
        return cost;
    }
   
    /**
     * Compute the max imbalance replica and query factor.
     * 
     */
    public void computeImbalanceFactor() throws Exception
    {
        // get variables assignment
        getMapVariableValue();
        
        // run over each replica
        double costReplica;
        List<Double> replicas = new ArrayList<Double>();
        
        // replica
        for (int r = 0; r < nReplicas; r++) {
            
            costReplica = 0.0;
            for (QueryPlanDesc desc : queryPlanDescs) {
                costReplica += costQuery(r, desc.getStatementID(), desc);
            
                if (desc.getSQLCategory().isSame(NOT_SELECT))
                    costReplica += this.costUpdateIndexes(r, desc);
            }
            
            // add the base update cost
            costReplica += getTotalBaseTableUpdateCost() / nReplicas;             
            replicas.add(costReplica);
        }
        
        System.out.println(" Max imbalance REPLICA: " + maxRatioInList(replicas));
        
        // query
        List<Double> costs = new ArrayList<Double>();
        
        double maxRatio = -1;
        double ratio;
        double cost;
        
        for (QueryPlanDesc desc : queryPlanDescs) {
            
            // only consider select statement
            if (desc.getSQLCategory().isSame(NOT_SELECT))
                continue;
            
            costs = new ArrayList<Double>();
            for (int r = 0; r < nReplicas; r++)  {
                cost = costQuery(r, desc.getStatementID(), desc);
                if (cost > 0)
                    costs.add(cost);
            }
        
            Collections.sort(costs);
            
            ratio = maxRatioInList(costs);
            if (ratio > maxRatio)
               maxRatio = ratio;
                        
        }
        
        System.out.println(" Max imbalance QUERY: " + maxRatio);
    }
    
    /**
     * Compute the maximum ratio between any two elements in the list.
     * 
     * @param costs
     *      The list of value
     * @return
     *      The maximum ratio
     */
    public double maxRatioInList(List<Double> costs)
    {
        double maxRatio = -1;
        double ratio;
        
        for (int r1 = 0; r1 < costs.size(); r1++)
            for (int r2 = r1 + 1; r2 < costs.size(); r2++) {
                
                ratio = (double) costs.get(r1) / costs.get(r2);
                if (ratio > maxRatio)
                    maxRatio = ratio;
                
                ratio = (double) costs.get(r2) / costs.get(r1);
                if (ratio > maxRatio)
                    maxRatio = ratio;
            }
        
        return maxRatio;
    }
    
}
