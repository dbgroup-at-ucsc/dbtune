package edu.ucsc.dbtune.bip.div;

import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;

import java.util.ArrayList;
import java.util.List;

import edu.ucsc.dbtune.bip.core.AbstractBIPSolver;
import edu.ucsc.dbtune.bip.core.BIPVariable;
import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.inum.FullTableScanIndex;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Rt;

import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_DIV;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_MOD;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_S;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_X;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_Y;
import static edu.ucsc.dbtune.workload.SQLCategory.NOT_SELECT;
import static edu.ucsc.dbtune.workload.SQLCategory.SELECT;
import static edu.ucsc.dbtune.workload.SQLCategory.UPDATE;
import static edu.ucsc.dbtune.workload.SQLCategory.INSERT;
import static edu.ucsc.dbtune.workload.SQLCategory.DELETE;

import static edu.ucsc.dbtune.bip.div.UtilConstraintBuilder.modifyCoef;
import static edu.ucsc.dbtune.bip.div.UtilConstraintBuilder.maxRatioInList;
import static edu.ucsc.dbtune.bip.div.UtilConstraintBuilder.computeVal;
import static edu.ucsc.dbtune.bip.div.UtilConstraintBuilder.showComputeVal;

/**
 * Reference {@link http://www-01.ibm.com/support/docview.wss?uid=swg21400034} 
 * for tuning CPLEX solver.
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
    protected boolean isUpperTotalCost = false;
    protected double upperTotalCost;
    
    
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
     * Allow to find only feasible solution
     */
    public void checkFeasibleSolutionOnly()
    {
        super.isCheckFeasible = true;
    }
    
    /**
     * Allow to set an upper bound on total cost
     */
    public void setUpperTotalCost(double upper)
    {
        isUpperTotalCost = true;
        upperTotalCost = upper;
    }
    
    /**
     * Clear all the structures used
     */
    public void clear()
    {   
        super.clear();
        this.poolVariables.clear();
    }   

   
    @Override
    protected void buildBIP() 
    {
        numConstraints = 0;
        
        try {            
            UtilConstraintBuilder.cplex = cplex;
            
            // 1. Add variables into list
            constructVariables();
            super.createCplexVariable(poolVariables.variables());
            
            // 2. Construct the query cost at each replica
            totalCost();
            
            // 3. Atomic constraints
            atomicConstraints();
            
            // 4. Use index constraint
            usedIndexConstraints();
            
            // 5. Top-m best cost 
            loadBalanceFactorConstraints();
            
            // 6. Space constraints
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
        poolVariables = new DivVariablePool();
        
        // variable for each query descriptions
        for (int r = 0; r < nReplicas; r++)
            for (QueryPlanDesc desc : queryPlanDescs)
                constructVariables(r, desc.getStatementID(), desc);
                 
        // for TYPE_S
        for (int r = 0; r < nReplicas; r++) 
            for (Index index : candidateIndexes) 
                poolVariables.createAndStore(VAR_S, r, 0, 0, 0, index.getId());
            
        // for div_a and mod_a
        for (int r = 0; r < nReplicas; r++)
            for (Index index : candidateIndexes) {
                poolVariables.createAndStore(VAR_DIV, r, 0, 0, 0, index.getId());                 
                poolVariables.createAndStore(VAR_MOD, r, 0, 0, 0, index.getId());
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
            poolVariables.createAndStore(VAR_Y, r, q, k, 0, 0);
            
        for (int planId = 0; planId < desc.getNumberOfTemplatePlans(); planId++)
            for (int slotId = 0; slotId < desc.getNumberOfSlots(planId); slotId++)  
                for (Index index : desc.getIndexesAtSlot(planId, slotId)) 
                    poolVariables.createAndStore(VAR_X, r, q, planId, slotId, index.getId());
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
     * Set the total cost not to exceed some upper bound constraint
     * @param upperCost
     * @throws IloException
     */
    protected void totalCostConstraint(double upperCost) throws IloException
    {
        IloLinearNumExpr expr = cplex.linearNumExpr();
        
        for (int r = 0; r < nReplicas; r++) 
            expr.add(replicaCost(r));
        
        cplex.addLe(expr, upperCost, "total_cost_constraint_" + numConstraints);
        numConstraints++;
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
        // only applicable for SELECT and UPDATE statements
        for (QueryPlanDesc desc : queryPlanDescs) 
            if (desc.getSQLCategory().isSame(SELECT) || desc.getSQLCategory().isSame(UPDATE))
                expr.add(modifyCoef(queryExpr(r, desc.getStatementID(), desc), 
                                   getFactorStatement(desc) * desc.getStatementWeight()
                                   ));
        
        // update component of NOT_SELECT statement
        for (QueryPlanDesc desc : queryPlanDescs) 
            if (desc.getSQLCategory().isSame(NOT_SELECT)) { 
                expr.add(modifyCoef(indexUpdateCost(r, desc.getStatementID(), desc),
                        desc.getStatementWeight()));
            }
        
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
     *      The update cost formulas (NOT take into account the frequency)
     *      
     * @throws IloException
     */
    protected IloLinearNumExpr indexUpdateCost(int r, int q, QueryPlanDesc desc) 
                throws IloException
    {
        IloLinearNumExpr expr = cplex.linearNumExpr();
        int idS;
        double  cost;
        
        for (Index index : candidateIndexes) {
            
            if (index instanceof FullTableScanIndex)
                continue;
            
            cost = desc.getUpdateCost(index);
            
            if (cost > 0.0) {
                idS = poolVariables.get(VAR_S, r, 0, 0, 0, index.getId()).getId();
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
     *      The linear expression of the query (NOT take into account the frequency)
     *      
     * @throws IloException
     */
    protected IloLinearNumExpr queryExpr(int r, int q, QueryPlanDesc desc) 
              throws IloException
    {
        int id;
        double cost;
        IloLinearNumExpr expr = cplex.linearNumExpr();
        
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {                    
            id = poolVariables.get(VAR_Y, r, q, k, 0, 0).getId();
            expr.addTerm(desc.getInternalPlanCost(k), cplexVar.get(id));                    
        }                
                    
        // Index access cost                            
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
            for (int i = 0; i < desc.getNumberOfSlots(k); i++)
                for (Index index : desc.getIndexesAtSlot(k, i)) {
                    id = poolVariables.get(VAR_X, r, q, k, i, index.getId()).getId();
                    cost = desc.getAccessCost(k, i, index);
                    
                    if (!Double.isInfinite(cost))
                        expr.addTerm(cost, cplexVar.get(id));
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
     * Internal atomic constraints: 
     *      - Only one template plan is chosen to compute {@code cost(q,r)}.
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
            idY = poolVariables.get(VAR_Y, r, q, k, 0, 0).getId();
            expr.addTerm(1, cplexVar.get(idY));
        }
        
        cplex.addLe(expr, 1, "atomic_internal_" + numConstraints);
        numConstraints++;
    }
    
    /**
     * Slot atomic constraints:
     *      - At most one index is selected to plug into a slot. 
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
        
        // \sum_{a \in S_i} x(r, q, k, i, a) = y(r, q, k)
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
            
            idY = poolVariables.get(DivVariablePool.VAR_Y, r, q, k, 0, 0).getId();
            
            for (int i = 0; i < desc.getNumberOfSlots(k); i++) {            
            
                expr = cplex.linearNumExpr();
                expr.addTerm(-1, cplexVar.get(idY));
                
                for (Index index : desc.getIndexesAtSlot(k, i)) { 
                    idX = poolVariables.get(VAR_X, r, q, k, i, index.getId()).getId();                            
                    expr.addTerm(1, cplexVar.get(idX));
                }
                
                cplex.addEq(expr, 0, "atomic_constraint_" + numConstraints);
                numConstraints++;
                
            }
        }        
    }

    /**
     * Construct the set of atomic constraints.
     * 
     * @throws IloException
     */
    protected void usedIndexConstraints() throws IloException
    {
        for (int r = 0; r < nReplicas; r++)
            for (QueryPlanDesc desc : queryPlanDescs) 
                usedIndexConstraints(r, desc.getStatementID(), desc);
    }
    
    /**
     *  
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
    protected void usedIndexConstraints(int r, int q, QueryPlanDesc desc) throws IloException
    {
        IloLinearNumExpr expr;
        int idX;
        int idS;
        
        // used index
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
            for (int i = 0; i < desc.getNumberOfSlots(k); i++)   
                for (Index index : desc.getIndexesAtSlot(k, i)) {
                    
                    idX = poolVariables.get(VAR_X, r, q, k, i, index.getId()).getId();
                    idS = poolVariables.get(VAR_S, r, 0, 0, 0, index.getId()).getId();
                    
                    expr = cplex.linearNumExpr();
                    expr.addTerm(1, cplexVar.get(idX));
                    expr.addTerm(-1, cplexVar.get(idS));
                    cplex.addLe(expr, 0, "index_present_" + numConstraints);
                    numConstraints++;
                    
                }
    }

    /**
     * Load-balance factor constraint
     * 
     * @throws IloException
     *      If there is error in formulating the expression in CPLEX. 
     * 
     */
    protected void loadBalanceFactorConstraints() throws IloException
    {
        for (QueryPlanDesc desc : queryPlanDescs) {
            if (desc.getSQLCategory().isSame(INSERT) || desc.getSQLCategory().isSame(DELETE))
                continue;
            loadBalanceFactorConstraints(desc);
        }
    }
    
    /**
     * 
     * 
     */
    protected void loadBalanceFactorConstraints(QueryPlanDesc desc) throws IloException
    {   
        IloLinearNumExpr expr; 
        int idY;        
        int q;
        int rhs;
        
        q = desc.getStatementID();
        expr = cplex.linearNumExpr();
            
        for (int r = 0; r < nReplicas; r++)
            for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
                idY = poolVariables.get(VAR_Y, r, q, k, 0, 0).getId();
                expr.addTerm(1, cplexVar.get(idY));
            }
            
        rhs = (desc.getSQLCategory().isSame(UPDATE)) ? nReplicas : loadfactor;
        cplex.addEq(expr, rhs, "top_m_query_" + q);            
        numConstraints++;
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
        for (int r = 0; r < nReplicas; r++)
            spaceConstraints(r);
    }
    
    /**
     * Impose space constraint on the materialized indexes at the given window.
     * 
     * @throws IloException
     *      If there is error in formulating the expression in CPLEX. 
     * 
     */
    protected void spaceConstraints(int r) throws IloException
    {   
        IloLinearNumExpr expr; 
        int idS;       
        
        expr = cplex.linearNumExpr();
            
        for (Index index : candidateIndexes) {  
            // not consider FTS index
            if (!(index instanceof FullTableScanIndex)) {
                idS = poolVariables.get(VAR_S, r, 0, 0, 0, index.getId()).getId();                
                expr.addTerm(index.getBytes(), cplexVar.get(idS));
            }
        }
            
        cplex.addLe(expr, B, "space_" + numConstraints);
        numConstraints++;
    }
    
    /**
     * Retrieve the update cost, computed by INUM including the cost for the query shell
     * and the update indexes (NOT including the cost to update base tables)
     * 
     * @return
     *      The update cost
     */
    public double getUpdateCostFromCplex() throws Exception
    {   
       double cost = 0.0;
       IloLinearNumExpr expr;
       
       for (int r = 0; r < nReplicas; r++)
           for (QueryPlanDesc desc : super.queryPlanDescs) {

               if (!desc.getSQLCategory().isSame(NOT_SELECT))
                   continue;
               
               expr = cplex.linearNumExpr();
               
               // query shell of UPDATE statement
               if (desc.getSQLCategory().isSame(UPDATE))
                   expr = modifyCoef(queryExpr(r, desc.getStatementID(), desc), 
                           getFactorStatement(desc) * desc.getStatementWeight()
                           );
               
               // update index access cost
               expr.add(modifyCoef(indexUpdateCost(r, desc.getStatementID(), desc),
                       desc.getStatementWeight()));
               
               cost += computeVal(expr);
           }
       
       return cost;
    }
    
    
    /**
     * Retrieve the max imbalance replica cost
     * 
     * @return
     *      The imbalance factor
     * @throws Exception
     */
    public double getTotalCost() throws Exception
    {
        double total = 0.0;
        
        for (int r = 0; r < nReplicas; r++) 
            total += computeVal(replicaCost(r));
        
        return total;
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
                totalBaseTableUpdateCost += 
                        (desc.getBaseTableUpdateCost() * desc.getStatementWeight());
            
        // need to time the number of replica
        return totalBaseTableUpdateCost * nReplicas;
    }
    
    /**
     * Reads the value of variables corresponding to the presence of indexes
     * at each replica and returns indexes to materialize at each replica.
     * 
     * @return
     *      List of indexes to be materialized at each replica.
     *       
     */
    @Override
    protected IndexTuningOutput getOutput() throws Exception 
    {
        DivConfiguration conf = new DivConfiguration(nReplicas, loadfactor);
        
        // Iterate over variables s^r_{i,w}
        /*
        for (IloNumVar cVar : cplexVar) {
            if (cplex.getValue(cVar) > 0) {
                DivVariable var = (DivVariable) poolVariables.get(cVar.getName());
                if (var.getType() == VAR_S) {
                    Index index = mapVarSToIndex.get(var.getName());
                    if (!(index instanceof FullTableScanIndex))
                        conf.addIndexReplica(var.getReplica(), index);
                }
            }
        }  
        */
        
        for (int r = 0; r < nReplicas; r++) 
            findRecommendedIndexes (r, conf);
        
        // {@code y} variables
        for (QueryPlanDesc desc : queryPlanDescs) {
            // only distribute queries to replicas
            // all updates are routed to every replica
            if (desc.getSQLCategory().equals(NOT_SELECT))
                continue;
            
            routeQuery(desc, conf);
        }
        
        return conf;
    }
    
    /**
     * Derive the recommended indexes at the given replica
     * @param r
     * @param conf
     */
    protected void findRecommendedIndexes(int r, DivConfiguration conf)
            throws Exception
    {
        int idS;
        
        for (Index index : candidateIndexes) {
            
            if ((index instanceof FullTableScanIndex))
                continue;
            
            idS = poolVariables.get(VAR_S, r, 0, 0, 0, index.getId()).getId();
            if (cplex.getValue(cplexVar.get(idS)) > 0) 
                conf.addIndexReplica(r, index);
            
        }
    }
    
    /**
     * Derive the recommended indexes at the given replica and 
     * update to the given configuration.
     * 
     * @param desc
     *     The query
     * @param conf
     *      The divergent configuration
     */
    protected void routeQuery(QueryPlanDesc desc, DivConfiguration conf)
        throws Exception
    {
        int idY;
        int q;
        q = desc.getStatementID();
        
        for (int r = 0; r < nReplicas; r++)
            for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
                idY = poolVariables.get(VAR_Y, r, q, k, 0, 0).getId();
                if (cplex.getValue(cplexVar.get(idY)) > 0) {
                    conf.routeQueryToReplica(q, r);
                    break;
                }
            }
    }
    
    /**
     * Retrieve the max imbalance replica cost
     * 
     * @return
     *      The imbalance factor
     * @throws Exception
     */
    public double getNodeImbalance() throws Exception
    {
        List<Double> replicas = new ArrayList<Double>();
        double cost;
        
        for (int r = 0; r < nReplicas; r++) {
            cost = computeVal(replicaCost(r));
            if (cost > 0.0)
                replicas.add(cost);
        }
        
        return maxRatioInList(replicas);
    }
    
    /**
     * Compute number of queries specialize at each replica
     *  
     * @return
     *  todo
     * @throws Exception
     */
    public List<Integer> computeNumberQueriesSpecializeForReplica() throws Exception
    {
        List<Integer> counts = new ArrayList<Integer>();
        List<Double> costs;
        
        int count;
        
        for (int r = 0; r < nReplicas; r++) {
            costs = getQueryCostReplicaByCplex(r);
            count = 0;
            for (int q = 0; q < queryPlanDescs.size(); q++) {
                if (queryPlanDescs.get(q).getSQLCategory().isSame(SELECT)) {
                    if (costs.get(q) > 0.1)
                        count++;
                }
            }
            
            counts.add(count);
        }
        
        return counts;
    }
    
    
    /**
     * Calculate the query costs at replica {@code r}.
     * 
     * @return
     *      The imbalance factor
     * @throws Exception
     */
    public List<Double> getQueryCostReplicaByCplex(int r) throws Exception
    {
        List<Double> queries = new ArrayList<Double>();
        
        IloLinearNumExpr expr;
        double val;
        
        for (QueryPlanDesc desc : super.queryPlanDescs) {
            
            expr = cplex.linearNumExpr();
            
            // query shell of SELECT or UPDATE statement
            if (desc.getSQLCategory().isSame(SELECT) ||
                    desc.getSQLCategory().isSame(UPDATE))
                expr = modifyCoef(queryExpr(r, desc.getStatementID(), desc),
                                desc.getStatementWeight());
            
            val = 0.0;
            
            // update index access cost
            if (desc.getSQLCategory().isSame(NOT_SELECT)) {
                expr.add(modifyCoef(indexUpdateCost(r, desc.getStatementID(), desc),
                        desc.getStatementWeight()));
                val += desc.getBaseTableUpdateCost() * desc.getStatementWeight();
            }
            
            val += computeVal(expr);
            queries.add(val);
        }
        
        return queries;
    }
    
    /**
     * Use for debugging purpose
     * @param r
     * @param q
     * @throws Exception
     */
    public void showStepComputeQuery(int r, int q) throws Exception
    {
        IloLinearNumExpr expr;        
        expr = cplex.linearNumExpr();
        
        QueryPlanDesc desc = super.queryPlanDescs.get(q);
        Rt.p("Query plan description: " + desc.getStatementID() 
                + "\n" + desc);
        
        // query shell of SELECT or UPDATE statement
        if (desc.getSQLCategory().isSame(SELECT) ||
                desc.getSQLCategory().isSame(UPDATE))
            expr = modifyCoef(queryExpr(r, desc.getStatementID(), desc),
                            desc.getStatementWeight());
        
        // update index access cost
        if (desc.getSQLCategory().isSame(NOT_SELECT)) {
            expr.add(modifyCoef(indexUpdateCost(r, desc.getStatementID(), desc),
                    desc.getStatementWeight()));
        }
        
        showComputeVal(expr);
    }
}
