package edu.ucsc.dbtune.bip.div;


import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_S;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_U;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_XO;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_YO;
import static edu.ucsc.dbtune.bip.div.UtilConstraintBuilder.imbalanceConstraintOrder;
import static edu.ucsc.dbtune.bip.div.UtilConstraintBuilder.modifyCoef;
import static edu.ucsc.dbtune.workload.SQLCategory.DELETE;
import static edu.ucsc.dbtune.workload.SQLCategory.INSERT;
import static edu.ucsc.dbtune.workload.SQLCategory.NOT_SELECT;
import static edu.ucsc.dbtune.workload.SQLCategory.SELECT;
import static edu.ucsc.dbtune.workload.SQLCategory.UPDATE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.List;
import java.util.Set;

import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.inum.FullTableScanIndex;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.workload.SQLStatement;
import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;
import ilog.concert.IloNumVar;

/**
 * A robust divergent advisor takes node-imbalance constraints into account
 * and optimize the new load when one replica fails as well
 * 
 * @author Quoc Trung Tran
 *
 */
public class RobustDivBIP extends DivBIP 
{
    protected double upperReplicaCost;
    protected double optimalTotalCost;
    
    protected double nodeFactor;
    protected double failureFactor;
    protected double coefWithoutFailure;
    protected double coefWithFailure;
    protected int newLoadFactor;
    protected boolean isGreedy;
    protected QueryCostOptimalBuilderGeneral queryOptimalBuilder;
    protected Map<String, Double> mapVarVal;
    
    
    /**
     * Retrieve probability of failure
     * 
     * @return
     *      The prob. of failure
     */
    public double getFailureFactor()
    {
        return failureFactor;
    }
    
    /**
     * Retrieve load imbalance factor
     * 
     * @return
     *      The load imbalance factor
     */
    public double getNodeFactor()
    {
        return nodeFactor;
    }
    
    /**
     * Constructor of a DIVBIP with probability of failure,
     * and the load imbalance factor 
     * 
     * @param optCost
     *      The optimal cost to be used for greedy solution of load imbalance constraint
     *      If this value is -1, it means that we utilize the exact solution
     * @param nodeFactor
     *      The node-imbalance factor           
     * @param failureFactor
     *      The failure-imbalance factor 
     *  
     */
    public RobustDivBIP(double optCost, double nodeFactor, double failureFactor)
    {
        this.optimalTotalCost = optCost;
        this.nodeFactor = nodeFactor;
        this.failureFactor = failureFactor;
        
        if (optCost < 0.0)
            isGreedy = false;
        else
            isGreedy = true;
    }
    
    
    @Override
    protected void buildBIP() 
    {
        numConstraints = 0;
        newLoadFactor = super.loadfactor;
        coefWithoutFailure = UtilConstraintBuilder.computeCoefCostWithoutFailure(failureFactor, super.nReplicas);
        coefWithFailure = UtilConstraintBuilder.computeCoefCostWithFailure(failureFactor, super.nReplicas);
                
        try {
            UtilConstraintBuilder.cplex = cplex;
            // 1. Add variables into list
            constructVariables();
            createCplexVariable(poolVariables.variables());
            
            // 2. Atomic constraints
            super.atomicConstraints();
            
            // 3. Used index constraints
            super.usedIndexConstraints();
            
            // 4. Top-m best cost 
            super.routingMultiplicityConstraints();
            
            // 5. Space constraints
            super.spaceConstraints();
           
            // 6. imbalance node factor
            if (nodeFactor > 0){
                if (isGreedy)
                    imbalanceReplicaGreedy(nodeFactor);
                else
                    imbalanceReplicaExact(nodeFactor);
            }
            
            // 7. failure Handler
            expectedCost();
        }
        catch (IloException e) {
            e.printStackTrace();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /*********************************************************
     * 
     *   Node imbalance constraint handlers
     * 
     * 
     **********************************************************/
    
    /**
     * Exact solution to handle imbalance constraints
     * 
     */
    protected void imbalanceReplicaExact(double factor) throws Exception
    {
        
        // Constraint for cost(q, r) corresponds to the optimal
        // execution cost of q on replica r
        boolean isApproximation = true;
        queryOptimalBuilder = new QueryCostOptimalBuilderGeneral(cplex, cplexVar, 
                poolVariables, isApproximation);
        UtilConstraintBuilder.cplexVar = cplexVar;
        
        for (QueryPlanDesc desc : queryPlanDescs) {
            if (desc.getSQLCategory().isSame(INSERT) || desc.getSQLCategory().isSame(DELETE))
                continue;
            for (int r = 0; r < nReplicas; r++)  
                queryOptimalBuilder.optimalConstraints(r, desc.getStatementID(), desc);
        }
        
        // imbalance constraints
        List<IloLinearNumExpr> exprs = new ArrayList<IloLinearNumExpr>();
        
        for (int r = 0; r < nReplicas; r++) 
            exprs.add(super.replicaCost(r));
        
        // let the load of nreplicas in the order
        // of 1, 2, ..., nReplicas;
        for (int r = 0; r < nReplicas - 1; r++)
            imbalanceConstraintOrder(exprs.get(r), exprs.get(r + 1), 1.00);
        
        // load(nReplicas - 1) <= factor * load(1)
        imbalanceConstraintOrder(exprs.get(nReplicas -1), 
                                exprs.get(0), factor);
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
    protected void constructConstraintVariables(int r, int q, QueryPlanDesc desc)
    { 
        boolean isFTS;
        // U variables for the local optimal
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
            for (int i = 0; i < desc.getNumberOfSlots(k); i++) {
                isFTS = false;
                for (Index index : desc.getIndexesAtSlot(k, i)) {
                    if (index instanceof FullTableScanIndex)
                        isFTS = true;
                    poolVariables.createAndStore(VAR_U, r, q, k, i, index.getId());
                }
                
                // We need a variable for FTS of type VAR_U
                // if FTS has not appeared in this slot
                if (!isFTS){
                    Index fts = desc.getFTSAtSlot(k, i);
                    poolVariables.createAndStore(VAR_U, r, q, k, i, fts.getId());
                }
            }    
    }
    
    /**
     * The greedy solution to solve imbalance constraint
     * 
     * @param factor
     *      Load imbalance factor
     *      
     * @throws Exception
     */
    protected void imbalanceReplicaGreedy(double factor) throws Exception
    {
        deriveUpperLoadReplica(factor);
        
        for (int r = 0; r < nReplicas; r++)
            upperReplicaConstraint(r);
    } 
    
    /**
     * Derive an upper bound on the all of each replica
     * (in the greedy solution)
     * 
     * @param r
     *      The replica ID
     * @throws IloException
     */
    protected void upperReplicaConstraint(int r) throws IloException
    {   
        IloLinearNumExpr expr = replicaCost(r);
        cplex.addLe(expr, upperReplicaCost, "upper_replica_" + numConstraints);
        numConstraints++;
    }
    
    /**
     * Derive the upper value for load replica
     * @param factor
     */
    protected void deriveUpperLoadReplica(double factor)
    {
        double alpha;
        alpha = (factor - 1) / (1 + (nReplicas - 1) * factor);
        // If alpha is less 10% ---> adjust to 10%
        // allow a bit more tight constraints
        if (alpha < 0.1)
            alpha = 0.1;
        
        upperReplicaCost = (1 + alpha) * optimalTotalCost / nReplicas;
        Rt.p("alpha value = " + alpha
                + " upper cost = " + upperReplicaCost);
    }
    
    /******************************************************
     * 
     *   Failure handlers
     * 
     * 
     ******************************************************/
    
    /**
     * Handling the case with failure
     * 
     * @throws Exception
     */
    protected void expectedCost() throws Exception
    {   
        if (coefWithFailure > 0.0){
            // We take into consideration the case of failure
            
            // load factor failure
            for (int r = 0; r < nReplicas; r++)
                routingMultiplicityFailure(r);
            
            // atomic constraint
            for (int r = 0; r < nReplicas; r++)
                atomicFailureConstraints(r);
            
            // used index
            for (int r = 0; r < nReplicas; r++)
                usedIndexFailureConstraints(r);
        }
        
        // Total cost
        totalCostFailure();
    }
    
    /**
     * Minimize the objective function: 
     *      (1 - alpha)^N * TotalCost + 
     *          alpha * (1-alpha)^{N-1} * TotalCostFailure
     * 
     * @throws Exception
     */
    protected void totalCostFailure() throws Exception
    {
        IloLinearNumExpr objTot = cplex.linearNumExpr();
        IloLinearNumExpr objFailure = cplex.linearNumExpr();
        IloLinearNumExpr obj = cplex.linearNumExpr();
        
        for (int r = 0; r < nReplicas; r++) { 
            objTot.add(replicaCost(r));
            if (coefWithFailure > 0.0)
                objFailure.add(totalCostForFailure(r));
        }
        
        // add coefficient for cost without failure
        obj.add(modifyCoef(objTot, coefWithoutFailure));
        // add coefficient for cost with failure
        if (coefWithFailure > 0.0)
            obj.add(modifyCoef(objFailure, coefWithFailure / nReplicas));
        Rt.p(" without failure coef = " + coefWithoutFailure
                + " with failure = " + coefWithFailure / nReplicas);
        cplex.addMinimize(obj); 
    }
    
    /**
     * Construct the set of atomic constraints.
     * 
     * @throws IloException
     */
    protected void atomicFailureConstraints(int failR) throws IloException
    {
        for (int r = 0; r < nReplicas; r++) {
            if (r == failR)
                continue;
            
            for (QueryPlanDesc desc : queryPlanDescs) {
                internalAtomicFailureConstraint(r, desc.getStatementID(), desc, failR);
                slotAtomicFailureConstraints(r, desc.getStatementID(), desc, failR);
            }
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
    protected void internalAtomicFailureConstraint(int r, int q, QueryPlanDesc desc, int failR) throws IloException
    {
        IloLinearNumExpr expr;
        int idY;
       
        // \sum_{k \in [1, Kq]}y^{r}_{qk} <= 1
        expr = cplex.linearNumExpr();
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
            idY = poolVariables.get(VAR_YO, combineReplicaID(r, failR), q, k, 0, 0).getId();
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
    protected void slotAtomicFailureConstraints(int r, int q, QueryPlanDesc desc, int failR) throws IloException
    {
        IloLinearNumExpr expr;
        int idY;
        int idX;
        
        // \sum_{a \in S_i} x(r, q, k, i, a) = y(r, q, k)
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
            
            idY = poolVariables.get(DivVariablePool.VAR_YO, combineReplicaID(r,failR), q, k, 0, 0).getId();
            
            for (int i = 0; i < desc.getNumberOfSlots(k); i++) {            
            
                expr = cplex.linearNumExpr();
                expr.addTerm(-1, cplexVar.get(idY));
                
                for (Index index : desc.getIndexesAtSlot(k, i)) { 
                    idX = poolVariables.get(VAR_XO, combineReplicaID(r, failR), q, k, i, index.getId()).getId();                            
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
    protected void usedIndexFailureConstraints(int failR) throws IloException
    {
        for (int r = 0; r < nReplicas; r++) {
            if (r == failR)
                continue;
            
            for (QueryPlanDesc desc : queryPlanDescs) 
                usedIndexFailureConstraints(r, desc.getStatementID(), desc, failR);
        }
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
    protected void usedIndexFailureConstraints(int r, int q, QueryPlanDesc desc, int failR) throws IloException
    {
        IloLinearNumExpr expr;
        int idX;
        int idS;
        
        // used index
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
            for (int i = 0; i < desc.getNumberOfSlots(k); i++)   
                for (Index index : desc.getIndexesAtSlot(k, i)) {
                    
                    idX = poolVariables.get(VAR_XO, combineReplicaID(r, failR), q, k, i, index.getId()).getId();
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
    protected void routingMultiplicityFailure(int failR) throws IloException
    {
        for (QueryPlanDesc desc : queryPlanDescs) {
            if (desc.getSQLCategory().isSame(INSERT) || desc.getSQLCategory().isSame(DELETE))
                continue;
            routingMultiplicityFailure(desc, failR);
        }
    }
    
    /**
     * Impose the routing-multiplicity constraint for the failure
     * component of the expected cost
     * 
     * @param desc
     *      The query description
     * @param failR
     *      The failed replica     
     *      
     */
    protected void routingMultiplicityFailure(QueryPlanDesc desc, int failR) throws IloException
    {   
        IloLinearNumExpr expr; 
        int idY;        
        int q;
        int rhs;
        
        q = desc.getStatementID();
        expr = cplex.linearNumExpr();
            
        for (int r = 0; r < nReplicas; r++) {
            if (r == failR)
                continue;
            
            for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
                idY = poolVariables.get(VAR_YO, combineReplicaID(r, failR), q, k, 0, 0).getId();
                expr.addTerm(1, cplexVar.get(idY));
            }
        }
        // Recall that one replica is unavailable
        rhs = (desc.getSQLCategory().isSame(UPDATE)) ? nReplicas - 1: newLoadFactor;
        cplex.addEq(expr, rhs, "routing_multiplicity_" + q);            
        numConstraints++;
    }
    
    
    /**
     * Construct the total cost assuming replica {@code failR} fails,
     * with the same divergent design and load-balance factor m - 1
     * 
     * @param failR
     *      The failure
     *      
     * @return
     *      The new load 
     */
    protected IloLinearNumExpr totalCostForFailure(int failR) throws Exception
    {
        IloLinearNumExpr obj = cplex.linearNumExpr();
        
        for (int r = 0; r < nReplicas; r++)
            if (r != failR)
                obj.add(replicaCostFailure(r, failR));
        
        return obj;
    }
    
    /**
     * The cost at replica {@code r} when replica {@failR} fails
     * 
     * @param r
     *      The alive replica
     * @param failR
     *      The failed replica
     * @return
     *      The cost at the alive replica
     *      
     * @throws Exception
     */
    protected IloLinearNumExpr replicaCostFailure(int r, int failR) throws Exception
    {
        IloLinearNumExpr expr = cplex.linearNumExpr();
        
        // query component
        // only applicable for SELECT and UPDATE statements
        for (QueryPlanDesc desc : queryPlanDescs) 
            if (desc.getSQLCategory().isSame(SELECT) || desc.getSQLCategory().isSame(UPDATE))
                expr.add(modifyCoef(queryExprFailure(r, desc.getStatementID(), desc, failR), 
                                   getFactorStatementFailure(desc) * desc.getStatementWeight()
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
    protected double getFactorStatementFailure(QueryPlanDesc desc)
    {
        return (desc.getSQLCategory().isSame(NOT_SELECT)) ? 
                1.0 :  (double) 1 / newLoadFactor;
    }
    
    /**
     * Formulate the expression of a query on a particular replica {@code r}
     * assuming that replica {@code failR} fails
     * 
     * @param r
     *      The replica ID
     * @param q
     *      The query ID
     * @param desc
     *      The query plan description.
     * @param failR
     *      The failure
     *           
     * @return
     *      The linear expression of the query (NOT take into account the frequency)
     *      
     * @throws IloException
     */
    protected IloLinearNumExpr queryExprFailure(int r, int q, QueryPlanDesc desc, int failR) 
              throws IloException
    {
        int id;
        double cost;
        IloLinearNumExpr expr = cplex.linearNumExpr();
        
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {                    
            id = poolVariables.get(VAR_YO, combineReplicaID(r, failR), q, k, 0, 0).getId();
            expr.addTerm(desc.getInternalPlanCost(k), cplexVar.get(id));                    
        }                
                    
        // Index access cost                            
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
            for (int i = 0; i < desc.getNumberOfSlots(k); i++)
                for (Index index : desc.getIndexesAtSlot(k, i)) {
                    id = poolVariables.get(VAR_XO, combineReplicaID(r, failR), q, k, i, index.getId()).getId();
                    cost = desc.getAccessCost(k, i, index);
                    
                    if (!Double.isInfinite(cost))
                        expr.addTerm(cost, cplexVar.get(id));
                }    
        
        return expr;
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
    protected void routeQueryUnderFailure(QueryPlanDesc desc, DivConfiguration conf, int failR)
                   throws Exception
    {
        int idY;
        int q;
        q = desc.getStatementID();
        
        for (int r = 0; r < nReplicas; r++) {
            
            if (r == failR)
                continue;
            
            for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
                idY = poolVariables.get(VAR_YO, combineReplicaID(r, failR), q, k, 0, 0).getId();
                if (cplex.getValue(cplexVar.get(idY)) > 0) {
                    conf.routeQueryToReplicaUnderFailure(q, r, failR);
                    break;
                }
            }
        }
    }
    
    /**
     * Extract the value-assignment by CPLEX
     * 
     * @return
     *      the map between variables' names their
     *      assignment
     */
    public Map<String, Double> extractBinaryVariableAssignment()
            throws Exception
    {
        Map<String, Double> mapVarVal = new HashMap<String, Double>();
        
        for (IloNumVar cVar : cplexVar)  
            mapVarVal.put(cVar.getName(), cplex.getValue(cVar));
        
        return mapVarVal;
    }
    
    
    @Override
    protected void constructVariables() throws IloException
    {   
        super.constructVariables();
        
        // variable for each query descriptions
        for (int r = 0; r < nReplicas; r++)
            for (QueryPlanDesc desc : queryPlanDescs) 
                constructVariablesForFailure(r, desc.getStatementID(), desc);
        
        // variable for each query descriptions
        if (!isGreedy) {
            for (int r = 0; r < nReplicas; r++)
                for (QueryPlanDesc desc : queryPlanDescs) 
                    constructConstraintVariables(r, desc.getStatementID(), desc);
        }
    }
    
    /**
     * 
     * Construct variables to handle node failures.
     * 
     * @param r
     *   Todo
     * @param q
     * @param desc
     */
    protected void constructVariablesForFailure(int r, int q, QueryPlanDesc desc)
    {
        // combined variables to handle product of two binary variables
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) 
            for (int r2 = 0; r2 < nReplicas; r2++)
                if (r2 != r)
                    poolVariables.createAndStore(VAR_YO, combineReplicaID(r, r2), 
                                                 q, k, 0, 0);
            
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
            for (int i = 0; i < desc.getNumberOfSlots(k); i++)  
                for (Index index : desc.getIndexesAtSlot(k, i)) 
                    for (int r2 = 0; r2 < nReplicas; r2++)
                        if (r2 != r)
                            poolVariables.createAndStore(VAR_XO, combineReplicaID(r, r2),
                                                         q, k, i, index.getId());
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
        DivConfiguration conf = (DivConfiguration) super.getOutput();
        
        if (coefWithFailure > 0.0){
            for (int r = 0; r < nReplicas; r++)
             // {@code y} variables
                for (QueryPlanDesc desc : queryPlanDescs) {
                    // only distribute queries to replicas
                    // all updates are routed to every replica
                    if (desc.getSQLCategory().equals(NOT_SELECT))
                        continue;
                    
                    routeQueryUnderFailure(desc, conf, r);
                }
        }
        
        return conf;
    }
    
    /**
     * Compute query cost (Note that for updates, we need to 
     * implement differently)
     * 
     */
    public double computeOptimizerCost() throws Exception
    {
        DivConfiguration conf = (DivConfiguration) getOutput();
        double costWithoutFailure, costWithFailure;
        
        costWithoutFailure = super.computeOptimizerCostWithoutFailure(conf);
        if (coefWithFailure > 0.0)
            costWithFailure = computeTotalOptimizerCostWithFailure(conf);
        else
            costWithFailure = 0.0;
        
        
        Rt.p(" cost without failure = " + costWithoutFailure
                + " WITH coef = " + coefWithoutFailure * costWithoutFailure);
        Rt.p(" cost WITH failure = " + costWithFailure
                + " WITH coef = " + 
                (double) (coefWithFailure * costWithFailure / nReplicas));
        
        return (coefWithoutFailure * costWithoutFailure
                + (double) (coefWithFailure * costWithFailure / nReplicas));
        
    }
    
    
    /**
     * Compute the cost in optimizer unit with failure
     * 
     * @param conf      
     *      The derived configuration
     *       
     * @return
     *      The cost
     * @throws Exception
     */
    protected double computeTotalOptimizerCostWithFailure(DivConfiguration conf)
                throws Exception
    {
        double costWithFailure = 0.0;
        for (int failR = 0; failR < nReplicas; failR++)
            costWithFailure += 
                    computeTotalOptimizerCostWithFailure (conf, failR);
        
        return costWithFailure;
    }
    
    /**
     * Cost of the system assuming that replica {@code failR} fails
     * 
     * @param conf
     *      The divergent design
     * @param failR
     *      The failure 
     * @return
     *      Total cost of the system assumping the given replica fails
     *      
     * @throws Exception
     */
    protected double computeTotalOptimizerCostWithFailure(DivConfiguration conf, int failR)
        throws Exception
    {   
        double cost;
        int counter;
        int q;        
        double totalCost;
        QueryPlanDesc desc;
        
        counter = -1;
        totalCost = 0.0;
        
        Set<Integer> all = new HashSet<Integer>();
        for (int r = 0; r < nReplicas; r++) {
            if (r != failR)
                all.add(r);
        }
        Set<Integer> partitions;
        
        for (SQLStatement sql : workload) {
            
            counter++;
            desc = queryPlanDescs.get(counter);
            q = desc.getStatementID();
            
            if (sql.getSQLCategory().equals(NOT_SELECT)) 
                partitions = all;
            else
                partitions = conf.getRoutingReplicaUnderFailure(q, failR); 
            
            for (int r : partitions) {
                cost = super.inumOptimizer.getDelegate().explain
                        (sql, conf.indexesAtReplica(r)).getTotalCost();
                cost = cost * this.getFactorStatementFailure(desc);
                totalCost += cost;
            }
            
            q++;
        }
        
        return totalCost;
    }
    

    /**
     * Construct a ``pseudo'' ID of a replica that is a combination of a replica with the identifier
     * {@code id} and a replica {@code failID} that is failed.
     * 
     * @param id
     *      The replica that is alive
     * @param failID
     *      The replica that fails.
     *      
     * @return
     *      The 'pseudo'' ID 
     */
    protected int combineReplicaID(int id, int failID)
    {
        return (id * 1001 + failID * 13);
    }
}
