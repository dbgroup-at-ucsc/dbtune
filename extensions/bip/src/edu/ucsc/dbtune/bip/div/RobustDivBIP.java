package edu.ucsc.dbtune.bip.div;


import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_S;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_XO;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_YO;
import static edu.ucsc.dbtune.bip.div.UtilConstraintBuilder.modifyCoef;
import static edu.ucsc.dbtune.workload.SQLCategory.DELETE;
import static edu.ucsc.dbtune.workload.SQLCategory.INSERT;
import static edu.ucsc.dbtune.workload.SQLCategory.NOT_SELECT;
import static edu.ucsc.dbtune.workload.SQLCategory.SELECT;
import static edu.ucsc.dbtune.workload.SQLCategory.UPDATE;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Rt;
import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;

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
    protected double coefTotalCost;
    protected int newLoadFactor;
    
    /**
     * todo
     * @param optCost
     * @param nodeFactor
     * @param coef
     */
    public RobustDivBIP(double optCost, double nodeFactor, double coef)
    {
        this.optimalTotalCost = optCost;
        this.nodeFactor = nodeFactor;
        this.coefTotalCost = coef;
    }
    
    public double getCoefTotalCost()
    {
        return this.coefTotalCost;
    }
    
    @Override
    protected void buildBIP() 
    {
        numConstraints = 0;
        newLoadFactor = super.loadfactor;
                
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
            super.loadBalanceFactorConstraints();
            
            // 5. Space constraints
            super.spaceConstraints();
            
            // 6. imbalance node factor
            imbalanceReplicaGreedy(nodeFactor);
            
            // 7. failure Handler
            failureHandler();
        }
        catch (IloException e) {
            e.printStackTrace();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
    protected void failureHandler() throws Exception
    {   
        if (this.coefTotalCost < 1.0){
            // load factor failure
            for (int r = 0; r < nReplicas; r++)
                loadFactorFailure(r);
            
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
     *      coef * TotalCost + (1 - coef) * TotalCostFailure
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
            if (coefTotalCost < 1.0)
                objFailure.add(totalCostForFailure(r));
        }
        
        // adjust the constant factor
        obj.add(modifyCoef(objTot, coefTotalCost));
        if (coefTotalCost < 1.0)
            obj.add(modifyCoef(objFailure, (1 - coefTotalCost) / nReplicas));
        
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

    
    protected void loadFactorFailure() throws Exception
    {
        
    }
    
    /**
     * Load-balance factor constraint
     * 
     * @throws IloException
     *      If there is error in formulating the expression in CPLEX. 
     * 
     */
    protected void loadFactorFailure(int failR) throws IloException
    {
        for (QueryPlanDesc desc : queryPlanDescs) {
            if (desc.getSQLCategory().isSame(INSERT) || desc.getSQLCategory().isSame(DELETE))
                continue;
            loadFactorFailure(desc, failR);
        }
    }
    
    /**
     * 
     * 
     */
    protected void loadFactorFailure(QueryPlanDesc desc, int failR) throws IloException
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
            
        rhs = (desc.getSQLCategory().isSame(UPDATE)) ? nReplicas : newLoadFactor;
        cplex.addEq(expr, rhs, "load_factor_" + q);            
        numConstraints++;
    }
    
    /**
     * todo
     * @param factor
     * @throws Exception
     */
    protected void imbalanceReplicaGreedy(double factor) throws Exception
    {
        deriveUpperLoadReplica(factor);
        
        for (int r = 0; r < nReplicas; r++)
            upperReplicaConstraint(r);
        
    } 
    
    /**
     * Set replica constraints
     * @param r
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
        
        upperReplicaCost = (1 + alpha) * optimalTotalCost / nReplicas;
        Rt.p("alpha value = " + alpha
                + " upper cost = " + upperReplicaCost);
    }
    
    /**
     * Construct the total cost assuming replica {@code failR} fails,
     * assuming the same divergent design and load-balance factor m - 1
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
    
    @Override
    protected void constructVariables() throws IloException
    {   
        super.constructVariables();
        
        // variable for each query descriptions
        for (int r = 0; r < nReplicas; r++)
            for (QueryPlanDesc desc : queryPlanDescs) 
                constructVariablesForFailure(r, desc.getStatementID(), desc);
        
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
