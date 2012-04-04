package edu.ucsc.dbtune.bip.div;


import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_U;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_SUM_Y;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_COMBINE_X;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_COMBINE_Y;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_X;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_Y;

import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_XO;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_YO;

import static edu.ucsc.dbtune.workload.SQLCategory.NOT_SELECT;

import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;
import ilog.cplex.IloCplex.DoubleParam;

import java.util.ArrayList;

import java.util.HashSet;
import java.util.List;
import java.util.Set;


import edu.ucsc.dbtune.bip.core.QueryPlanDesc;

import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Sets;

public class ConstraintDivBIP extends DivBIP
{
    public static int IMBALANCE_REPLICA = 1001;
    public static int IMBALANCE_QUERY   = 1002;
    public static int NODE_FAILURE      = 1003;
   
    protected boolean isSumYConstraint;
    protected List<DivConstraint> constraints;
    protected QueryCostOptimalBuilder queryOptimalBuilder;
    
    public ConstraintDivBIP(List<DivConstraint> constraints)
    {  
        isSumYConstraint = false;
        this.constraints = constraints;
    }
    
    @Override
    protected void buildBIP() 
    {
        numConstraints = 0;
        
        try {            
            // time limit
            cplex.setParam(DoubleParam.TiLim, 300);
            UtilConstraintBuilder.cplex = cplex;
            
            // 1. Add variables into list
            constructVariables();
            createCplexVariable(poolVariables.variables());
            
            // 2. Construct the query cost at each replica
            super.totalCost();
            
            // 3. Atomic constraints
            super.atomicConstraints();      
            
            // 4. Top-m best cost 
            super.topMBestCostConstraints();
            
            // 5. Space constraints
            super.spaceConstraints();
            
            // 6. if we have additional constraints
            // need to impose top-m best cost constraints
            if (constraints.size() > 0) {
                // initial auxiliaries classes
                queryOptimalBuilder = new QueryCostOptimalBuilder(cplex, cplexVar, poolVariables);
                UtilConstraintBuilder.cplexVar = cplexVar;
                topMBestCostExplicit();
            }
            
            // 7. additional constraints
            for (DivConstraint c : constraints) {
                if (c.getType() == IMBALANCE_REPLICA)
                    imbalanceReplicaConstraints(c.getFactor());
                else if (c.getType() == NODE_FAILURE)
                    nodeFailures(c.getFactor());
                else if (c.getType() == IMBALANCE_QUERY)
                    imbalanceQueryConstraints(c.getFactor());
            }
        }     
        catch (IloException e) {
            e.printStackTrace();
        }
    }
    
    @Override
    protected void constructVariables() throws IloException
    {   
        super.constructVariables();
        
        // variable for each query descriptions
        for (int r = 0; r < nReplicas; r++)
            for (QueryPlanDesc desc : queryPlanDescs) {
                constructConstraintVariables(r, desc.getStatementID(), desc);
                constructBinaryProductVariables(r, desc.getStatementID(), desc);
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
    protected void constructConstraintVariables(int r, int q, QueryPlanDesc desc)
    {       
        // YO: y for local optimal
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
            poolVariables.createAndStore(VAR_YO, r, q, k, 0);
            
        // XO: x for local optimal
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
            for (int i = 0; i < desc.getNumberOfSlots(); i++)  
                for (Index index : desc.getIndexesAtSlot(i)) 
                    poolVariables.createAndStore(VAR_XO, r, q, k, index.getId());
        
        // U variables for the local optimal
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
            for (int i = 0; i < desc.getNumberOfSlots(); i++)  
                for (Index index : desc.getIndexesAtSlot(i)) 
                    poolVariables.createAndStore(VAR_U, r, q, k, index.getId());
        
        // variable sum_y(r, q) = \sum_{k = 1}^{K_q} y^r_{qk}
        poolVariables.createAndStore(VAR_SUM_Y, r, q, 0, 0);    
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
    protected void constructBinaryProductVariables(int r, int q, QueryPlanDesc desc)
    {
        // combined variables to handle product of two binary variables
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) 
            for (int r2 = 0; r2 < nReplicas; r2++)
                if (r2 != r)
                    poolVariables.createAndStore(VAR_COMBINE_Y, combineReplicaID(r, r2), 
                                                 q, k, 0);
            
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
            for (int i = 0; i < desc.getNumberOfSlots(); i++)  
                for (Index index : desc.getIndexesAtSlot(i)) 
                    for (int r2 = 0; r2 < nReplicas; r2++)
                        if (r2 != r)
                            poolVariables.createAndStore(VAR_COMBINE_X, combineReplicaID(r, r2),
                                                         q, k, index.getId());
    }
    
    /**
     * Impose the top m best cost explicit
     * 
     * @throws IloException
     */
    protected void topMBestCostExplicit() throws IloException
    {   
        
        
        List<IloLinearNumExpr> exprOptimals;
        List<IloLinearNumExpr> exprQueries;

        for (QueryPlanDesc desc : queryPlanDescs) {
            
            exprOptimals = new ArrayList<IloLinearNumExpr>();
            exprQueries = new ArrayList<IloLinearNumExpr>();
            
            for (int r = 0; r < nReplicas; r++) {
                exprOptimals.add(queryOptimalBuilder.queryExprOptimal
                                                    (r, desc.getStatementID(), desc));
                exprQueries.add(super.queryExpr(r, desc.getStatementID(), desc));   
                
                // query cost optimal constraints
                queryOptimalBuilder.optimalConstraints(r, desc.getStatementID(), desc);
            }
            
            topMBestCostExplicit(desc, exprQueries, exprOptimals);
        }
    }
    
    /**
     * Impose the constrains for top-m best cost explicitly
     * <p>
     * <ol> Each query cost, cost(q,r) does not exceed the local optimal cost, and 
     * <ol> the sum of query cost across replicas does not exceed the sum of any 
     * {@code m} elements of {@code cost_opt(q,r)}
     * </p>
     * 
     * @param exprCrossReplica
     *      The query cost formula over all replicas
     * @param listQueryOptimal
     *      The list of optimal query cost formulas over all replicas
     * 
     * @throws IloException
     */
    protected void topMBestCostExplicit(QueryPlanDesc desc,
                                        List<IloLinearNumExpr> listQueryCost,
                                        List<IloLinearNumExpr> listQueryOptimal) 
                   throws IloException
    {
        IloLinearNumExpr expr;
        IloLinearNumExpr exprCrossReplica;
        IloLinearNumExpr exprTopm;
        
        int numQuery = listQueryCost.size();
        
        // sum of query q over all replicas
        exprCrossReplica = cplex.linearNumExpr();
        
        for (int i = 0; i < numQuery; i++) {
            expr = cplex.linearNumExpr();
            expr.add(listQueryCost.get(i));
            
            expr.add(UtilConstraintBuilder.modifyCoef(listQueryOptimal.get(i), -1)); 
            cplex.addLe(expr, 0, "query_optimal" + numConstraints);
            numConstraints++;
     
            // sum of q over all replicas
            exprCrossReplica.add(listQueryCost.get(i));
        }
        
        // if the query shell from update, DO NOT need to have explicit 
        // top-m best cost constraints
        if (desc.getSQLCategory().isSame(NOT_SELECT))
            return;
        
        // enumerate all subset of size m
        Set<Integer> positions = new HashSet<Integer>();
        for (int i = 0; i < numQuery; i++)
            positions.add(i);
        
        Sets<Integer> s = new Sets<Integer>();
        Set<Set<Integer>> positionSizeM = s.powerSet(positions, super.loadfactor);
        
        for (Set<Integer> position : positionSizeM) {
            
            exprTopm = cplex.linearNumExpr();
            exprTopm.add(exprCrossReplica);
            
            for (Integer p : position)
                exprTopm.add(UtilConstraintBuilder.modifyCoef(listQueryOptimal.get(p), -1));
            
            cplex.addLe(exprTopm, 0, "top_m_explicit_" + numConstraints);
            numConstraints++;
        }
    }
    
    /**
     * Impose the imbalance constraints among replicas
     */
    protected void imbalanceReplicaConstraints(double beta) throws IloException
    {   
        List<IloLinearNumExpr> exprs = new ArrayList<IloLinearNumExpr>();
        
        for (int r = 0; r < nReplicas; r++) 
            exprs.add(super.replicaCost(r));
        
        // for each pair of replicas, impose the imbalance factor constraint 
        for (int r1 = 0; r1 < nReplicas - 1; r1++)
            for (int r2 = r1 + 1; r2 < nReplicas; r2++) 
                UtilConstraintBuilder.imbalanceConstraint(exprs.get(r1), exprs.get(r2), beta);
    }
    
    
    
    
    /**
     * Construct a set of imbalance query constraints. That is, the cost of top-m best place
     * for every query does not exceed beta times.
     * 
     * @param beta
     *      The query imbalance factor
     * @throws IloException 
     */
    protected void imbalanceQueryConstraints(double beta) throws IloException
    {   
        // sum_y = sum of y^j_{qk} over all template plans
        for (int r = 0; r < nReplicas; r++)
            for (QueryPlanDesc desc : queryPlanDescs)
                sumYConstraint(r, desc.getStatementID(), desc);
        
        // query imbalance constraint
        for (QueryPlanDesc desc : queryPlanDescs)
            for (int r1 = 0; r1 < nReplicas; r1++)
                for (int r2 = r1 + 1; r2 < nReplicas; r2++)
                    imbalanceQueryConstraint(desc, r1, r2, beta);
            
    }
    
    /**
     * Constraint the variable of type {@code VAR_SUM_Y} to be the summation of corresponding variables
     * of type {@code VAR_Y}
     * 
     * @param r
     *      The replica ID
     * @param q
     *      The query ID
     * @param desc
     *      The query plan description
     *      
     * @throws IloException
     */
    protected void sumYConstraint(int r, int q, QueryPlanDesc desc) throws IloException
    {
        // the constraint has been implemented before
        if (isSumYConstraint)
            return;
        
        int idY;
        int idSumY;
        
        idSumY = poolVariables.get(VAR_SUM_Y, r, q, 0, 0).getId();
        IloLinearNumExpr expr = cplex.linearNumExpr();
        expr.addTerm(-1, cplexVar.get(idSumY));
        
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
            
            idY = poolVariables.get(VAR_Y, r, q, k, 0).getId();
            expr.addTerm(1, cplexVar.get(idY));
        }
        
        cplex.addEq(expr, 0, "sum_y" + numConstraints);
        numConstraints++;
        // mark this constraint has been implemented
        isSumYConstraint = true;
    }
    
    /**
     * Build the imbalance query constraint for the given query w.r.t. two replicas
     * 
     * @param desc
     *      The query on which we impose the imbalance constraint
     * @param r1
     *      The ID of the first replica
     * @param r2
     *      The ID of the second replica
     * @throws IloException 
     */
    protected void imbalanceQueryConstraint(QueryPlanDesc desc, int r1, int r2, double beta) 
             throws IloException
    {  
        int q = desc.getStatementID();
        
        // constraints for the combine variables
        constraintCombineVariable(desc, r1, r2, q);
        constraintCombineVariable(desc, r2, r1, q);
        
        // query imbalance constraint
        imbalanceQueryConstraintOrder(desc, r1, r2, beta);
        imbalanceQueryConstraintOrder(desc, r2, r1, beta);         
    }
    
    /**
     * Implement the constraint for combined variables defined on the given query plan description.
     * @param desc
     *  todo
     * @param r
     * @param q
     * @throws IloException 
     */
    protected void constraintCombineVariable(QueryPlanDesc desc, int r1, int r2, int q) 
                throws IloException
    {
        int idCombine;
        int idY;
        int idX;
        int idSumY;
        
        // Y2 (r1) 
        idSumY = poolVariables.get(VAR_SUM_Y, r2, q, 0, 0).getId();
        
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
            idCombine = poolVariables.get(VAR_COMBINE_Y, combineReplicaID(r1, r2), q, k, 0).getId();
            idY       = poolVariables.get(VAR_Y, r1, q, k, 0).getId();
            UtilConstraintBuilder.constraintCombineVariable(idCombine, idSumY, idY);
        }
        
        // Index access cost                            
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
            for (int i = 0; i < desc.getNumberOfSlots(); i++)
                for (Index index : desc.getIndexesAtSlot(i)) {
                    
                    idCombine = poolVariables.get(VAR_COMBINE_X, combineReplicaID(r1, r2), 
                                           q, k, index.getId()).getId();
                    idX       = poolVariables.get(VAR_X, r1, q, k, index.getId()).getId();
                    UtilConstraintBuilder.constraintCombineVariable(idCombine, idSumY, idX);
                }
    }
    
    
    
    /**
     * Build the imbalance query constraint for the given query w.r.t. two replicas in the order
     * 
     * @param desc
     *      The query on which we impose the imbalance constraint
     * @param r1
     *      The ID of the first replica
     * @param r2
     *      The ID of the second replica
     * @throws IloException 
     */
    protected void imbalanceQueryConstraintOrder(QueryPlanDesc desc, int r1, int r2, double beta) 
                   throws IloException
    {
        int q = desc.getStatementID();
        int idCombine;
        
        IloLinearNumExpr expr2; 
        
        expr2 = super.queryExpr(r2, q, desc);
        
        // Y2 * expr1 <= \beta * expr2
        IloLinearNumExpr expr = cplex.linearNumExpr();
        
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {                    
            idCombine = poolVariables.get(VAR_COMBINE_Y, combineReplicaID(r1, r2), q, k, 0).getId();
            expr.addTerm(desc.getInternalPlanCost(k), cplexVar.get(idCombine));                    
        }                
                    
        // Index access cost                            
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
            for (int i = 0; i < desc.getNumberOfSlots(); i++)
                for (Index index : desc.getIndexesAtSlot(i)) {
                    idCombine = poolVariables.get(VAR_COMBINE_X, combineReplicaID(r1, r2), 
                                           q, k, index.getId()).getId();
                    expr.addTerm(desc.getAccessCost(k, index), cplexVar.get(idCombine));
                }   
        
        // add -beta time of expr2 
        // get iterator over exprs
        expr.add(UtilConstraintBuilder.modifyCoef(expr2, -beta));
        cplex.addLe(expr, 0, "imbalance_query_" + numConstraints);
        numConstraints++;
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
        return (id * 1000 + failID);
    }
    
    /**
     * Impose the imbalance constraints when any one of the replicas fail.
     * 
     * @param beta
     *      The imbalance factor,
     *      
     * @throws IloException
     */
    protected void nodeFailures(double beta) throws IloException
    {
        for (int r = 0; r < nReplicas; r++)
            for (QueryPlanDesc desc : queryPlanDescs)
                sumYConstraint(r, desc.getStatementID(), desc);
        
        for (int failR = 0; failR < nReplicas; failR++)
            nodeFailureConstraint(failR, beta);
        
    }
    
    
    /**
     * Construct the set of imbalance factor when the replica with the ID {@code failR} fails.
     * 
     * @param failR
     *      The fail replica ID.
     * @param beta
     *      The imbalance factor.
     *      
     * @throws IloException
     */
    protected void nodeFailureConstraint(int failR, double beta) throws IloException
    {
        IloLinearNumExpr expr;
        List<IloLinearNumExpr> exprs = new ArrayList<IloLinearNumExpr>();
        
        for (int r = 0; r < nReplicas; r++)
            if (r != failR) {
                
                expr = cplex.linearNumExpr();
                for (QueryPlanDesc desc : queryPlanDescs)
                    expr.add(increadLoadQuery(r, desc.getStatementID(), desc, failR));
                
                exprs.add(expr);
            }
        
        for (int r1 = 0; r1 < exprs.size() - 1; r1++)
            for (int r2 = r1 + 1; r2 < exprs.size(); r2++)
                UtilConstraintBuilder.imbalanceConstraint(exprs.get(r1), exprs.get(r2), beta);
    }
    
    /**
     * Compute the increase in the load of processing query {@code d} at the given replica {@code r}
     * assuming that replica with the identifier {@code failR} has been failed.
     *  
     * @param r
     *      The replica on which the statement is defined
     * @param q
     *      The query ID
     * @param desc
     *      The query plan description.
     * @param failR
     *      The ID of the replica that is assumed to fail
     *           
     * @return
     *      The increase load expression
     */
    protected IloLinearNumExpr increadLoadQuery(int r, int q, QueryPlanDesc desc, int failR)
              throws IloException
    {
        int idX;
        int idY;
        int idSumY;
        int idCombine;
        
        // we need to impose constraints for the combined variables
        // the load of query desc is distributed to the remaining
        // only if failR is among top-m best places of processing q.
        // 
        // SUM_Y(q, failR) > 0
        
        idSumY = poolVariables.get(VAR_SUM_Y, failR, q, 0, 0).getId();
        
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
            
            idCombine = poolVariables.get(VAR_COMBINE_Y, combineReplicaID(r, failR), q, k, 0).getId();
            idY       = poolVariables.get(VAR_Y, r, q, k, 0).getId();
            UtilConstraintBuilder.constraintCombineVariable(idCombine, idSumY, idY);
        
        }
        
        // Index access cost                            
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
            for (int i = 0; i < desc.getNumberOfSlots(); i++)
                for (Index index : desc.getIndexesAtSlot(i)) {
                    
                    idCombine = poolVariables.get(VAR_COMBINE_X, combineReplicaID(r, failR), 
                                           q, k, index.getId()).getId();
                    idX       = poolVariables.get(VAR_X, r, q, k, index.getId()).getId();
                    UtilConstraintBuilder.constraintCombineVariable(idCombine, idSumY, idX);
                    
                }
        
        double factor = (double) 1 / (loadfactor * (loadfactor - 1));
        
        // the increase cost
        IloLinearNumExpr expr = cplex.linearNumExpr();
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {                    
            idCombine = poolVariables.get(VAR_COMBINE_Y, combineReplicaID(r, failR), q, k, 0).getId();
            expr.addTerm(factor * desc.getInternalPlanCost(k), cplexVar.get(idCombine));                    
        }                
                    
        // Index access cost                            
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
            for (int i = 0; i < desc.getNumberOfSlots(); i++)
                for (Index index : desc.getIndexesAtSlot(i)) {
                    idCombine = poolVariables.get(VAR_COMBINE_X, combineReplicaID(r, failR), 
                                           q, k, index.getId()).getId();
                    expr.addTerm(factor * desc.getAccessCost(k, index), cplexVar.get(idCombine));
                }    
        
        return expr;
    }
}
        