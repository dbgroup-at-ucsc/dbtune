package edu.ucsc.dbtune.bip.div;


import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_U;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_SUM_Y;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_COMBINE_X;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_COMBINE_Y;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_X;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_Y;


import static edu.ucsc.dbtune.workload.SQLCategory.NOT_SELECT;
import static edu.ucsc.dbtune.workload.SQLCategory.SELECT;
import static edu.ucsc.dbtune.workload.SQLCategory.INSERT;
import static edu.ucsc.dbtune.workload.SQLCategory.DELETE;
import static edu.ucsc.dbtune.workload.SQLCategory.UPDATE;

import static edu.ucsc.dbtune.bip.div.UtilConstraintBuilder.computeVal;
import static edu.ucsc.dbtune.bip.div.UtilConstraintBuilder.maxRatioInList;
import static edu.ucsc.dbtune.bip.div.UtilConstraintBuilder.modifyCoef;
import static edu.ucsc.dbtune.bip.div.UtilConstraintBuilder.imbalanceConstraint;
import static edu.ucsc.dbtune.bip.div.UtilConstraintBuilder.constantRHSImbalanceConstraint;

import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;


import java.util.ArrayList;

import java.util.HashSet;
import java.util.List;
import java.util.Set;


import edu.ucsc.dbtune.bip.core.QueryPlanDesc;

import edu.ucsc.dbtune.inum.FullTableScanIndex;
import edu.ucsc.dbtune.metadata.Index;
import edu.ucsc.dbtune.util.Rt;
import edu.ucsc.dbtune.util.Sets;

import static edu.ucsc.dbtune.util.EnvironmentProperties.QUERY_IMBALANCE;
import static edu.ucsc.dbtune.util.EnvironmentProperties.NODE_IMBALANCE;
import static edu.ucsc.dbtune.util.EnvironmentProperties.FAILURE_IMBALANCE;

public class ConstraintDivBIP extends DivBIP
{   
    protected boolean isApproximation;
    protected boolean isGreedy;
    protected List<DivConstraint> constraints;
    protected QueryCostOptimalBuilderGeneral queryOptimalBuilder;
    
    protected double upperReplicaCost;
    protected double upperNewLoad;
    protected double optimalTotalCost;
    
    
    public ConstraintDivBIP(final List<DivConstraint> constraints, final boolean isApproximation,
                            final boolean isGreedy)
    {  
        this.isApproximation  = isApproximation;
        this.isGreedy = isGreedy;
        this.constraints = constraints;
    }
    
    @Override
    protected void buildBIP() 
    {
        numConstraints = 0;
                
        try {
            UtilConstraintBuilder.cplex = cplex;
            
            // 1. Add variables into list
            constructVariables();
            createCplexVariable(poolVariables.variables());
            
            // 2. Construct the query cost at each replica
            super.totalCost();
            
            // 3. Atomic constraints
            super.atomicConstraints();
            
            // 4. Used index constraints
            super.usedIndexConstraints();
            
            // 5. Top-m best cost 
            super.routingMultiplicityConstraints();
            
            // 6. Space constraints
            super.spaceConstraints();
            
            // TOTAL cost constraint
            if (super.isUpperTotalCost)
                super.totalCostConstraint(super.upperTotalCost);
            
            // Greedy solution
            if (isGreedy){
                double defaultNodeFactor = 2.0;
                
                for (DivConstraint c : constraints) {
                    if (c.getType().equals(NODE_IMBALANCE)) {
                        imbalanceReplicaGreedy(c.getFactor());
                        defaultNodeFactor = c.getFactor();
                    }
                    else if (c.getType().equals(FAILURE_IMBALANCE))
                        failureGreedy(defaultNodeFactor, c.getFactor());
                }
                return;
            }
            
            
            // 6. if we have additional constraints
            // need to impose top-m best cost constraints
            if (constraints.size() > 0) {
                // initial auxiliaries classes
                queryOptimalBuilder = new QueryCostOptimalBuilderGeneral(cplex, cplexVar, 
                                                                  poolVariables, isApproximation);
                UtilConstraintBuilder.cplexVar = cplexVar;
                //topMBestCostExplicit();                
            }
            
            // this variable is turned on when 
            // the constraints contain IMBALANCE_QUERY or NODE_FAILURE
            boolean isSumYConstraint = false;
            
            for (DivConstraint c : constraints)
                if (c.getType().equals(QUERY_IMBALANCE) 
                      || c.getType().equals(FAILURE_IMBALANCE) ) {
                    isSumYConstraint = true;
                    break;
                }
            
            if (isSumYConstraint)
                sumYConstraint();
            
            // 7. additional constraints
            for (DivConstraint c : constraints) {
                if (c.getType().equals(NODE_IMBALANCE)) {
                    setConstantReplicaImbalanceConstraint(c.getFactor());
                    imbalanceReplicaConstraints(c.getFactor());
                }
                else if (c.getType().equals(FAILURE_IMBALANCE)) {
                    setConstantFailureImbalanceConstraint(c.getFactor());
                    nodeFailures(c.getFactor());
                }
                else if (c.getType().equals(QUERY_IMBALANCE)) {
                    setConstantQueryImbalanceConstraint();
                    imbalanceQueryConstraints(c.getFactor());                    
                }
            }
        }     
        catch (IloException e) {
            e.printStackTrace();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
    
    /**********************************************************
     * 
     * Greedy solutions methods
     * 
     * 
     **********************************************************/
    
    /**
     * Set the upper bound optimal cost: the cost that
     * any divergent design cannot exceed
     */
    public void setOptimalTotalCost(double cost)
    {
        this.optimalTotalCost = cost;
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
        Rt.p("L185: alpha value = " + alpha
                + " upper cost = " + upperReplicaCost);
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
        cplex.addLe(expr, upperReplicaCost, "replica_" + numConstraints);
        numConstraints++;
    }
    
    protected void failureGreedy(double nodeFactor, double failureFactor)
            throws Exception
    {
        UtilConstraintBuilder.cplexVar = cplexVar;
        Rt.p(" FAILURE greedy constraint , factor = " + failureFactor);
        deriveUpperLoadReplica (nodeFactor);
        double lowerReplica =  optimalTotalCost - (nReplicas - 1) * upperReplicaCost;
        upperNewLoad = lowerReplica * failureFactor;
        Rt.p(" upper new load = " + upperNewLoad
                + " upper old load = "
                + upperReplicaCost
                + " RATIO = " 
                + (upperNewLoad / upperReplicaCost)
                + " lower replica = " + lowerReplica
                + " load balance = " + (this.upperReplicaCost / lowerReplica));
        // Need to use SUM Y variables
        sumYConstraint();
        
        // Derive new load
        for (int failR = 0; failR < nReplicas; failR++) {
            failureGreedy(failR);
            break;
        }
    }
    
    /**
     * Assuming the given replica fails 
     * @param failR
     */
    protected void failureGreedy(int failR) throws Exception
    {
        for (IloLinearNumExpr expr : newLoads(failR)) {
            cplex.addLe(expr, upperNewLoad, 
                                "new_load_" + numConstraints);
            numConstraints++;
        }
    }
    
    
    
    /**********************************************************
     *
     * Exact solutions
     * 
     * 
     **********************************************************/
    
    
    /**
     * Sum Y constraint in general
     */
    protected void sumYConstraint() throws IloException
    {   
        // sum_y = sum of y^j_{qk} over all template plans
        for (int r = 0; r < nReplicas; r++)
            for (QueryPlanDesc desc : queryPlanDescs) {
                // only consider for SELECT statement
                if (desc.getSQLCategory().isSame(SELECT))
                    sumYConstraint(r, desc.getStatementID(), desc);
            }
    }
    
    /**
     * Construct the total cost formulas containing query statements only.
     * 
     * @throws IloException
     */
    protected void totalCostQueryOnly() throws IloException
    {
        IloLinearNumExpr expr = cplex.linearNumExpr();
       
        for (QueryPlanDesc desc : queryPlanDescs) { 
         
            if (desc.getSQLCategory().isSame(NOT_SELECT))
                continue;
            
            for (int r = 0; r < nReplicas; r++)
                expr.add(modifyCoef(queryExpr(r, desc.getStatementID(), desc), 
                         getFactorStatement(desc)));
        }
        
        cplex.addMinimize(expr);
    }
    
    
    /**
     * Each imbalance constraint is in the form: {cost(q,r_i) <= \alpha \times cost(q,r_j) + constant
     * where the constant is determined as the mininum of the internal plan cost 
     */
    protected void setConstantQueryImbalanceConstraint()
    {   
        double min = 999999999;
        
        for (QueryPlanDesc desc : queryPlanDescs) {
            
            if (desc.getSQLCategory().isSame(NOT_SELECT))
                continue;
            
            for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) 
                if (min > desc.getInternalPlanCost(k))
                    min = desc.getInternalPlanCost(k);
        }
        
        constantRHSImbalanceConstraint = min;
        Rt.p(" constant in QUERY imbalance constraint: " 
                            + UtilConstraintBuilder.constantRHSImbalanceConstraint);
            
    }
    
    /**
     * Each imbalance constraint is in the form: {replica_i <= \beta \times replica_j + constant
     * where the constant is determined as {@code \beta - 1} \times cost_update_base_table_one_relica,
     * since in the formula of the cost, we do not take into account this component.
     * 
     * @param beta
     *      The imbalance replica factor
     *  
     */
    protected void setConstantReplicaImbalanceConstraint(double beta)
    {   
        constantRHSImbalanceConstraint = (beta - 1) *  getTotalBaseTableUpdateCost() / nReplicas;
        Rt.p(" constant in REPLICA imbalance constraint: " 
                            + UtilConstraintBuilder.constantRHSImbalanceConstraint);
            
    }
    
    /**
     * Each imbalance constraint is in the form: {replica_i <= \beta \times replica_j + constant
     * where the constant is determined as {@code \beta - 1} \times cost_update_base_table_one_relica,
     * since in the formula of the cost, we do not take into account this component.
     * 
     * @param beta
     *      The imbalance replica factor
     *  
     */
    protected void setConstantFailureImbalanceConstraint(double beta)
    {   
        constantRHSImbalanceConstraint = (beta - 1) *  getTotalBaseTableUpdateCost() / nReplicas;
        Rt.p(" constant in REPLICA imbalance constraint: " 
                            + UtilConstraintBuilder.constantRHSImbalanceConstraint);
            
    }
    
    /**
     * Build the constraint on bounding the update cost. 
     * 
     * @throws IloException
     */
    protected void boundUpdateCostConstraint(double delta) throws IloException
    {
        IloLinearNumExpr expr = cplex.linearNumExpr();
        
        for (QueryPlanDesc desc : queryPlanDescs) {
            
            if (!desc.getSQLCategory().isSame(NOT_SELECT))
                continue;
            
            for (int r = 0; r < nReplicas; r++) {
                // query shell
                if (desc.getSQLCategory().isSame(UPDATE))
                    expr.add(modifyCoef(queryExpr(r, desc.getStatementID(), desc), 
                            getFactorStatement(desc)));
                
                // update cost
                expr.add(modifyCoef(indexUpdateCost(r, desc.getStatementID(), desc),
                        desc.getStatementWeight()));
            }
        }
        
        double totalBaseTableUpdateCost = super.getTotalBaseTableUpdateCost();
        
        // upper bound
        cplex.addLe(expr, delta - totalBaseTableUpdateCost);
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
        // variable sum_y(r, q) = \sum_{k = 1}^{K_q} y^r_{qk}
        poolVariables.createAndStore(VAR_SUM_Y, r, q, 0, 0, 0);    
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
                                                 q, k, 0, 0);
            
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
            for (int i = 0; i < desc.getNumberOfSlots(k); i++)  
                for (Index index : desc.getIndexesAtSlot(k, i)) 
                    for (int r2 = 0; r2 < nReplicas; r2++)
                        if (r2 != r)
                            poolVariables.createAndStore(VAR_COMBINE_X, combineReplicaID(r, r2),
                                                         q, k, i, index.getId());
    }
    
    /**
     * Impose the top m best cost explicit
     * 
     * @throws IloException
     */
    protected void topMBestCostExplicit() throws IloException
    {   
        // only applicable for SELECT and UPDATE statement
        for (QueryPlanDesc desc : queryPlanDescs) {
            if (desc.getSQLCategory().isSame(INSERT) || desc.getSQLCategory().isSame(DELETE))
                continue;
            for (int r = 0; r < nReplicas; r++)  
                queryOptimalBuilder.optimalConstraints(r, desc.getStatementID(), desc);
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
            
            expr.add(modifyCoef(listQueryOptimal.get(i), -1)); 
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
        
        double factor = -1;
        if (isApproximation)
            factor = -1.1;
        
        for (Set<Integer> position : positionSizeM) {
            
            exprTopm = cplex.linearNumExpr();
            exprTopm.add(exprCrossReplica);
            
            for (Integer p : position)
                exprTopm.add(modifyCoef(listQueryOptimal.get(p), factor));
            
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
                imbalanceConstraint(exprs.get(r1), exprs.get(r2), beta);
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
        // query imbalance constraint
        for (QueryPlanDesc desc : queryPlanDescs)
            for (int r1 = 0; r1 < nReplicas; r1++)
                for (int r2 = r1 + 1; r2 < nReplicas; r2++)
                    if (desc.getSQLCategory().isSame(SELECT))
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
        int idY;
        int idSumY;
        
        idSumY = poolVariables.get(VAR_SUM_Y, r, q, 0, 0, 0).getId();
        IloLinearNumExpr expr = cplex.linearNumExpr();
        expr.addTerm(-1, cplexVar.get(idSumY));
        
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
            idY = poolVariables.get(VAR_Y, r, q, k, 0, 0).getId();
            expr.addTerm(1, cplexVar.get(idY));
        }
        
        cplex.addEq(expr, 0, "sum_y" + numConstraints);
        numConstraints++;
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
        idSumY = poolVariables.get(VAR_SUM_Y, r2, q, 0, 0, 0).getId();
        
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
            idCombine = poolVariables.get(VAR_COMBINE_Y, combineReplicaID(r1, r2), q, k, 0, 0).getId();
            idY       = poolVariables.get(VAR_Y, r1, q, k, 0, 0).getId();
            UtilConstraintBuilder.constraintCombineVariable(idCombine, idSumY, idY);
        }
        
        // Index access cost                            
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
            for (int i = 0; i < desc.getNumberOfSlots(k); i++)
                for (Index index : desc.getIndexesAtSlot(k, i)) {
                    
                    idCombine = poolVariables.get(VAR_COMBINE_X, combineReplicaID(r1, r2), 
                                           q, k, i, index.getId()).getId();
                    idX       = poolVariables.get(VAR_X, r1, q, k, i, index.getId()).getId();
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
            idCombine = poolVariables.get(VAR_COMBINE_Y, combineReplicaID(r1, r2), q, k, 0, 0).getId();
            expr.addTerm(desc.getInternalPlanCost(k), cplexVar.get(idCombine));                    
        }                
                    
        // Index access cost                            
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
            for (int i = 0; i < desc.getNumberOfSlots(k); i++)
                for (Index index : desc.getIndexesAtSlot(k, i)) {
                    idCombine = poolVariables.get(VAR_COMBINE_X, combineReplicaID(r1, r2), 
                                           q, k, i, index.getId()).getId();
                    expr.addTerm(desc.getAccessCost(k, i, index), cplexVar.get(idCombine));
                }   
        
        // add -beta time of expr2 
        // get iterator over exprs
        expr.add(modifyCoef(expr2, -beta));
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
    protected void nodeFailures(double beta) throws Exception
    {   
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
    protected void nodeFailureConstraint(int failR, double beta) 
                throws Exception
    {
        List<IloLinearNumExpr> listNewLoads = newLoads(failR);
        
        // similar to node imbalance constraints
        // for each pair of replicas, impose the imbalance factor constraint 
        for (int r1 = 0; r1 < listNewLoads.size() - 1; r1++)
            for (int r2 = r1 + 1; r2 < listNewLoads.size(); r2++) 
                imbalanceConstraint(listNewLoads.get(r1), listNewLoads.get(r2), beta);
    }
    
    /**
     * Derive lists of expressions of alive node assuming
     * the given replica fails.
     * 
     * @param failR
     *      the failure
     * @return
     */
    protected List<IloLinearNumExpr> newLoads(int failR) throws Exception
    {
        IloLinearNumExpr exprNewLoad;
        List<IloLinearNumExpr> listNewLoads = new ArrayList<IloLinearNumExpr>();
        
        for (int r = 0; r < nReplicas; r++)
            if (r != failR) {
                exprNewLoad = cplex.linearNumExpr();
                
                // only consider query statements to the increased load
                for (QueryPlanDesc desc : queryPlanDescs) {
                    if (desc.getSQLCategory().isSame(SELECT))
                        exprNewLoad.add(increadLoadQuery(r, desc.getStatementID(), desc, failR));
                }
                
                // add existing load
                exprNewLoad.add(super.replicaCost(r));
                listNewLoads.add(exprNewLoad);
            }
        
        return listNewLoads;
    }
    
    
    /**
     * The constraint is in the form: {@code increaseExpr <= beta * load(r) }
     * @param r
     * @param expr
     */
    protected void nodeFailureConstraintReplica(int r, IloLinearNumExpr exprIncreaseLoad, double beta) 
                throws IloException
    {   
        IloLinearNumExpr expr;
                
        expr = cplex.linearNumExpr();
        expr.add(exprIncreaseLoad);
        expr.add(modifyCoef(super.replicaCost(r), -beta));
        cplex.addLe(expr, constantRHSImbalanceConstraint, "node_failure_" + numConstraints);
        numConstraints++;
    }
    
    /**
     * Compute the increase in the load of processing query {@code desc} at the given replica {@code r}
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
        
        idSumY = poolVariables.get(VAR_SUM_Y, failR, q, 0, 0, 0).getId();
        
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
            idCombine = poolVariables.get(VAR_COMBINE_Y, combineReplicaID(r, failR), q, k, 0, 0).getId();
            idY       = poolVariables.get(VAR_Y, r, q, k, 0, 0).getId();
            UtilConstraintBuilder.constraintCombineVariable(idCombine, idSumY, idY);
        }
        
        // Index access cost                            
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
            for (int i = 0; i < desc.getNumberOfSlots(k); i++)
                for (Index index : desc.getIndexesAtSlot(k, i)) {
                    idCombine = poolVariables.get(VAR_COMBINE_X, combineReplicaID(r, failR), 
                                           q, k, i, index.getId()).getId();
                    idX       = poolVariables.get(VAR_X, r, q, k, i, index.getId()).getId();
                    UtilConstraintBuilder.constraintCombineVariable(idCombine, idSumY, idX);
                }
        
        double factor = (double) 1 / (loadfactor * (loadfactor - 1));
        
        // the increased cost
        IloLinearNumExpr expr = cplex.linearNumExpr();
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {                    
            idCombine = poolVariables.get(VAR_COMBINE_Y, combineReplicaID(r, failR), q, k, 0, 0).getId();
            expr.addTerm(factor * desc.getInternalPlanCost(k), cplexVar.get(idCombine));                    
        }                
                    
        // Index access cost                            
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
            for (int i = 0; i < desc.getNumberOfSlots(k); i++)
                for (Index index : desc.getIndexesAtSlot(k, i)) {
                    idCombine = poolVariables.get(VAR_COMBINE_X, combineReplicaID(r, failR), 
                                           q, k, i, index.getId()).getId();
                    expr.addTerm(factor * desc.getAccessCost(k, i, index), cplexVar.get(idCombine));
                }    
        
        return expr;
    }
    
    /******************************************
     *
     *   Backup methods from DivBIP
     *
     *
     ******************************************/
    
    
    /**
     * Compute the node failure factor when one node fails
     * 
     * @return
     *      The max ratio
     *      
     * @throws Exception
     */
    public double getFailureImbalance() throws Exception
    {
        List<Double> queryReplica;
        List<List<Double>> queries;
        
        queries = new ArrayList<List<Double>>();
        
        // get the query execution costs
        for (QueryPlanDesc desc : super.queryPlanDescs) {
            
            // consider select-statement only
            if (desc.getSQLCategory().isSame(NOT_SELECT))
                continue;
        
            queryReplica = new ArrayList<Double>();
            
            for (int r = 0; r < nReplicas; r++) 
                queryReplica.add(computeVal(queryExpr(r, desc.getStatementID(), desc)));
            
            queries.add(queryReplica);
        }
        
        
        // query 1: replica 0, replica 1
        // query 2: replica 0, replica 1
        // query 3: replica 0, replica 1
        // .....................
        double maxRatio = -1;
        double ratio;
        
        for (int rFail = 0; rFail < nReplicas; rFail++) {
            ratio = getFailureImbalance(rFail, queries);
            maxRatio = (maxRatio > ratio) ? maxRatio : ratio;                   
        }
        
        return maxRatio;
    }
    
    /**
     * Compute the increase load of other replica when replica {@code rFail} fails.
     *  
     * @param rFail
     *  todo
     * @param queries
     * @param increasLoads
     * @throws Exception  
     */
    private double getFailureImbalance(int rFail, List<List<Double>> queries) throws Exception
    {
        List<Double> increaseLoad = new ArrayList<Double>();
        List<Double> replicaCost = new ArrayList<Double>();
        
        for (int r = 0; r < nReplicas; r++) {
            increaseLoad.add(0.0);
            
            if (r == rFail)
                replicaCost.add(0.0);
            else
                // remember to take into account the base table update cost
                replicaCost.add(computeVal(replicaCost(r)) + 
                                getTotalBaseTableUpdateCost() / nReplicas);
        }
        
        double cost;
        int q = 0;
        
        for (QueryPlanDesc desc : queryPlanDescs) {
            
            if (desc.getSQLCategory().isSame(NOT_SELECT))
                continue;
            
            if (queries.get(q).get(rFail) > 0) {
                // distribute this to other replica that has value greater than 0
                for (int r = 0; r < nReplicas; r++) {
                    
                    if (r == rFail)
                        continue;
                    
                    // avoid small value
                    if (queries.get(q).get(r) > 0.0) {
                        cost = increaseLoad.get(r) + queries.get(q).get(r) 
                                                        / (loadfactor * (loadfactor - 1));
                        increaseLoad.set(r, cost);
                    }    
                }
            }
            
            q++;
        }
        
        
        // compute the ratio
        double newLoad;
        List<Double> costs = new ArrayList<Double>();
        for (int r = 0; r < nReplicas; r++) {
            
            if (r == rFail)
                continue;
            
            newLoad = replicaCost.get(r) + increaseLoad.get(r);
            if (newLoad > 0.0)
                costs.add(newLoad);
        }
        
        return maxRatioInList(costs);
    }
    
    /**
     * Retrieve the max imbalance replica cost. We only consider SELECT statement only
     * 
     * @return
     *      The imbalance factor
     * @throws Exception
     */
    public double getQueryImbalance() throws Exception
    {
        List<Double> queries;
        double maxRatio = -1;
        double ratio;
        double val;
        
        for (QueryPlanDesc desc : super.queryPlanDescs) {
            
            // the query constraint is on SELECT-statement only
            // to facilitate the query routing scheme
            if (desc.getSQLCategory().isSame(NOT_SELECT))
                continue;
        
            queries = new ArrayList<Double>();
            
            for (int r = 0; r < nReplicas; r++) {
                val = computeVal(queryExpr(r, desc.getStatementID(), desc));
                
                if (val > 0.0)
                    queries.add(val);
            }
           
            ratio = maxRatioInList(queries);
            if (ratio > 10000) {
                Rt.p("WATCH-OUT list: " + queries + " ratio: " + ratio);
            }
            
            if (queries.size() != loadfactor) {
                Rt.p("Query's values: " + queries);
                //throw new RuntimeException("We expect to obtain exactly"
                  //        + loadfactor + " value of cost(q,r) that are greater than 0");
            }
            maxRatio = (maxRatio > ratio) ? maxRatio : ratio;
        }
        
        return maxRatio;
    }
}
        