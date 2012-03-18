package edu.ucsc.dbtune.bip.div;

import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_S;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_U;
import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;
import ilog.concert.IloLinearNumExprIterator;
import ilog.concert.IloNumVar;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;


import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.bip.interactions.SortableIndexAcessCost;
import edu.ucsc.dbtune.metadata.Index;

public class ConstraintDivBIP extends DivBIP
{
    public static int IMBALANCE_REPLICA = 1001;
    
    protected boolean isApproximation;
    protected List<DivConstraint> constraints;
    
    public ConstraintDivBIP(List<DivConstraint> constraints)
    {
        isApproximation = true;
        this.constraints = constraints;
    }
    
    @Override
    protected void buildBIP() 
    {
        numConstraints = 0;
        
        try {
            // 1. Add variables into list
            constructVariables();
            
            // 2. Construct the query cost at each replica
            super.totalCost();
            
            // 3. Atomic constraints
            super.atomicConstraints();      
            
            // 4. Top-m best cost 
            super.topMBestCostConstraints();
            
            // 5. Space constraints
            super.spaceConstraints();
            
            // 6. additional constraints
            for (DivConstraint c : constraints) {
                if (c.getType() == IMBALANCE_REPLICA)
                    imbalanceReplicaConstraints(c.getFactor());
            }
            
        }     
        catch (IloException e) {
            e.printStackTrace();
        }
    }
    
    @Override
    protected void constructVariables() throws IloException
    {   
        poolVariables = new DivVariablePool();
        mapVarSToIndex = new HashMap<String, Index>();
        
        // variable for each query descriptions
        for (int r = 0; r < nReplicas; r++)
            for (QueryPlanDesc desc : queryPlanDescs) {
                constructVariables(r, desc.getStatementID(), desc);
                constructConstraintVariables(r, desc.getStatementID(), desc);
            }
        
        // for TYPE_S
        for (int r = 0; r < nReplicas; r++) 
            for (Index index : candidateIndexes) {
                DivVariable var = poolVariables.createAndStore(VAR_S, r, 0, 0, index.getId());
                mapVarSToIndex.put(var.getName(), index);
            }
        
        createCplexVariable(poolVariables.variables());
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
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++)
            for (int i = 0; i < desc.getNumberOfSlots(); i++)  
                for (Index index : desc.getIndexesAtSlot(i)) 
                    poolVariables.createAndStore(VAR_U, r, q, k, index.getId());
    }
    
    /**
     * Impose the imbalance constraints among replicas
     */
    protected void imbalanceReplicaConstraints(double beta) throws IloException
    {   
        IloLinearNumExpr exprReplica;
        IloLinearNumExpr expr;
        IloNumVar        var;
        
        IloLinearNumExprIterator iter;
        List<IloLinearNumExpr> exprs = new ArrayList<IloLinearNumExpr>();
        
        // local optimal
        for (int r = 0; r < nReplicas; r++) {
           
            exprReplica = cplex.linearNumExpr(); 
            
            for (QueryPlanDesc desc : queryPlanDescs) {
                
                expr = super.queryExpr(r, desc.getStatementID(), desc);
                
                // constraint for the local optimal
                localOptimal(r, desc.getStatementID(), desc, expr);
                presentVariableLocalOptimal(r, desc.getStatementID(), desc);
                selectingIndexAtEachSlot(r, desc.getStatementID(), desc);
                // construct formula for the replica
                
                exprReplica.add(expr);
            
            }
            
            exprs.add(exprReplica);
        }
                
        double coef = 0.0;
        // for each pair of replicas, impose the imbalance factor constraint 
        for (int r1 = 0; r1 < nReplicas - 1; r1++)
            for (int r2 = r1 + 1; r2 < nReplicas; r2++) {
                
                // r1 - \beta x r2 <= 0
                expr = cplex.linearNumExpr();
                expr.add(exprs.get(r1));
                
                // get iterator over exprs[r2]
                iter = exprs.get(r2).linearIterator();
                while (iter.hasNext()) {
                    var = iter.nextNumVar();
                    coef = iter.getValue();
                    expr.addTerm(var, - beta * coef);
                }
                cplex.addLe(expr, 0, "imbalance_replica_" + numConstraints);
                numConstraints++;
                
                // r2 - \beta x r1 <= 0
                expr = cplex.linearNumExpr();
                expr.add(exprs.get(r2));
                
                // get iterator over exprs[r1]
                iter = exprs.get(r1).linearIterator();
                while (iter.hasNext()) {
                    var = iter.nextNumVar();
                    coef = iter.getValue();
                    expr.addTerm(var, - beta * coef);
                }
                cplex.addLe(expr, 0, "imbalance_replica_" + numConstraints);
                numConstraints++;
            }
                
    }
    
    /**
     * This set of constraints ensure {@code cost(q, X} is not greater than the local optimal cost 
     * of using any template plan.
     *   
     * @throws IloException 
     * 
     */
    protected void localOptimal(int r, int q, QueryPlanDesc desc, IloLinearNumExpr exprQuery)
                   throws IloException
    {   
        IloLinearNumExpr expr;        
        int idU;
        double approxCoef;        
        
        if (isApproximation)
            approxCoef = 1.1;
        else 
            approxCoef = 1.0;
        
        // local optimal
        for (int t = 0; t < desc.getNumberOfTemplatePlans(); t++) {
            
            expr = cplex.linearNumExpr();
            expr.add(exprQuery);    
                        
            for (int i = 0; i < desc.getNumberOfSlots(); i++) 
                for (Index index : desc.getIndexesAtSlot(i)) {
                    idU = poolVariables.get(VAR_U, r, q, t, index.getId()).getId();
                    expr.addTerm(-approxCoef * desc.getAccessCost(t, index), cplexVar.get(idU));
                }
            
            cplex.addLe(expr, approxCoef * desc.getInternalPlanCost(t), "local_" + numConstraints);
            numConstraints++;
        }
        
        // atomic constraint
        for (int t = 0; t < desc.getNumberOfTemplatePlans(); t++)
            for (int i = 0; i < desc.getNumberOfSlots(); i++) {
                expr = cplex.linearNumExpr();
                for (Index index : desc.getIndexesAtSlot(i)) {
                    idU = poolVariables.get(VAR_U, r, q, t, index.getId()).getId();
                    expr.addTerm(1, cplexVar.get(idU));           
                }
                
                cplex.addEq(expr, 1, "atomic_U" + numConstraints);
                numConstraints++;
            }
        
    }
            
    
    
    /**
     * Constraint on the present of {@code VAR_U} variables.
     * 
     * For example, a variable corresponding to some index {@code a} must be {@code 0}
     * if {@code a} is not recommended.
     * 
     * @throws IloException 
     * 
     */
    protected void presentVariableLocalOptimal(int r, int q, QueryPlanDesc desc) throws IloException
    {   
        IloLinearNumExpr expr;
        int idU;
        int idS;
            
        for (int t = 0; t < desc.getNumberOfTemplatePlans(); t++)
            for (int i = 0; i < desc.getNumberOfSlots(); i++) {
                // not constraint FTS variables
                for (Index index : desc.getIndexesWithoutFTSAtSlot(i)) {
                    idU = poolVariables.get(VAR_U, r, q, t, index.getId()).getId();
                    idS = poolVariables.get(VAR_S, r, 0, 0, index.getId()).getId();
                    expr = cplex.linearNumExpr();
                    expr.addTerm(1, cplexVar.get(idU));
                    expr.addTerm(-1, cplexVar.get(idS));            
                    cplex.addLe(expr, 0, "U_present_" + numConstraints);
                    numConstraints++;
                                        
                }         
            }
    }
    
    /**
     * 
     * The constraints ensure the index with the small index access cost is used
     * to compute {@code cost(q, r)} 
     * 
     * @throws IloException 
     * 
     */
    protected void selectingIndexAtEachSlot(int r, int q, QueryPlanDesc desc) throws IloException
    {  
        
        for (int t = 0; t < desc.getNumberOfTemplatePlans(); t++)
            for (int i = 0; i < desc.getNumberOfSlots(); i++) 
                selectingIndexAtEachSlot(r, q, t, i, desc);
    }
    
    protected void selectingIndexAtEachSlot(int r, int q, int t, int i, QueryPlanDesc desc) 
                   throws IloException
    {
        int idU;
        int idS;        
        int idFTS, numIndex;
     
        // Sort index access cost
        List<SortableIndexAcessCost> listSortedIndex  = new ArrayList<SortableIndexAcessCost>();
                
        for (Index index : desc.getIndexesAtSlot(i)) {
            SortableIndexAcessCost sac = new SortableIndexAcessCost 
                                            (desc.getAccessCost(t, index), index);
            listSortedIndex.add(sac);                       
        }                   
            
        numIndex = desc.getIndexesAtSlot(i).size();
        idFTS = desc.getIndexesAtSlot(i).get(numIndex - 1).getId();
            
        // sort in the increasing order of the index access cost
        Collections.sort(listSortedIndex);
                               
        List<Integer> varIDs = new ArrayList<Integer>();
        
        for (SortableIndexAcessCost sac : listSortedIndex) {  
            
            Index index = sac.getIndex();
            idU = poolVariables.get(VAR_U, r, q, t, index.getId()).getId();
            varIDs.add(idU);
                    
            if (index.getId() == idFTS) {
                IloLinearNumExpr exprInternal = cplex.linearNumExpr();
                for (int varID : varIDs)
                    exprInternal.addTerm(1, cplexVar.get(varID));
                
                cplex.addEq(exprInternal, 1, "FTS_" + numConstraints);
                numConstraints++;
                break; // the remaining variables will be assigned value 0 
            } else {                                    
                IloLinearNumExpr exprInternal = cplex.linearNumExpr();
                for (int varID : varIDs)
                    exprInternal.addTerm(1, cplexVar.get(varID));
                    
                idS = poolVariables.get(VAR_S, r, 0, 0, index.getId()).getId();
                exprInternal.addTerm(-1, cplexVar.get(idS));
                cplex.addGe(exprInternal, 0, "select_index_" + numConstraints); 
                numConstraints++;
            }   
        }   
    }
    
    
}
    
    

    
    

