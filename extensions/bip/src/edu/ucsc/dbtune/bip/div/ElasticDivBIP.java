package edu.ucsc.dbtune.bip.div;

import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.inum.FullTableScanIndex;
import edu.ucsc.dbtune.metadata.Index;

import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_DEPLOY;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_DIV;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_MOD;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_Y;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_S;

import static edu.ucsc.dbtune.bip.div.UtilConstraintBuilder.computeVal;

public class ElasticDivBIP extends DivBIP implements ElasticDivergent
{  
    private int     nDeploys;
    private int     nDeployVars;
    private double  upperDeployCost;
    
    /** 
     * Map index to the set of replicas that this index is deployed at the initial
     * setting. 
     **/
    private Map<Integer, Set<Integer>> initialIndexReplica;
    
    
    @Override
    public void setUpperDeployCost(double cost) 
    {
        upperDeployCost = cost;
    }

    @Override
    public void setInitialConfiguration(DivConfiguration div) 
    {
        nReplicas  = div.getNumberReplicas();
        loadfactor = div.getLoadFactor();
        
        initialIndexReplica = new HashMap<Integer, Set<Integer>>();
        
        for (int r = 0; r < div.getNumberReplicas(); r++) 
            for (Index index : div.indexesAtReplica(r)) {
                
                Set<Integer> replicaIDs = initialIndexReplica.get(index.getId());
                if (replicaIDs == null)
                    replicaIDs = new HashSet<Integer>();
                
                replicaIDs.add(r);
                initialIndexReplica.put(index.getId(), replicaIDs);
            }
        
    }

    @Override
    public void setNumberDeployReplicas(int n) 
    {
        nDeploys = n;
        nDeployVars = (nReplicas > nDeploys) ? nReplicas : nDeploys;
    }
    
    @Override
    protected void buildBIP()  
    {
        numConstraints = 0;
        
        try {
            // 1. Add variables into list
            constructVariablesElastic();
            createCplexVariable(poolVariables.variables());
            
            // 2. Construct the query cost at each replica
            totalCost();
            
            // 3. Atomic constraints
            atomicConstraints();
            
            // 4. Use index constraint
            usedIndexConstraints();
            
            // 5. Number of replicas constraint
            numReplicaConstraint();
       
            // 6. Index deploy
            indexDeployReplica();
            
            // 8. Top-m best cost 
            topMBestCostConstraints();    
        
            // 9. space constraints
            spaceConstraints();
            
            // 7. Deployment cost
            deployCostConstraints();
        }
        catch (IloException e) {
            e.printStackTrace();
        }
    }
   
    /**
     * Construct variables for the elastic divergent index tuning problem.
     * 
     * @throws IloException
     */
    private void constructVariablesElastic() throws IloException
    {
        super.constructVariables();
         
        for (int r = 0; r < nDeployVars; r++) 
            poolVariables.createAndStore(VAR_DEPLOY, r, 0, 0, 0);
        
        // for div_a and mod_a
        for (Index index : candidateIndexes)            
            for (int r = 0; r < nReplicas; r++) {                
                poolVariables.createAndStore(VAR_DIV, r, 0, 0, index.getId());                 
                poolVariables.createAndStore(VAR_MOD, r, 0, 0, index.getId());
            }
    }
    
    
    /**
     * Internal atomic constraints: only one template plan is chosen to compute {@code cost(q,r)}.
     * In addition, if replica r is not deploy, {@code cost(q,r) = 0}.
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
    @Override
    protected void internalAtomicConstraint(int r, int q, QueryPlanDesc desc) throws IloException
    {
        IloLinearNumExpr expr;
        int idY;
        int idD;
        
        // \sum_{k \in [1, Kq]}y^{r}_{qk} <= deploy_r
        expr = cplex.linearNumExpr();
        idD = poolVariables.get(VAR_DEPLOY, r, 0, 0, 0).getId();
        expr.addTerm(-1, cplexVar.get(idD));
        
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
            idY = poolVariables.get(VAR_Y, r, q, k, 0).getId();
            expr.addTerm(1, cplexVar.get(idY));
        }
        
        cplex.addLe(expr, 0, "atomic_internal_" + numConstraints);
        numConstraints++;
    }
    
    /**
     * An index is not used if the corresponding replica is not deployed.
     * 
     * @throws IloException
     */
    protected void indexDeployReplica() throws IloException
    {
        IloLinearNumExpr expr;
        int idD;
        int idS;
        
        for (int r = 0; r < nReplicas; r++) {
            idD = poolVariables.get(VAR_DEPLOY, r, 0, 0, 0).getId();
            for (Index index : candidateIndexes) {
                idS = poolVariables.get(VAR_S, r, 0, 0, index.getId()).getId();
                
                expr = cplex.linearNumExpr();
                expr.addTerm(1, cplexVar.get(idS));
                expr.addTerm(-1, cplexVar.get(idD));
                cplex.addLe(expr, 0, "deploy_" + numConstraints);
                numConstraints++;
            }
        }
    }
    
    /**
     * 
     * The number of replicas to build is constraint by the given value:
     *     \sum_{r \in [1, Nreplicas} deploy_r = Ndeploy
     * @throws IloException 
     * 
     */
    protected void numReplicaConstraint() throws IloException
    {   
        IloLinearNumExpr expr = cplex.linearNumExpr();        
        int idD;
        
        for (int r = 0; r < nDeployVars; r++){
            idD = poolVariables.get(VAR_DEPLOY, r, 0, 0, 0).getId();
            expr.addTerm(1, cplexVar.get(idD));
        }
        
        cplex.addEq(expr, nDeploys, "number_of_deploy");
        numConstraints++;
    }
    
    /**
     * Deploy cost to materialize indexes:
     *      2 div^r_a + mod^r_a - s^r_a = 1 - s^r_{a, 0}     
     *      cost = \sum_{r \in [1, Nreplicas} \sum_{a \in Ccand} div^r_a cost(a)
     * Here, s^r_{a, 0} is given as a part of the input
     * 
     * @throws IloException 
     */
    protected void deployCostConstraints() throws IloException
    {
        IloLinearNumExpr expr;
        IloLinearNumExpr exprDeploy = cplex.linearNumExpr();
        
        Set<Integer> initialReplicas;
        int idDiv;
        int idMod;
        int idS;
        int rhs; // 1 - s^r_{a,0}
        
        for (Index index : candidateIndexes) {
            
            if (index instanceof FullTableScanIndex)
                continue;
            
            initialReplicas = initialIndexReplica.get(index.getId());
            if (initialReplicas == null)
                initialReplicas = new HashSet<Integer>();
            
            for (int r = 0; r < nReplicas; r++) {
                
                idDiv = poolVariables.get(VAR_DIV, r, 0, 0, index.getId()).getId();
                idMod = poolVariables.get(VAR_MOD, r, 0, 0, index.getId()).getId();
                idS   = poolVariables.get(VAR_S, r, 0, 0, index.getId()).getId();
                
                rhs = (initialReplicas.contains(r)) ? 0 : 1;
                expr = cplex.linearNumExpr();   
                expr.addTerm(2, cplexVar.get(idDiv));
                expr.addTerm(1, cplexVar.get(idMod));
                expr.addTerm(-1, cplexVar.get(idS));
                cplex.addEq(expr, rhs);
                
                exprDeploy.addTerm(index.getCreationCost(), cplexVar.get(idDiv));
            
            }
        }
        
        cplex.addLe(exprDeploy, upperDeployCost, "deploy_constraint");  
    }
    
    /**
     * todo.
     * @return
     * @throws IloExpression
     */
    protected IloLinearNumExpr deriveDeploymentExpr() throws IloException
    {   
        IloLinearNumExpr exprDeploy = cplex.linearNumExpr();
        int idDiv;
        
        for (Index index : candidateIndexes) {
            
            if (index instanceof FullTableScanIndex)
                continue;
            
            for (int r = 0; r < nReplicas; r++) {
                idDiv = poolVariables.get(VAR_DIV, r, 0, 0, index.getId()).getId();
                exprDeploy.addTerm(index.getCreationCost(), cplexVar.get(idDiv));
            }
        }
        
        System.out.println(" deploy formula: " + exprDeploy);
        return  exprDeploy;
    }
    
    /**
     * Compute the deployment cost
     * @return
     * @throws Exception
     */
    public double computeDeploymentCost() throws Exception
    {
        return computeVal(deriveDeploymentExpr());
    }
}
