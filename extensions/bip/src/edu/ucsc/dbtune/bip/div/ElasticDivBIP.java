package edu.ucsc.dbtune.bip.div;

import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;
import ilog.concert.IloRange;
import ilog.cplex.IloCplex;
import ilog.cplex.IloCplex.DoubleParam;
import ilog.cplex.IloCplex.IntParam;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.inum.FullTableScanIndex;
import edu.ucsc.dbtune.metadata.Index;

import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_DEPLOY;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_DIV;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_MOD;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_Y;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_S;

public class ElasticDivBIP extends DivBIP implements ElasticDivergent
{  
    private int nDeploys;
    private List<Double> upperDeployCosts;
    private boolean usedDeployVariable;
    
    /** 
     * Map index to the set of replicas that this index is deployed at the initial
     * setting. 
     **/
    private Map<Integer, Set<Integer>> initialIndexReplica;
    private List<Double> totalCosts;
    
    @Override
    public void setUpperDeployCost(List<Double> costs) 
    {
        upperDeployCosts = costs;
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
        usedDeployVariable = (nDeploys < nReplicas) ? true : false;
        
        // reset the value of nReplica
        if (nDeploys > nReplicas)
            nReplicas = nDeploys;
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
            super.totalCost();
            
            // 3. Atomic constraints
            super.atomicConstraints();
            
            // 4. Use index constraint
            super.usedIndexConstraints();
            
            // 5. Top-m best cost 
            super.routingMultiplicityConstraints();    
        
            // 6. space constraints
            super.spaceConstraints();
            
            if (usedDeployVariable){
                // 5. Number of replicas constraint
                numReplicaConstraint();
       
                // 6. Index deploy
                indexDeployReplica();
            }           
        }
        catch (IloException e) {
            e.printStackTrace();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public IndexTuningOutput solve() throws Exception
    {   
     // 1. Communicate with INUM 
        // to derive the query plan descriptions 
        // including internal cost, index access cost, etc.
        logger.setStartTimer();
        // only popuplate the plan descs if they have not been assigned
        if (!isSetPlanDesc){
            if (communicateInumOnTheFly)
                populatePlanDescriptionOnTheFly();
            else 
                populatePlanDescriptionForStatements();
        } else {
            // re-use the set of query plan description
            candidateIndexes.clear();
            for (QueryPlanDesc desc : queryPlanDescs)
                candidateIndexes.addAll(desc.getIndexes());
        }
        logger.onLogEvent(LogListener.EVENT_POPULATING_INUM);
        
        // 2. Build BIP    
        logger.setStartTimer();
        
        // start CPLEX
        cplex = new IloCplex();
        
        // allow the solution differed 5% from the actual optimal value
        cplex.setParam(DoubleParam.EpGap, environment.getMaxCplexEpGap());
        
        // the running time
        cplex.setParam(DoubleParam.TiLim, environment.getMaxCplexRunningTime());
        // not output the log of CPLEX
        if (!environment.getIsShowCPlexOutput())
            cplex.setOut(null);
        // not output the warning
        cplex.setWarning(null);
        if (isCheckFeasible) 
            cplex.setParam(IntParam.IntSolLim, 1);
        
        buildBIP();       
        logger.onLogEvent(LogListener.EVENT_FORMULATING_BIP);
        
        // 3. Solve the BIP
        logger.setStartTimer();
        // add constraint
        if (initialIndexReplica.size() > 0 || usedDeployVariable)
            reConfigurationConstraint();
        
        totalCosts = new ArrayList<Double>();
        List<IloLinearNumExpr> exprs = new ArrayList<IloLinearNumExpr>();
        List<IloRange> ranges = new ArrayList<IloRange>();
        //IloLinearNumExpr exprDeploy = cplex.linearNumExpr();
        for (int r = 0; r < nReplicas; r++)
            exprs.add(reConfigurationExpr(r));
        //exprs.add(exprDeploy);
        
        boolean isFirst = true;
        for (double cost : upperDeployCosts) {
            
            if (!isFirst)
                for (IloRange range : ranges)
                    cplex.remove(range);
            ranges.clear();
            for (IloLinearNumExpr expr : exprs) {
                ranges.add(cplex.addLe(expr, cost, "deployment_" + numConstraints));
                numConstraints++;
            }
            
            if (cplex.solve()) 
                totalCosts.add(cplex.getObjValue());
            else 
                totalCosts.add(-1.0);
            
            isFirst = false;
        }
        logger.onLogEvent(LogListener.EVENT_SOLVING_BIP);
        return null;            
    }
    
    public List<Double> getTotalCosts()
    {
        return totalCosts;
    }
   
    /**
     * Construct variables for the elastic divergent index tuning problem.
     * 
     * @throws IloException
     */
    private void constructVariablesElastic() throws IloException
    {
        super.constructVariables();
         
        for (int r = 0; r < nReplicas; r++) 
            poolVariables.createAndStore(VAR_DEPLOY, r, 0, 0, 0, 0);
        
        // for div_a and mod_a
        for (Index index : candidateIndexes)            
            for (int r = 0; r < nReplicas; r++) {                
                poolVariables.createAndStore(VAR_DIV, r, 0, 0, 0, index.getId());                 
                poolVariables.createAndStore(VAR_MOD, r, 0, 0, 0, index.getId());
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
        // used super methods
        if (!usedDeployVariable) {
            super.internalAtomicConstraint(r, q, desc);
            return;
        }
            
        IloLinearNumExpr expr;
        int idY;
        int idD;
        
        // \sum_{k \in [1, Kq]}y^{r}_{qk} <= deploy_r
        expr = cplex.linearNumExpr();
        idD = poolVariables.get(VAR_DEPLOY, r, 0, 0, 0, 0).getId();
        expr.addTerm(-1, cplexVar.get(idD));
        
        for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
            idY = poolVariables.get(VAR_Y, r, q, k, 0, 0).getId();
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
            idD = poolVariables.get(VAR_DEPLOY, r, 0, 0, 0, 0).getId();
            for (Index index : candidateIndexes) {
                idS = poolVariables.get(VAR_S, r, 0, 0, 0, index.getId()).getId();
                
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
        
        for (int r = 0; r < nReplicas; r++){
            idD = poolVariables.get(VAR_DEPLOY, r, 0, 0, 0, 0).getId();
            expr.addTerm(1, cplexVar.get(idD));
        }
        
        cplex.addEq(expr, nDeploys, "number_of_deploy");
        numConstraints++;
    }
    
    protected void reConfigurationConstraint() throws IloException
    {
        for (int r = 0; r < nReplicas; r++)
            reConfigurationConstraint(r);
    }
    /**
     * Deploy cost to materialize indexes:
     *      2 div^r_a + mod^r_a - s^r_a = 1 - s^r_{a, 0}     
     *      cost = \sum_{r \in [1, Nreplicas} \sum_{a \in Ccand} div^r_a cost(a)
     * Here, s^r_{a, 0} is given as a part of the input
     * 
     * @throws IloException 
     */
    protected void reConfigurationConstraint(int r) throws IloException
    {
        IloLinearNumExpr expr;
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
            
                
            idDiv = poolVariables.get(VAR_DIV, r, 0, 0, 0, index.getId()).getId();
            idMod = poolVariables.get(VAR_MOD, r, 0, 0, 0, index.getId()).getId();
            idS   = poolVariables.get(VAR_S, r, 0, 0, 0, index.getId()).getId();
                
            rhs = (initialReplicas.contains(r)) ? 0 : 1;
            expr = cplex.linearNumExpr();   
            expr.addTerm(2, cplexVar.get(idDiv));
            expr.addTerm(1, cplexVar.get(idMod));
            expr.addTerm(-1, cplexVar.get(idS));
            cplex.addEq(expr, rhs);
        }
    }
    
    
    /**
     * todo.
     * @return
     * @throws IloExpression
     */
    protected IloLinearNumExpr reConfigurationExpr(int r) throws IloException
    {   
        IloLinearNumExpr expr;
        // empty initial configuration && the same number of replicas
        if (initialIndexReplica.size() == 0 && usedDeployVariable == false){
            int idS;
            expr = cplex.linearNumExpr();
            for (Index index : candidateIndexes)
                if (!(index instanceof FullTableScanIndex)) {
                    idS = poolVariables.get(VAR_S, r, 0, 0, 0, index.getId()).getId();                
                    expr.addTerm(index.getCreationCost(), cplexVar.get(idS));
                }            
                     
            return expr;
        }
        
        
        IloLinearNumExpr exprDeploy = cplex.linearNumExpr();
        int idDiv;
        
        for (Index index : candidateIndexes) {
            if (index instanceof FullTableScanIndex)
                continue;
           
            idDiv = poolVariables.get(VAR_DIV, r, 0, 0, 0, index.getId()).getId();
            exprDeploy.addTerm(index.getCreationCost(), cplexVar.get(idDiv));
        }
        
        return  exprDeploy;
    }
    
    /**
     * Compute the deployment cost
     * @return
     * @throws Exception
     */
    public double computeReconfigurationCost() throws Exception
    {
        throw new RuntimeException("This method has not been implemented");
        //return computeVal(reConfigurationExpr());
    }    
}
