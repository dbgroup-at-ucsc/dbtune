package edu.ucsc.dbtune.bip.div;

import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;


import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.metadata.Index;

import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_DEPLOY;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_DIV;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_MOD;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_Y;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_S;

public class ElasticDivBIP extends DivBIP 
{  
    private int     Ndeploys;
    private int     inputNdeploys;
    private int     inputNreplicas;
    private boolean isExpand;
    
    private double  upperCDeploy;
    
    private Map<String, Integer> mapVarDeployToReplica;
    
    /** Map index to the set of replicas that this index is deploy at the initial
     * setting. */
    private Map<Index, Set<Integer>> materializedIndexReplica;
    
    
    /** 
     * Shrink Replicas Divergent index tuning
     *     
     * @param listWorkload
     *      Each entry of this list is a list of SQL Statements that belong to a same schema
     * @param candidateIndexes
     *      List of candidate indexes (the same at each replica)
     * @param mapIndexesReplicasInitialSetUp
     *      The mapping of indexes materialized at each replica in the inital configuration     
     * @param Nreplicas
     *      The number of replicas
     * @param Ndeploys
     *      The number of replicas to deploy ( <= Nreplicas)     
     * @param loadfactor
     *      Load balancing factor
     * @param upperCdeploy
     *      The maximum deployment cost
     * 
     * @return
     *      A set of indexes to be materialized at each replica
     * @throws SQLException 
     * 
     * {\b Note}: {@code listPreparators} will be removed when this class is fully implemented
     */
    public ElasticDivBIP(int Nreplicas, int loadfactor, double B, int Ndeploys, double upperCDeploy,
                        Map<Index, Set<Integer>> materializedIndexReplica) 
    {
        //super(Nreplicas, loadfactor, B);
        
        // the set of indexes materialized in the initial configuraiton
        this.materializedIndexReplica = materializedIndexReplica;

        // numbers
        this.Ndeploys       = Ndeploys;        
        this.inputNdeploys  = Ndeploys;
        this.inputNreplicas = Nreplicas;
        this.upperCDeploy   = upperCDeploy;
        
        if (Ndeploys > Nreplicas) {
            isExpand = true;
            // Expand replica cases
            // swap between Nreplicas and Ndeploys
            this.nReplicas = this.inputNdeploys;
            this.Ndeploys = this.inputNreplicas;
        } else
            isExpand = false;
        
        // Expand replica cases
        // swap between Nreplicas and Ndeploys
        if (Ndeploys > Nreplicas)
            this.nReplicas = Ndeploys;
    }
    
    
    @Override
    protected void buildBIP()  
    {
        numConstraints = 0;
        
        try {
            // 1. Add variables into list
            constructVariablesElastic();
            
            // 2. Construct the query cost at each replica
            totalCost();
            
            // 3. Atomic constraints
            if (!isExpand)
                atomicInternalPlanShrinkReplicas();
            else 
                ;
                //atomicInternalPlanConstraints();
            
            //atomicAcessCostConstraints();      
            
            // 4. Number of replicas constraint
            if (!isExpand) 
                shrinkReplicas();
            
            // 5. Deployment cost
            deployCostConstraints();
            
            // 6. Top-m best cost 
            topMBestCostConstraints();    
        }
        catch (IloException e) {
            e.printStackTrace();
        }
    }
   
    private void constructVariablesElastic() throws IloException
    {
        super.constructVariables();
        
        mapVarDeployToReplica = new HashMap<String, Integer>();
        
        // for TYPE_DEPLOY for shrink replicas
        if (isExpand == false)
            for (int r = 0; r < nReplicas; r++) {
               DivVariable var = poolVariables.createAndStore(VAR_DEPLOY, r, 0, 0, 0);
               mapVarDeployToReplica.put(var.getName(), r);
            }

        
        // for div_a and mod_a
        for (Index index : candidateIndexes)            
            for (int r = 0; r < nReplicas; r++) {                
                poolVariables.createAndStore(VAR_DIV, r, index.getId(), 0, 0);                 
                poolVariables.createAndStore(VAR_MOD, r, index.getId(), 0, 0);
            }
    }
    
    
    /**
     * Constraints on internal plans: 
     *      \sum_{k \in [1, Kq]} y^r_{qk} <= deploy_r
     * @throws IloException 
     *      
     */
    protected void atomicInternalPlanShrinkReplicas() throws IloException
    {   
        IloLinearNumExpr expr; 
        int idY;
        int idD;
        int q;
        
        for (QueryPlanDesc desc : queryPlanDescs){
            
            q = desc.getStatementID();
        
            for (int r = 0; r < nReplicas; r++) {
                
                expr = cplex.linearNumExpr();
                idD = poolVariables.get(VAR_DEPLOY, r, 0, 0, 0).getId();
                expr.addTerm(-1, cplexVar.get(idD));
                
                // \sum_{k \in [1, Kq]}y^{r}_{qk} <= 1
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
                    idY = poolVariables.get(VAR_Y, r, q, k, 0).getId();
                    expr.addTerm(1, cplexVar.get(idY));
                }
                
                cplex.addLe(expr, 0);
                numConstraints++;
            }
        }
    }
    
    /**
     * 
     * The number of replicas to build is constraint by the given value:
     *     \sum_{r \in [1, Nreplicas} deploy_r <= Ndeploy
     * @throws IloException 
     * 
     */
    protected void shrinkReplicas() throws IloException
    {   
        IloLinearNumExpr expr = cplex.linearNumExpr();        
        int idD;
        
        for (int r = 0; r < nReplicas; r++){
            idD = poolVariables.get(VAR_DEPLOY, r, 0, 0, 0).getId();
            expr.addTerm(1, cplexVar.get(idD));
        }
        
        cplex.addLe(expr, Ndeploys);
        numConstraints++;
    }
    
    /**
     * Deploy cost to materialize indexes:
     *      2 div^r_a + mod^r_a - s^r_a = 1 - s^r_{a, 0}     
     *      Cdeploy = \sum_{r \in [1, Nreplicas} \sum_{a \in Ccand} div^r_a cost(a)
     *      
     * 
     * {\b Note}:  s^r_{a, 0} is given as a part of the input
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
            initialReplicas = materializedIndexReplica.get(index);
            
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
        
        cplex.addLe(exprDeploy, upperCDeploy);  
    }

    
   
    /**
     * The method reads the value of variables corresponding to the presence of indexes
     * at each replica and returns this list of indexes to materialize at each replica
     * 
     * @return
     *      List of indexes to be materialized at each replica
     */
    @Override
    protected IndexTuningOutput getOutput() 
    {
        
        DivConfiguration conf = new DivConfiguration(nReplicas, loadfactor);
        /*
        Map<Integer, Integer> mapDeployedReplicas = new HashMap<Integer, Integer>();
        
        for (Entry<String, Integer> pairVarVal : super.mapVariableValue.entrySet()) {
            if (pairVarVal.getValue() == 1) {
                DivVariable divVar = (DivVariable) this.poolVariables.get(pairVarVal.getKey());
                if (divVar.getType() == DivVariablePool.VAR_DEPLOY){
                    mapDeployedReplicas.put(new Integer(divVar.getReplica()), new Integer(1));
                }
                if (divVar.getType() == DivVariablePool.VAR_S){
                    // only the replicas that will be deployed
                    if (mapDeployedReplicas.containsKey(new Integer(divVar.getReplica())) == false) {
                        continue;
                    }
                    // TODO: only record the normal indexes
                    conf.addMaterializedIndexAtReplica(divVar.getReplica(), mapVarSToIndex.get(pairVarVal.getKey()));
                    
                }
            }
        } 
        */
        return conf;
    }
}
