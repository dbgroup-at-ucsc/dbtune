package edu.ucsc.dbtune.bip.div;

import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;


import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import edu.ucsc.dbtune.bip.core.IndexTuningOutput;
import edu.ucsc.dbtune.bip.core.QueryPlanDesc;
import edu.ucsc.dbtune.metadata.Index;

import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_DEPLOY;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_DIV;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_MOD;
import static edu.ucsc.dbtune.bip.div.DivVariablePool.VAR_Y;

public class ElasticDivBIP extends DivBIP 
{  
    private int     Ndeploys;
    private int     inputNdeploys;
    private int     inputNreplicas;
    private boolean isExpand;
    
    private Map<String, Integer>      mapVarDeployToReplica;
    
    
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
    public ElasticDivBIP(int Nreplicas, int loadfactor, double B, int Ndeploys, double upperCdeploy,
                        Map<Index, List<Integer>> mapIndexesReplicasInitialConfiguration) 
    {
        super(Nreplicas, loadfactor, B);
        //this.mapIndexesReplicasInitialConfiguration = mapIndexesReplicasInitialConfiguration;
        //this.upperCdeploy = upperCdeploy;
        this.Ndeploys = Ndeploys;
        
        this.inputNdeploys = Ndeploys;
        this.inputNreplicas = Nreplicas;
        
        if (Ndeploys > Nreplicas) {
            isExpand = true;
            // Expand replica cases
            // swap between Nreplicas and Ndeploys
            this.Nreplicas = this.inputNdeploys;
            this.Ndeploys = this.inputNreplicas;
        } else {
            isExpand = false;
        }
        
        if (Ndeploys > Nreplicas) {
            // Expand replica cases
            // swap between Nreplicas and Ndeploys
            this.Nreplicas = Ndeploys;
        }
    }
    
    
    @Override
    protected void buildBIP()  
    {
        numConstraints = 0;
        
        try {
            // 1. Add variables into list
            constructVariablesElastic();
            
            // 2. Construct the query cost at each replica
            queryCostReplica();
            
            // 3. Atomic constraints
            if (!isExpand)
                atomicInternalPlanShrinkReplicas();
            else 
                atomicInternalPlanConstraints();
            
            atomicAcessCostConstraints();      
            
            // 4. Number of replicas constraint
            if (!isExpand) 
                numReplicaConstraintShrinkReplica();
            
            // 5. Deployment cost
            buildDeployCostConstraints();
            
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
            for (int r = 0; r < this.Nreplicas; r++) {
               DivVariable var = poolVariables.createAndStore(VAR_DEPLOY, r, 0, 0, 0);
               mapVarDeployToReplica.put(var.getName(), new Integer(r));
            }

        
        // for div_a and mod_a
        for (Index index : candidateIndexes)            
            for (int r = 0; r < Nreplicas; r++) {                
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
        
            for (int r = 0; r < Nreplicas; r++) {
                
                expr = cplex.linearNumExpr();
                idD = poolVariables.getDivVariable(VAR_DEPLOY, r, 0, 0, 0).getId();
                expr.addTerm(-1, cplexVar.get(idD));
                
                // \sum_{k \in [1, Kq]}y^{r}_{qk} <= 1
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
                    
                    idY = poolVariables.getDivVariable(VAR_Y, r, q, k, 0).getId();
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
     *     \sum_{r \in [1, Nreplicas} deploy_r = Ndeploy
     * @throws IloException 
     * 
     */
    protected void numReplicaConstraintShrinkReplica() throws IloException
    {
        IloLinearNumExpr expr = cplex.linearNumExpr();        
        int idD;
        
        for (int r = 0; r < Nreplicas; r++){
            idD = poolVariables.getDivVariable(VAR_DEPLOY, r, 0, 0, 0).getId();
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
     */
    protected void buildDeployCostConstraints()
    {
        /*
        List<String> linList = new ArrayList<String>();
        for (Index index : candidateIndexes) {
            List<Integer> found = mapIndexesReplicasInitialConfiguration.get(index);
            Map<Integer, Integer> mapReplicas = new HashMap<Integer, Integer>();
            
            if (found != null) 
                for (Integer replica : found) 
                    mapReplicas.put(replica, replica);

            int ga = index.getId();
            
            // TODO: optimize for expand  case, with new replica, don't need to impose the constraints
            // on div_a and mod_a
            for (int r = 0; r < Nreplicas; r++) {
                String div_a = poolVariables.createAndStore(VAR_DIV, r, ga, 0, 0).getName();
                String mod_a = poolVariables.createAndStoreVariable(DivVariablePool.VAR_MOD, r, ga, 0, 0, 0).getName();
                String s_a = poolVariables.createAndStoreVariable(DivVariablePool.VAR_S, r, ga, 0, 0, 0).getName();
                // check if the index is materialized at this replica in the initial set up
                int s_a0 = 0;
                if (mapReplicas.containsKey(new Integer(r)) == true) {
                    s_a0 = 1;
                }
                
                int RHS = 1 - s_a0;
                String LHS = "2" + div_a + " + " + mod_a + " - " + s_a; 
                buf.getCons().add("deploy_cost_7_" + numConstraints + ": " + LHS + " = " + Integer.toString(RHS));
                numConstraints++;
                linList.add(Double.toString(index.getCreationCost()) + div_a);
            }
        }
        Cdeploy = Strings.concatenate(" + ", linList); 
        buf.getCons().add("atomic_deploy_cost_8" + numConstraints + ": " + Cdeploy
                + " <= " + Double.toString(this.upperCdeploy));
                */
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
        
        DivConfiguration conf = new DivConfiguration(Nreplicas);
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
