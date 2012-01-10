package edu.ucsc.dbtune.bip.div;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.ucsc.dbtune.bip.util.BIPIndexPool;
import edu.ucsc.dbtune.bip.util.LogListener;
import edu.ucsc.dbtune.bip.util.QueryPlanDesc;
import edu.ucsc.dbtune.bip.util.StringConcatenator;
import edu.ucsc.dbtune.metadata.Index;

public class ElasticDivLinGenerator extends DivLinGenerator 
{
    private int Ndeploys;
    private double upperCdeploy;
    private Map<String, Integer> mapVarDeployToReplica;
    private Map<Index, List<Integer>> mapIndexesReplicasInitialConfiguration;
    private String Cdeploy;
    private boolean isExpand;
    
    // tricky: swap between Ndeploy and Nreplicas so that the source code changes minimally 
    private int inputNdeploys, inputNreplicas;
    
    ElasticDivLinGenerator (String prefix, BIPIndexPool poolIndexes, List<QueryPlanDesc> listQueryPlanDecs, 
                            Map<Index, List<Integer>> mapIndexesReplicasInitialConfiguration,
                            int Nreplicas, int Ndeploys, int loadfactor, double upperCdeploy) 
    {                       
        super(prefix, poolIndexes, listQueryPlanDecs, Nreplicas, loadfactor, 0.0);
        this.mapIndexesReplicasInitialConfiguration = mapIndexesReplicasInitialConfiguration;
        this.upperCdeploy = upperCdeploy;
        this.Ndeploys = Ndeploys;
        mapVarDeployToReplica = new HashMap<String, Integer>();
        
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
    }
    
    /**
     * The function builds the BIP Divergent Index Tuning Problem:
     * <p>
     * <ol>
     *  <li> Atomic constraints </li>
     *  <li> Top-m best cost constraints </li>
     *  <li> Space constraints </li>
     * </ol>
     * </p>   
     * 
     * @param listener
     *      Log the building process
     * 
     * @throws IOException
     */
    public void build(final LogListener listener) throws IOException 
    {
        if (isExpand == true) {
            listener.onLogEvent(LogListener.BIP, "Building expand replicas DIV program...");
        } else {
            listener.onLogEvent(LogListener.BIP, "Building shrink replicas DIV program...");
        }
        numConstraints = 0;
        
        // 1. Add variables into list
        constructVariablesElastic();
        
        // 2. Construct the query cost at each replica
        buildQueryCostReplica();
        
        // 3. Atomic constraints
        if (isExpand == false) {
            buildAtomicInternalPlanShrinkReplicasConstraints();
        } else {
            buildAtomicInternalPlanConstraints();
        }
        buildAtomicIndexAcessCostConstraints();      
        
        // 4. Number of replicas constraint
        if (isExpand == false) {
            buildNumReplicaConstraintShrinkReplica();
        }
        
        // 5. Deployment cost
        buildDeployCostConstraints();
        
        // 6. Top-m best cost 
        buildTopmBestCostConstraints();
        
        // 7. Optimal constraint
        buildObjectiveFunction();
        
        // 8. Binary variables
        binaryVariableConstraints();
        
        buf.close();
                
        if (isExpand == true) {
            listener.onLogEvent(LogListener.BIP, "Built expand replicas DIV program");
        } else {
            listener.onLogEvent(LogListener.BIP, "Built shrink replicas DIV program");
        }
    }
   
    private void constructVariablesElastic()
    {
        super.constructVariables();
        // for TYPE_DEPLOY for shrink replicas
        if (this.isExpand == false) {
            for (int r = 0; r < this.Nreplicas; r++) {
               DivVariable var = poolVariables.createAndStoreBIPVariable(DivVariablePool.VAR_DEPLOY, r, 0, 0, 0, 0);
               this.mapVarDeployToReplica.put(var.getName(), new Integer(r));
            }
        }
        
        // for div_a and mod_a
        for (int ga = 0; ga < poolIndexes.getTotalIndex(); ga++) {
            for (int r = 0; r < Nreplicas; r++) {
                poolVariables.createAndStoreBIPVariable(DivVariablePool.VAR_DIV, r, ga, 0, 0, 0);
                poolVariables.createAndStoreBIPVariable(DivVariablePool.VAR_MOD, r, ga, 0, 0, 0);
            }
        }
    }
    
    
    /**
     * Constraints on internal plans: 
     *      \sum_{k \in [1, Kq]} y^r_{qk} <= deploy_r
     *      
     */
    protected void buildAtomicInternalPlanShrinkReplicasConstraints()
    {   
        for (QueryPlanDesc desc : listQueryPlanDescs){
            int q = desc.getStatementID();
        
            for (int r = 0; r < Nreplicas; r++) {
                String var_deploy = poolVariables.getDivVariable(DivVariablePool.VAR_DEPLOY, r, 0, 0, 0, 0).getName();
                List<String> linList = new ArrayList<String>();
                // \sum_{k \in [1, Kq]}y^{r}_{qk} <= 1
                for (int k = 0; k < desc.getNumberOfTemplatePlans(); k++) {
                    linList.add(poolVariables.getDivVariable(DivVariablePool.VAR_Y, r, q, k, 0, 0).getName());
                }
                buf.getCons().println("atomic_2a_" + numConstraints + ": " + StringConcatenator.concatenate(" + ", linList) 
                                        + " - " + var_deploy +
                                        " <= 0");
                numConstraints++;
            }
        }
    }
    
    /**
     * 
     * The number of replicas to build is constraint by the given value:
     *     \sum_{r \in [1, Nreplicas} deploy_r = Ndeploy
     * 
     */
    protected void buildNumReplicaConstraintShrinkReplica()
    {
        List<String> linList = new ArrayList<String>();
        for (int r = 0; r < Nreplicas; r++){
            String var_deploy = poolVariables.getDivVariable(DivVariablePool.VAR_DEPLOY, r, 0, 0, 0, 0).getName();
            linList.add(var_deploy);
        }
        buf.getCons().println("num_replica_6_" + numConstraints + ": " + StringConcatenator.concatenate(" + ", linList) 
                + " <= " + Ndeploys);
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
        List<String> linList = new ArrayList<String>();
        for (int ga = 0; ga < poolIndexes.getTotalIndex(); ga++) {
            Index index = this.poolIndexes.indexes().get(ga);
            List<Integer> found = mapIndexesReplicasInitialConfiguration.get(index);
            Map<Integer, Integer> mapReplicas = new HashMap<Integer, Integer>();
            if (found != null) {
                for (Integer replica : found) {
                    mapReplicas.put(replica, replica);
                }
            }
            
            // TODO: optimize for expand  case, with new replica, don't need to impose the constraints
            // on div_a and mod_a
            for (int r = 0; r < Nreplicas; r++) {
                String div_a = poolVariables.createAndStoreBIPVariable(DivVariablePool.VAR_DIV, r, ga, 0, 0, 0).getName();
                String mod_a = poolVariables.createAndStoreBIPVariable(DivVariablePool.VAR_MOD, r, ga, 0, 0, 0).getName();
                String s_a = poolVariables.createAndStoreBIPVariable(DivVariablePool.VAR_S, r, ga, 0, 0, 0).getName();
                // check if the index is materialized at this replica in the initial set up
                int s_a0 = 0;
                if (mapReplicas.containsKey(new Integer(r)) == true) {
                    s_a0 = 1;
                }
                
                int RHS = 1 - s_a0;
                String LHS = "2" + div_a + " + " + mod_a + " - " + s_a; 
                buf.getCons().println("deploy_cost_7_" + numConstraints + ": " + LHS + " = " + Integer.toString(RHS));
                numConstraints++;
                linList.add(Double.toString(index.getCreationCost()) + div_a);
            }
        }
        Cdeploy = StringConcatenator.concatenate(" + ", linList); 
        buf.getCons().println("atomic_deploy_cost_8" + numConstraints + ": " + Cdeploy
                + " <= " + Double.toString(this.upperCdeploy));
    }
}
