package edu.ucsc.dbtune.bip.div;


import java.util.List;

import edu.ucsc.dbtune.bip.util.BIPIndexPool;
import edu.ucsc.dbtune.bip.util.QueryPlanDesc;

public class ElasticDivLinGenerator extends DivLinGenerator 
{
    private int Ndeploy;
    
    ElasticDivLinGenerator(String prefix, BIPIndexPool poolIndexes, List<QueryPlanDesc> listQueryPlanDecs, 
                            int Nreplicas, int Ndeploy, int loadfactor, double B) 
    {                       
        super(prefix, poolIndexes, listQueryPlanDecs, Nreplicas, loadfactor, B);
        this.Ndeploy = Ndeploy;
    }
    
    /**
     * Constraints on internal plans: 
     *      \sum_{k \in [1, Kq]} y^r_{qk} <= deploy_r
     *      
     */
    /*
    protected void buildAtomicInternalPlanConstraints()
    {
        List<String> linList = new ArrayList<String>();
        
        for (int r = 0; r < Nreplicas; r++){
            String var_deploy = varCreator.constructVariableName(BIPVariableCreator.VAR_DEPLOY, r, 0, 0, 0, 0);
            listVar.add(var_deploy);
        }
        
        for (MultiQueryPlanDesc desc : listQueryPlanDescs){
            int q = desc.getId();
        
            for (int r = 0; r < Nreplicas; r++) {
                String var_deploy = varCreator.constructVariableName(BIPVariableCreator.VAR_DEPLOY, r, 0, 0, 0, 0);
                linList.clear();
                // \sum_{k \in [1, Kq]}y^{r}_{qk} <= 1
                for (int k = 0; k < desc.getNumPlans(); k++) {
                    linList.add(varCreator.constructVariableName(BIPVariableCreator.VAR_Y, r, q, k, 0, 0));
                }
                buf.getCons().println("atomic_2a_" + numConstraints + ": " + StringConcatenator.concatenate(" + ", linList) 
                                        + " - " + var_deploy +
                                        " <= 0");
                numConstraints++;
            }
        }
    }
    */
    /**
     * 
     * The number of replicas to build is constraint by the given value:
     *     \sum_{r \in [1, Nreplicas} deploy_r = Ndeploy
     * 
     */
    /*
    protected void buildNumReplicaConstraint()
    {
        List<String> linList = new ArrayList<String>();
        for (int r = 0; r < Nreplicas; r++){
            String var_deploy = varCreator.constructVariableName(BIPVariableCreator.VAR_DEPLOY, r, 0, 0, 0, 0);
            linList.add(var_deploy);
        }
        buf.getCons().println("num_replica_6_" + numConstraints + ": " + StringConcatenator.concatenate(" + ", linList) 
                + " <= " + Ndeploy);
        numConstraints++;
    }
    */
    /**
     * Deploy cost to materialize indexes:
     *      2 div^r_a + mod^r_a - s^r_a = 1 - s^r_{a, 0}     
     *      Cdeploy = \sum_{r \in [1, Nreplicas} \sum_{a \in Ccand} div^r_a cost(a)
     *      
     * 
     * {\b Note}:    
     */
    protected void buildDeployCostFormula()
    {
        
    }

}
