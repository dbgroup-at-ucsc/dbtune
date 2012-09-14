package edu.ucsc.dbtune.bip.div;

import java.util.List;

public interface ElasticDivergent 
{
    /**
     * Set the (new) number of replicas to deploy. If {@code n} is greater then the current number
     * of replicas in the system, then this is the expanding case.
     * Otherwise, it is the shrinking case.
     * 
     * @param n
     *      The number of replicas to deploy.
     */
    void setNumberDeployReplicas(int n);

    /**
     * Set the initial configurations that are deployed in the current system.
     * 
     * @param div
     *      The configuration that contains indexes that are currently materialized at each replica.
     */
    void setInitialConfiguration(DivConfiguration div);

    /**
     * Set the upper bound on the deployment cost.
     * 
     * @param cost
     *      The maximum of deployment cost.
     */
    void setUpperDeployCost(List<Double> costs);
}
